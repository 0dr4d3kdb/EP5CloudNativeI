package cl.duoc.ventas;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Repartitioned;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.To;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.WindowStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Properties;
import java.util.Set;

public final class StreamsApp {

    private static final Logger LOGGER = LoggerFactory.getLogger(StreamsApp.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static final String STATE_STORE = "ventas-por-tienda";
    private static final String DEDUP_STORE = "eventos-procesados";
    private static final String LATE_STORE = "tiempo-maximo-eventos";
    private static final String MAX_STREAM_TIME_KEY = "\u0000STREAM_TIME\u0000";

    private StreamsApp() {
    }

    public static void main(String[] args) {
        String bootstrap = env("KAFKA_BOOTSTRAP", "localhost:9092");
        String rawTopic = env("RAW_TOPIC", "ventas-raw");
        String aggTopic = env("AGG_TOPIC", "ventas-agregadas");
        String dlqTopic = env("DLQ_TOPIC", "ventas-raw-dlq");
        String applicationId = env("GROUP_ID", "ventas-aggregator");

        long windowSeconds = Long.parseLong(env("WINDOW_SECONDS", "60"));
        long graceSeconds = Long.parseLong(env("WINDOW_GRACE_SECONDS", "30"));
        long dedupRetentionHours = Long.parseLong(env("DEDUP_RETENTION_HOURS", "24"));

        if (windowSeconds <= 0 || graceSeconds < 0 || dedupRetentionHours <= 0) {
            throw new IllegalArgumentException(
                    "WINDOW_SECONDS debe ser positivo; WINDOW_GRACE_SECONDS no puede ser negativo; " +
                    "DEDUP_RETENTION_HOURS debe ser positivo");
        }

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
        props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 2);
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000);
        props.put(StreamsConfig.STATE_DIR_CONFIG, env("STREAMS_STATE_DIR", "/tmp/kafka-streams"));
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KafkaStreams streams = new KafkaStreams(
                buildTopology(rawTopic, aggTopic, dlqTopic,
                        windowSeconds, graceSeconds, dedupRetentionHours),
                props);

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

        LOGGER.info(
                "Kafka Streams iniciado. app={} raw={} agg={} dlq={} ventana={}s gracia={}s deduplicacion={}h",
                applicationId, rawTopic, aggTopic, dlqTopic,
                windowSeconds, graceSeconds, dedupRetentionHours);

        streams.start();
    }

    public static org.apache.kafka.streams.Topology buildTopology(
            String rawTopic,
            String aggTopic,
            long windowSeconds,
            long graceSeconds) {

        String dlqTopic = env("DLQ_TOPIC", "ventas-raw-dlq");
        long retentionHours = Long.parseLong(env("DEDUP_RETENTION_HOURS", "24"));

        return buildTopology(
                rawTopic, aggTopic, dlqTopic,
                windowSeconds, graceSeconds, retentionHours);
    }

    private static org.apache.kafka.streams.Topology buildTopology(
            String rawTopic,
            String aggTopic,
            String dlqTopic,
            long windowSeconds,
            long graceSeconds,
            long retentionHours) {

        if (windowSeconds <= 0 || graceSeconds < 0 || retentionHours <= 0) {
            throw new IllegalArgumentException("Configuración temporal inválida");
        }

        StreamsBuilder builder = new StreamsBuilder();

        long retentionMs = Duration.ofHours(retentionHours).toMillis();

        StoreBuilder<KeyValueStore<String, Long>> dedupStoreBuilder =
                Stores.keyValueStoreBuilder(
                        Stores.persistentKeyValueStore(DEDUP_STORE),
                        Serdes.String(),
                        Serdes.Long());

        StoreBuilder<KeyValueStore<String, Long>> lateStoreBuilder =
                Stores.keyValueStoreBuilder(
                        Stores.persistentKeyValueStore(LATE_STORE),
                        Serdes.String(),
                        Serdes.Long());

        builder.addStateStore(dedupStoreBuilder);
        builder.addStateStore(lateStoreBuilder);

        /*
         * Primera redistribución: agrupa por eventId para que un mismo
         * identificador de negocio siempre llegue a la misma partición.
         * Se conserva la clave original y el JSON sin modificar.
         */
        KStream<String, RawSale> source = builder
                .stream(rawTopic, Consumed.with(Serdes.String(), Serdes.String()))
                .mapValues((originalKey, rawValue) -> new RawSale(originalKey, rawValue));

        KStream<String, RawSale> byEventId = source
                .selectKey((key, raw) -> eventIdPartitionKey(raw.rawValue(), raw.originalKey()))
                .repartition(
                        Repartitioned.<String, RawSale>with(
                                        Serdes.String(),
                                        new JsonSerde<>(RawSale.class))
                                .withName("ventas-por-event-id"));

        /*
         * Validación y deduplicación.
         * El Transformer conserva el JSON original y establece el timestamp
         * de los eventos válidos a partir de eventTime.
         */
        KStream<String, ValidationResult> validated = byEventId.transform(
                () -> new ValidationTransformer(rawTopic, retentionMs),
                DEDUP_STORE);

        @SuppressWarnings("unchecked")
        KStream<String, ValidationResult>[] validationBranches = validated.branch(
                (key, result) -> result.sale() != null,
                (key, result) -> true);

        KStream<String, ValidationResult> validSales = validationBranches[0];
        KStream<String, ValidationResult> invalidSales = validationBranches[1];

        /*
         * DLQ de mensajes con JSON malformado o datos que no cumplen
         * los requisitos del evento de venta.
         */
        invalidSales
                .map((key, result) -> KeyValue.pair(
                        result.raw().originalKey(),
                        dlqJson(
                                result.raw(),
                                rawTopic,
                                result.errorType(),
                                result.reason())))
                .to(dlqTopic, Produced.with(Serdes.String(), Serdes.String()));

        /*
         * Segunda redistribución: agrupa por tienda.
         * Esto permite que el control de tardanza y la agregación
         * trabajen sobre la misma distribución por clave.
         */
        KStream<String, ValidatedSale> byStore = validSales
                .map((key, result) -> KeyValue.pair(
                        result.sale().store(),
                        new ValidatedSale(result.sale(), result.raw())))
                .repartition(
                        Repartitioned.<String, ValidatedSale>with(
                                        Serdes.String(),
                                        new JsonSerde<>(ValidatedSale.class))
                                .withName("ventas-store-events-repartition"));

        /*
         * Control explícito de eventos que llegan después del período
         * de gracia. Los eventos dentro de gracia continúan hacia la ventana.
         */
        KStream<String, LateResult> timeChecked = byStore.transform(
                () -> new LateEventTransformer(windowSeconds, graceSeconds),
                LATE_STORE);

        @SuppressWarnings("unchecked")
        KStream<String, LateResult>[] lateBranches = timeChecked.branch(
                (key, result) -> !result.late(),
                (key, result) -> result.late());

        KStream<String, LateResult> onTimeSales = lateBranches[0];
        KStream<String, LateResult> tooLateSales = lateBranches[1];

        /*
         * Los eventos tardíos se identifican como LATE_EVENT.
         * No se clasifican como errores de JSON.
         */
        tooLateSales
                .map((store, result) -> KeyValue.pair(
                        result.validated().raw().originalKey(),
                        dlqJson(
                                result.validated().raw(),
                                rawTopic,
                                "LATE_EVENT",
                                result.reason())))
                .to(dlqTopic, Produced.with(Serdes.String(), Serdes.String()));

        KStream<String, Sale> acceptedSales = onTimeSales
                .mapValues(result -> result.validated().sale());

        TimeWindows windows = TimeWindows.ofSizeAndGrace(
                Duration.ofSeconds(windowSeconds),
                Duration.ofSeconds(graceSeconds));

        KTable<Windowed<String>, TicketAggregate> aggregate = acceptedSales
                .groupByKey(Grouped.with(
                        Serdes.String(),
                        new JsonSerde<>(Sale.class)))
                .windowedBy(windows)
                .aggregate(
                        TicketAggregate::new,
                        (store, sale, totals) -> totals.add(sale),
                        Materialized.<String, TicketAggregate, WindowStore<Bytes, byte[]>>as(STATE_STORE)
                                .withKeySerde(Serdes.String())
                                .withValueSerde(new JsonSerde<>(TicketAggregate.class)));

        aggregate.toStream()
                .map((windowedKey, totals) -> KeyValue.pair(
                        windowedKey.key(),
                        resultJson(windowedKey, totals)))
                .to(aggTopic, Produced.with(Serdes.String(), Serdes.String()));

        return builder.build();
    }

    private static String eventIdPartitionKey(String json, String originalKey) {
        try {
            JsonNode node = MAPPER.readTree(json);
            JsonNode eventId = node == null ? null : node.get("eventId");

            if (eventId != null && eventId.isTextual() && !eventId.asText().isBlank()) {
                return eventId.asText();
            }
        } catch (Exception ignored) {
            // El evento será enviado a DLQ por el proceso de validación.
        }

        if (originalKey != null && !originalKey.isBlank()) {
            return originalKey;
        }

        return "__invalid__";
    }

    private static String validateSale(Sale sale) {
        if (sale == null) {
            return "JSON_INVALID: el contenido no representa una venta";
        }

        if (isBlank(sale.eventId())) {
            return "REQUIRED_FIELD_MISSING: eventId";
        }
        if (isBlank(sale.eventTime())) {
            return "REQUIRED_FIELD_MISSING: eventTime";
        }
        if (isBlank(sale.store())) {
            return "REQUIRED_FIELD_MISSING: store";
        }
        if (isBlank(sale.storeFormat())) {
            return "REQUIRED_FIELD_MISSING: storeFormat";
        }
        if (isBlank(sale.city())) {
            return "REQUIRED_FIELD_MISSING: city";
        }
        if (isBlank(sale.channel())) {
            return "REQUIRED_FIELD_MISSING: channel";
        }
        if (isBlank(sale.category())) {
            return "REQUIRED_FIELD_MISSING: category";
        }
        if (isBlank(sale.product())) {
            return "REQUIRED_FIELD_MISSING: product";
        }
        if (isBlank(sale.customerId())) {
            return "REQUIRED_FIELD_MISSING: customerId";
        }
        if (isBlank(sale.paymentMethod())) {
            return "REQUIRED_FIELD_MISSING: paymentMethod";
        }

        if (sale.quantity() <= 0) {
            return "BUSINESS_RULE_VIOLATION: quantity debe ser mayor que cero";
        }

        if (sale.unitPriceClp() <= 0) {
            return "BUSINESS_RULE_VIOLATION: unitPriceClp debe ser mayor que cero";
        }

        if (sale.totalClp() <= 0) {
            return "BUSINESS_RULE_VIOLATION: totalClp debe ser mayor que cero";
        }

        long expectedTotal;
        try {
            expectedTotal = Math.multiplyExact(
                    (long) sale.quantity(),
                    (long) sale.unitPriceClp());
        } catch (ArithmeticException e) {
            return "BUSINESS_RULE_VIOLATION: desbordamiento al calcular el total";
        }

        if (expectedTotal != sale.totalClp()) {
            return "BUSINESS_RULE_VIOLATION: totalClp no coincide con quantity * unitPriceClp";
        }

        String format = sale.storeFormat().toLowerCase(Locale.ROOT);
        String channel = sale.channel().toLowerCase(Locale.ROOT);

        if (!Set.of("tienda", "online").contains(format)) {
            return "INVALID_FIELD: storeFormat debe ser tienda u online";
        }

        if (!Set.of("presencial", "web").contains(channel)) {
            return "INVALID_FIELD: channel debe ser presencial o web";
        }

        if (format.equals("tienda") && !channel.equals("presencial")) {
            return "BUSINESS_RULE_VIOLATION: una tienda física debe usar canal presencial";
        }

        if (format.equals("online") && !channel.equals("web")) {
            return "BUSINESS_RULE_VIOLATION: una tienda online debe usar canal web";
        }

        String payment = sale.paymentMethod().toLowerCase(Locale.ROOT);
        if (!Set.of("débito", "crédito", "efectivo").contains(payment)) {
            return "INVALID_FIELD: paymentMethod no reconocido";
        }

        return null;
    }

    private static boolean isBlank(String value) {
        return value == null || value.isBlank();
    }

    private static String dlqJson(
            RawSale raw,
            String sourceTopic,
            String errorType,
            String reason) {

        DlqRecord record = new DlqRecord(
                raw.rawValue(),
                raw.originalKey(),
                sourceTopic,
                errorType,
                reason,
                Instant.now().toString());

        try {
            return MAPPER.writeValueAsString(record);
        } catch (Exception e) {
            throw new IllegalStateException("No se pudo serializar el registro DLQ", e);
        }
    }

    private static String resultJson(
            Windowed<String> windowedKey,
            TicketAggregate totals) {

        long average = totals.count == 0
                ? 0
                : Math.round((double) totals.totalClp / totals.count);

        WindowResult result = new WindowResult(
                windowedKey.window().startTime().toString(),
                windowedKey.window().endTime().toString(),
                windowedKey.key(),
                totals.count,
                totals.totalClp,
                average);

        try {
            return MAPPER.writeValueAsString(result);
        } catch (Exception e) {
            throw new IllegalStateException("No se pudo serializar el agregado", e);
        }
    }

    private static String env(String key, String fallback) {
        String value = System.getenv(key);
        return value == null || value.isBlank() ? fallback : value;
    }

    public record RawSale(String originalKey, String rawValue) {
    }

    public record ValidatedSale(Sale sale, RawSale raw) {
    }

    private record ValidationResult(
            RawSale raw,
            Sale sale,
            String errorType,
            String reason) {
    }

    private record LateResult(
            ValidatedSale validated,
            boolean late,
            String reason) {
    }

    private record DlqRecord(
            String originalValue,
            String originalKey,
            String sourceTopic,
            String errorType,
            String reason,
            String rejectedAt) {
    }

    private static final class ValidationTransformer
            implements Transformer<String, RawSale, KeyValue<String, ValidationResult>> {

        private final String sourceTopic;
        private final long retentionMs;

        private ProcessorContext context;
        private KeyValueStore<String, Long> dedupStore;

        private ValidationTransformer(String sourceTopic, long retentionMs) {
            this.sourceTopic = sourceTopic;
            this.retentionMs = retentionMs;
        }

        @Override
        @SuppressWarnings("unchecked")
        public void init(ProcessorContext context) {
            this.context = context;
            this.dedupStore = (KeyValueStore<String, Long>) context.getStateStore(DEDUP_STORE);

            context.schedule(
                    Duration.ofMinutes(1),
                    PunctuationType.STREAM_TIME,
                    this::expireOldEvents);
        }

        @Override
        public KeyValue<String, ValidationResult> transform(String key, RawSale raw) {
            if (raw == null || raw.rawValue() == null) {
                return reject(key, raw == null ? new RawSale(key, null) : raw,
                        "JSON_INVALID", "El valor del mensaje está vacío",
                        context.timestamp());
            }

            Sale sale;

            try {
                sale = MAPPER.readValue(raw.rawValue(), Sale.class);
            } catch (Exception e) {
                return reject(key, raw, "JSON_MALFORMED",
                        "No se pudo interpretar el JSON: " + e.getMessage(),
                        context.timestamp());
            }

            if (sale == null) {
                return reject(key, raw, "JSON_INVALID",
                        "El JSON representa un valor nulo",
                        context.timestamp());
            }

            Instant eventInstant;

            try {
                eventInstant = Instant.parse(sale.eventTime());
            } catch (Exception e) {
                return reject(key, raw, "INVALID_EVENT_TIME",
                        "eventTime debe ser una fecha ISO-8601 válida",
                        context.timestamp());
            }

            long eventTimestamp = eventInstant.toEpochMilli();

            if (eventTimestamp < 0) {
                return reject(key, raw, "INVALID_EVENT_TIME",
                        "eventTime no puede ser anterior al epoch Unix",
                        context.timestamp());
            }

            String validationError = validateSale(sale);

            if (validationError != null) {
                return reject(key, raw, "VALIDATION_ERROR",
                        validationError, eventTimestamp);
            }

            Long previousTimestamp = dedupStore.get(sale.eventId());

            if (previousTimestamp != null) {
                long difference = eventTimestamp >= previousTimestamp
                        ? eventTimestamp - previousTimestamp
                        : previousTimestamp - eventTimestamp;

                if (difference <= retentionMs) {
                    LOGGER.info(
                            "Evento duplicado ignorado. topic={} key={} eventId={}",
                            sourceTopic, raw.originalKey(), sale.eventId());
                    return null;
                }
            }

            dedupStore.put(sale.eventId(), eventTimestamp);

            ValidationResult result = new ValidationResult(
                    raw, sale, null, null);

            context.forward(
                    key,
                    result,
                    To.all().withTimestamp(eventTimestamp));

            return null;
        }

        private KeyValue<String, ValidationResult> reject(
                String key,
                RawSale raw,
                String errorType,
                String reason,
                long timestamp) {

            LOGGER.warn(
                    "Evento rechazado. topic={} key={} tipo={} motivo={}",
                    sourceTopic,
                    raw.originalKey(),
                    errorType,
                    reason);

            context.forward(
                    key,
                    new ValidationResult(raw, null, errorType, reason),
                    To.all().withTimestamp(Math.max(0, timestamp)));

            return null;
        }

        private void expireOldEvents(long streamTime) {
            List<String> expiredKeys = new ArrayList<>();

            try (KeyValueIterator<String, Long> iterator = dedupStore.all()) {
                while (iterator.hasNext()) {
                    KeyValue<String, Long> entry = iterator.next();

                    if (entry.value != null
                            && streamTime > entry.value
                            && streamTime - entry.value > retentionMs) {
                        expiredKeys.add(entry.key);
                    }
                }
            }

            for (String key : expiredKeys) {
                dedupStore.delete(key);
            }
        }

        @Override
        public void close() {
        }
    }

    private static final class LateEventTransformer
            implements Transformer<String, ValidatedSale, KeyValue<String, LateResult>> {

        private final long windowMs;
        private final long graceMs;

        private ProcessorContext context;
        private KeyValueStore<String, Long> lateStore;

        private LateEventTransformer(long windowSeconds, long graceSeconds) {
            this.windowMs = Duration.ofSeconds(windowSeconds).toMillis();
            this.graceMs = Duration.ofSeconds(graceSeconds).toMillis();
        }

        @Override
        @SuppressWarnings("unchecked")
        public void init(ProcessorContext context) {
            this.context = context;
            this.lateStore = (KeyValueStore<String, Long>) context.getStateStore(LATE_STORE);
        }

        @Override
        public KeyValue<String, LateResult> transform(
                String store,
                ValidatedSale validated) {

            long eventTimestamp = context.timestamp();

            Long previousMax = lateStore.get(MAX_STREAM_TIME_KEY);
            long maxStreamTime = previousMax == null
                    ? eventTimestamp
                    : Math.max(previousMax, eventTimestamp);

            lateStore.put(MAX_STREAM_TIME_KEY, maxStreamTime);

            long windowStart = Math.floorDiv(eventTimestamp, windowMs) * windowMs;
            long windowEnd = windowStart + windowMs;
            long closeTime = windowEnd + graceMs;

            boolean isLate = maxStreamTime > closeTime;

            String reason = null;

            if (isLate) {
                reason = "eventTime=" + Instant.ofEpochMilli(eventTimestamp)
                        + ", windowEnd=" + Instant.ofEpochMilli(windowEnd)
                        + ", graceSeconds=" + (graceMs / 1000)
                        + ", maxStreamTime=" + Instant.ofEpochMilli(maxStreamTime);

                LOGGER.warn(
                        "Evento tardío descartado. store={} eventId={} motivo={}",
                        store,
                        validated.sale().eventId(),
                        reason);
            }

            return KeyValue.pair(
                    store,
                    new LateResult(validated, isLate, reason));
        }

        @Override
        public void close() {
        }
    }
}
