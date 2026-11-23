package cl.duoc.ventas;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

class StreamsAppTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static final String RAW = "ventas-raw";
    private static final String AGG = "ventas-agregadas";
    private static final String DLQ = "ventas-raw-dlq";

    private TestContext createContext() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "ventas-test");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, "org.apache.kafka.common.serialization.Serdes$StringSerde");
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, "org.apache.kafka.common.serialization.Serdes$StringSerde");
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);

        TopologyTestDriver driver = new TopologyTestDriver(
                StreamsApp.buildTopology(RAW, AGG, 60, 30),
                props);

        TestInputTopic<String, String> input = driver.createInputTopic(
                RAW, new StringSerializer(), new StringSerializer());

        TestOutputTopic<String, String> aggregate = driver.createOutputTopic(
                AGG, new StringDeserializer(), new StringDeserializer());

        TestOutputTopic<String, String> dlq = driver.createOutputTopic(
                DLQ, new StringDeserializer(), new StringDeserializer());

        return new TestContext(driver, input, aggregate, dlq);
    }

    @Test
    void debeEnviarJsonMalformadoALaDlq() throws Exception {
        try (TestContext context = createContext()) {
            context.input.pipeInput(
                    "qa-invalid",
                    "{json-roto",
                    Instant.ofEpochMilli(10_000));

            assertEquals(1, context.dlq.getQueueSize());

            JsonNode rejected = MAPPER.readTree(context.dlq.readValue());

            assertEquals("{json-roto", rejected.get("originalValue").asText());
            assertEquals("qa-invalid", rejected.get("originalKey").asText());
            assertEquals(RAW, rejected.get("sourceTopic").asText());
            assertEquals("JSON_MALFORMED", rejected.get("errorType").asText());
            assertTrue(rejected.get("reason").asText().contains("JSON"));
        }
    }

    @Test
    void debeEnviarVentaConTotalIncorrectoALaDlq() throws Exception {
        try (TestContext context = createContext()) {
            context.input.pipeInput(
                    "qa-customer",
                    saleJson("invalid-total", 10_000, "QA Store", 2, 1000, 1500),
                    Instant.ofEpochMilli(10_000));

            assertEquals(1, context.dlq.getQueueSize());

            JsonNode rejected = MAPPER.readTree(context.dlq.readValue());

            assertEquals("VALIDATION_ERROR", rejected.get("errorType").asText());
            assertTrue(rejected.get("reason").asText().contains("quantity * unitPriceClp"));
            assertTrue(rejected.get("originalValue").asText().contains("invalid-total"));
            assertTrue(context.aggregate.isEmpty());
        }
    }

    @Test
    void debeIgnorarDuplicadoSinInflarAgregado() throws Exception {
        try (TestContext context = createContext()) {
            String sale = saleJson(
                    "duplicate-001", 10_000, "QA Duplicate Store", 2, 1000, 2000);

            context.input.pipeInput("qa-customer", sale, Instant.ofEpochMilli(10_000));
            context.input.pipeInput("qa-customer", sale, Instant.ofEpochMilli(10_000));

            List<String> results = context.aggregate.readValuesToList();

            assertEquals(1, results.size());

            JsonNode aggregate = MAPPER.readTree(results.get(0));

            assertEquals("QA Duplicate Store", aggregate.get("store").asText());
            assertEquals(1, aggregate.get("sales_count").asInt());
            assertEquals(2000, aggregate.get("total_clp").asLong());
            assertEquals(2000, aggregate.get("avg_ticket_clp").asLong());
            assertTrue(context.dlq.isEmpty());
        }
    }

    @Test
    void debeCalcularAgregadoPorTienda() throws Exception {
        try (TestContext context = createContext()) {
            context.input.pipeInput(
                    "customer-1",
                    saleJson("aggregate-001", 10_000, "QA Aggregate Store", 2, 1000, 2000),
                    Instant.ofEpochMilli(10_000));

            context.input.pipeInput(
                    "customer-2",
                    saleJson("aggregate-002", 20_000, "QA Aggregate Store", 1, 1500, 1500),
                    Instant.ofEpochMilli(20_000));

            List<String> results = context.aggregate.readValuesToList();

            assertEquals(2, results.size());

            JsonNode lastAggregate = MAPPER.readTree(results.get(results.size() - 1));

            assertEquals("QA Aggregate Store", lastAggregate.get("store").asText());
            assertEquals(2, lastAggregate.get("sales_count").asInt());
            assertEquals(3500, lastAggregate.get("total_clp").asLong());
            assertEquals(1750, lastAggregate.get("avg_ticket_clp").asLong());
            assertNotNull(lastAggregate.get("window_start"));
            assertNotNull(lastAggregate.get("window_end"));
        }
    }

    @Test
    void debeAceptarEventoTardioDentroDeGracia() throws Exception {
        try (TestContext context = createContext()) {
            context.input.pipeInput(
                    "customer-later",
                    saleJson("grace-later", 65_000, "QA Grace Store", 1, 1000, 1000),
                    Instant.ofEpochMilli(65_000));

            context.input.pipeInput(
                    "customer-earlier",
                    saleJson("grace-earlier", 55_000, "QA Grace Store", 1, 1234, 1234),
                    Instant.ofEpochMilli(55_000));

            List<String> results = context.aggregate.readValuesToList();

            assertEquals(2, results.size());

            boolean earlierWindowAccepted = results.stream().anyMatch(json -> {
                try {
                    JsonNode node = MAPPER.readTree(json);
                    return node.get("store").asText().equals("QA Grace Store")
                            && node.get("total_clp").asLong() == 1234;
                } catch (Exception e) {
                    return false;
                }
            });

            assertTrue(earlierWindowAccepted);
            assertTrue(context.dlq.isEmpty());
        }
    }

    @Test
    void debeEnviarEventoFueraDeGraciaALaDlq() throws Exception {
        try (TestContext context = createContext()) {
            context.input.pipeInput(
                    "customer-current",
                    saleJson("late-current", 100_000, "QA Late Store", 1, 1000, 1000),
                    Instant.ofEpochMilli(100_000));

            context.input.pipeInput(
                    "customer-old",
                    saleJson("late-old", 50_000, "QA Late Store", 1, 2000, 2000),
                    Instant.ofEpochMilli(50_000));

            assertEquals(1, context.dlq.getQueueSize());

            JsonNode rejected = MAPPER.readTree(context.dlq.readValue());

            assertEquals("LATE_EVENT", rejected.get("errorType").asText());
            assertTrue(rejected.get("reason").asText().contains("graceSeconds=30"));
            assertTrue(rejected.get("originalValue").asText().contains("late-old"));
        }
    }

    private static String saleJson(
            String eventId,
            long timestamp,
            String store,
            int quantity,
            int unitPrice,
            int total) throws Exception {

        Sale sale = new Sale(
                eventId,
                Instant.ofEpochMilli(timestamp).toString(),
                store,
                "tienda",
                "Santiago",
                "presencial",
                "prueba",
                "Producto QA",
                quantity,
                unitPrice,
                total,
                "qa-customer",
                "débito");

        return MAPPER.writeValueAsString(sale);
    }

    private record TestContext(
            TopologyTestDriver driver,
            TestInputTopic<String, String> input,
            TestOutputTopic<String, String> aggregate,
            TestOutputTopic<String, String> dlq) implements AutoCloseable {

        @Override
        public void close() {
            driver.close();
        }
    }
}
