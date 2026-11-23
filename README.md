# Streaming de ventas con Kafka y Kafka Streams

Proyecto de procesamiento de ventas en tiempo real desarrollado para DSY1107 – Desarrollo Cloud Native I.

## 1. Arquitectura

La solución utiliza dos brokers Kafka, ZooKeeper, un productor Java, un procesador Kafka Streams y una plataforma de monitoreo.

| Componente | Dirección | Función |
|---|---|---|
| Kafka 1 + ZooKeeper | 10.0.0.21 | Broker principal y coordinación |
| Kafka 2 | 10.0.0.81 | Segunda réplica |
| Aplicación | 10.0.0.4 | Productor y Kafka Streams |
| Monitoreo | 10.0.0.38 | Prometheus, Grafana y Loki |

El acceso a las máquinas se realiza mediante AWS Systems Manager Session Manager.

### Flujo de procesamiento

1. ProducerApp genera eventos de venta en formato JSON.
2. Los eventos se publican en `ventas-raw`.
3. Kafka Streams valida los datos y detecta duplicados mediante `eventId`.
4. Los eventos aceptados se agrupan por tienda y ventana temporal.
5. Los resultados se publican en `ventas-agregadas`.
6. Los registros inválidos, duplicados y tardíos reciben el tratamiento correspondiente. Los inválidos y tardíos se conservan en `ventas-raw-dlq`; los duplicados se descartan y quedan registrados en los logs.

La clave original del productor es `customerId`. Para la deduplicación se redistribuyen los eventos por `eventId`, y para la agregación se utiliza la tienda como clave.

## 2. Tecnologías

- Java 21.
- Apache Kafka 3.9.2.
- Kafka Streams 3.9.2.
- Jackson 2.17.2.
- Maven 3.9.
- Docker y Docker Compose.
- JUnit 5.
- Kafka Streams Test Utils.
- Prometheus, Grafana y Loki.

## 3. Tópicos Kafka

| Tópico | Particiones | Replicación | Uso |
|---|---:|---:|---|
| ventas-raw | 4 | 2 | Ventas originales |
| ventas-agregadas | 2 | 2 | Resultados por tienda |
| ventas-raw-dlq | 4 | 2 | Registros rechazados |

El tópico `ventas-raw-dlq` tiene retención de siete días. La configuración del clúster utiliza `min.insync.replicas=1`.

Kafka Streams también genera tópicos internos para redistribución y recuperación de sus almacenes de estado. Estos tópicos se crean automáticamente.

Con dos brokers no corresponde utilizar un factor de replicación 3.

## 4. Configuración

La aplicación recibe sus variables desde `app.env`, ubicado junto a `docker-compose.app.yaml`.

| Variable | Descripción |
|---|---|
| KAFKA_BOOTSTRAP | Direcciones de los brokers |
| RAW_TOPIC | Tópico de entrada |
| AGG_TOPIC | Tópico de agregados |
| DLQ_TOPIC | Tópico de rechazos |
| GROUP_ID | Identificador de Kafka Streams |
| WINDOW_SECONDS | Duración de la ventana |
| WINDOW_GRACE_SECONDS | Período de gracia |
| DEDUP_RETENTION_HOURS | Retención de identificadores procesados |
| PRODUCE_INTERVAL | Intervalo de producción |
| PRODUCER_MAX_EVENTS | Límite de eventos; 0 significa ilimitado |
| CUSTOMER_POOL | Cantidad de clientes disponibles |
| APP_PRIVATE_IP | IP privada de la EC2 de aplicación |

Los valores sensibles y las direcciones específicas de cada ambiente se mantienen fuera del código fuente.

Configuración temporal utilizada:

- Ventanas tumbling de 60 segundos.
- Período de gracia de 30 segundos.
- Retención de identificadores duplicados de 24 horas.

Los resultados de agregación se publican como actualizaciones al aceptar ventas; no se espera al cierre de la ventana para emitir cada actualización.

## 5. Ejecución

### Iniciar Kafka 1 y ZooKeeper

En la EC2 10.0.0.21:

    sudo docker compose --env-file broker-a.env -f docker-compose.broker.yaml up -d

### Iniciar Kafka 2

En la EC2 10.0.0.81:

    sudo docker compose --env-file broker-b.env -f docker-compose.broker.yaml up -d

### Iniciar la aplicación

En la EC2 10.0.0.4:

    cd /home/ssm-user
    sudo docker compose --env-file app.env -f docker-compose.app.yaml up -d --build

La aplicación contiene dos servicios:

- `producer`: genera y publica las ventas.
- `consumer`: ejecuta Kafka Streams.

Consultar el estado:

    sudo docker compose --env-file app.env -f docker-compose.app.yaml ps

Consultar registros:

    sudo docker compose --env-file app.env -f docker-compose.app.yaml logs --tail=50 producer consumer

Detener solamente el productor:

    sudo docker compose --env-file app.env -f docker-compose.app.yaml stop producer

### Iniciar monitoreo

En la EC2 de monitoreo:

    cd ~/monitoring
    sudo docker compose --env-file monitoring.env -f docker-compose.monitoring.yaml up -d

Interfaces disponibles mediante acceso de red autorizado o túneles SSM:

- Grafana: puerto 3000.
- Prometheus: puerto 9090.
- Loki: puerto 3100.

El dashboard de Grafana utilizado es `EA3 - Observabilidad integral Kafka y ventas`.

## 6. Crear e inspeccionar tópicos

Los comandos siguientes se ejecutan en Kafka 1.

Crear el tópico de entrada:

    sudo docker exec -e JAVA_TOOL_OPTIONS= kafka kafka-topics --bootstrap-server 10.0.0.21:9092 --create --if-not-exists --topic ventas-raw --partitions 4 --replication-factor 2 --config min.insync.replicas=1

Crear el tópico de agregados:

    sudo docker exec -e JAVA_TOOL_OPTIONS= kafka kafka-topics --bootstrap-server 10.0.0.21:9092 --create --if-not-exists --topic ventas-agregadas --partitions 2 --replication-factor 2 --config min.insync.replicas=1

Crear la DLQ:

    sudo docker exec -e JAVA_TOOL_OPTIONS= kafka kafka-topics --bootstrap-server 10.0.0.21:9092 --create --if-not-exists --topic ventas-raw-dlq --partitions 4 --replication-factor 2 --config retention.ms=604800000

Describir un tópico:

    timeout 12s sudo docker exec -e JAVA_TOOL_OPTIONS= kafka kafka-topics --bootstrap-server 10.0.0.21:9092 --describe --topic ventas-raw

Listar tópicos:

    sudo docker exec -e JAVA_TOOL_OPTIONS= kafka kafka-topics --bootstrap-server 10.0.0.21:9092 --list

Consultar grupos de consumidores:

    sudo docker exec -e JAVA_TOOL_OPTIONS= kafka kafka-consumer-groups --bootstrap-server 10.0.0.21:9092 --list

Consultar el grupo de Kafka Streams:

    sudo docker exec -e JAVA_TOOL_OPTIONS= kafka kafka-consumer-groups --bootstrap-server 10.0.0.21:9092 --describe --group ventas-aggregator

## 7. Pruebas automatizadas

Las pruebas se encuentran en:

    app/src/test/java/cl/duoc/ventas/StreamsAppTest.java

Para ejecutarlas se utiliza Maven con Java 21 mediante un contenedor temporal:

    sudo docker run --rm -v /home/ssm-user/app:/workspace -w /workspace maven:3.9-eclipse-temurin-21 mvn -B test

Las seis pruebas automatizadas ejecutadas fueron aprobadas:

| Prueba | Resultado |
|---|---|
| JSON malformado enviado a DLQ | Aprobada |
| Total incorrecto enviado a DLQ | Aprobada |
| Duplicado por eventId | Aprobada |
| Agregación por tienda | Aprobada |
| Evento tardío dentro de gracia | Aprobada |
| Evento tardío fuera de gracia | Aprobada |

Resultado registrado: 6 pruebas, 0 fallidas, 0 errores y 0 omitidas.

## 8. Escenarios de operación y recuperación

### Mensajes inválidos

Los registros que no cumplen el formato JSON o las reglas de negocio se envían a `ventas-raw-dlq`.

La estructura conserva:

- Valor original.
- Clave original.
- Tópico de origen.
- Tipo de error.
- Motivo del rechazo.
- Fecha del rechazo.

El consumidor continúa procesando los siguientes registros.

### Duplicados

Se utiliza `eventId` y un almacén persistente de estado de Kafka Streams para evitar contabilizar dos veces una misma venta durante el período configurado.

`EXACTLY_ONCE_V2` protege las operaciones de procesamiento y sus transacciones, pero no reemplaza la deduplicación de negocio.

### Eventos tardíos

Los eventos dentro del período de gracia pueden incorporarse a su ventana original.

Los que superan el período permitido se identifican como `LATE_EVENT`, se registran y se envían a la DLQ.

### Caída de un broker

Para detener Kafka 2:

    sudo docker compose --env-file broker-b.env -f docker-compose.broker.yaml stop kafka

Para recuperarlo:

    sudo docker compose --env-file broker-b.env -f docker-compose.broker.yaml start kafka

Durante la prueba se observó:

- ISR inicial: 2,1.
- ISR durante la caída: 1.
- ISR después de la recuperación: 1,2.

El productor continuó publicando durante la interrupción. El factor de replicación es 2 y `min.insync.replicas=1`, por lo que el clúster puede mantener escrituras confirmadas con una sola réplica sincronizada.

### Recuperación de Kafka Streams

Al reiniciar la aplicación, Kafka Streams restaura el estado mediante los tópicos changelog y continúa desde los offsets confirmados.

En una ejecución se registró la restauración de 42.336 registros del changelog y la transición del cliente al estado `RUNNING`.

## 9. Límites conocidos

- ZooKeeper se ejecuta en un único nodo y constituye un punto único de falla.
- El clúster tiene dos brokers; durante una caída se pierde temporalmente la redundancia.
- La política de deduplicación conserva identificadores durante 24 horas. Eventos repetidos después de ese período pueden volver a ser aceptados.
- La tolerancia de eventos tardíos depende del tiempo máximo de eventos observado por las tareas de Kafka Streams.
- El sistema requiere conectividad con los brokers y disponibilidad de los servicios de AWS.

## 10. Seguridad y repositorio

No se deben versionar archivos que contengan credenciales, contraseñas, tokens ni valores privados del ambiente.

Se deben mantener fuera del repositorio:

- `app.env`
- `broker-a.env`
- `broker-b.env`
- `monitoring.env`

Los archivos de ejemplo pueden versionarse siempre que no contengan secretos reales.

Antes de entregar, revisar el estado de Git, los archivos incluidos y el historial para evitar publicar credenciales.
