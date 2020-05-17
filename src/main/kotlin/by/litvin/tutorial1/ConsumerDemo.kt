package by.litvin.tutorial1

import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig.*
import org.apache.kafka.clients.producer.ProducerRecord
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.util.*

fun main() {
    val logger: Logger = LoggerFactory.getLogger("Runner")

    val properties = Properties().apply {
        setProperty(BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
        setProperty(KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
        setProperty(VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    }

    val producer = KafkaProducer<String, String>(properties)

    (0 .. 10).forEach { i ->
        val producerRecord: ProducerRecord<String, String> =
            ProducerRecord("first_topic", "key_$i","hello from java + $i")
        logger.info("Key $i")
        producer.send(producerRecord) { metadata, exception ->
            if (exception == null) {
                with(metadata) {
                    logger.info("Received new metadata \n" +
                            "Topic ${topic()} \n" +
                            "Partition ${partition()} \n" +
                            "Offset ${offset()} \n" +
                            "Timestamp ${timestamp()}")
                }
            } else {
                logger.error("Error while producing", exception)
            }
        }
    }

    producer.flush()
    producer.close()

}