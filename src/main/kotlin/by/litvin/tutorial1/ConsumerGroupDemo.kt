package by.litvin

import org.apache.kafka.clients.consumer.ConsumerConfig.*
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer
import org.slf4j.LoggerFactory
import java.time.Duration
import java.time.temporal.TemporalUnit
import java.util.*

fun main() {
    val logger = LoggerFactory.getLogger("Consumer demo")

    val properties = Properties().apply {
        setProperty(BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
        setProperty(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java.name)
        setProperty(VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java.name)
        setProperty(GROUP_ID_CONFIG, "my-fourth-app")
        setProperty(AUTO_OFFSET_RESET_CONFIG, "earliest")
    }

    val consumer = KafkaConsumer<String, String>(properties)

    consumer.subscribe(listOf("first_topic"))

    while (true) {
        val consumerRecords = consumer.poll(Duration.ofMillis(100))

        consumerRecords.forEach {
            with(it) {
                logger.info("Key ${key()}, Value ${value()}")
                logger.info("Partition ${partition()}, Offset ${offset()}")
            }
        }
    }
}