package by.litvin.tutorial1

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.WakeupException
import org.apache.kafka.common.serialization.StringDeserializer
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.time.Duration
import java.util.*
import java.util.concurrent.CountDownLatch
import kotlin.concurrent.thread

fun main() {
    val latch = CountDownLatch(1)
    val consumerThreads = ConsumerThreads(latch)
    thread(start = true) {
        Thread(consumerThreads)
    }

    Runtime.getRuntime().addShutdownHook(Thread(Runnable {
        println("Cough shutdown hook")
        consumerThreads.shutdown()
        latch.await()
    }))

    try {
        latch.await()
    } finally {
        print("app is closing")
    }

}

class ConsumerThreads(val latch: CountDownLatch) : Runnable {

    companion object {
        val logger: Logger = LoggerFactory.getLogger(ConsumerThreads::class.qualifiedName)
    }

    private var consumer: KafkaConsumer<String, String>

    init {
        val properties = Properties().apply {
            setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
            setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java.name)
            setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java.name)
            setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
        }

        consumer = KafkaConsumer<String, String>(properties)

        val topicPartition = TopicPartition("first_topic", 0)
        val offsetToReadFrom = 15L
        consumer.assign(listOf(topicPartition))
        consumer.seek(topicPartition, offsetToReadFrom)
    }

    override fun run() {
        try {
            while (true) {
                val consumerRecords = consumer.poll(Duration.ofMillis(100))

                consumerRecords.forEach {
                    with(it) {
                        logger.info("Key ${key()}, Value ${value()}")
                        logger.info("Partition ${partition()}, Offset ${offset()}")
                    }
                }
            }
        } catch (e: WakeupException) {
            logger.info("Received shutdown signal")
        } finally {
            consumer.close()
            latch.countDown()
        }
    }

    fun shutdown() {
        consumer.wakeup()
    }
}