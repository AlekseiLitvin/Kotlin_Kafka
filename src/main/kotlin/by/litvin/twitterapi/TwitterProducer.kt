package by.litvin.twitterapi

import com.twitter.hbc.ClientBuilder
import com.twitter.hbc.core.Client
import com.twitter.hbc.core.Constants
import com.twitter.hbc.core.HttpHosts
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint
import com.twitter.hbc.core.processor.StringDelimitedProcessor
import com.twitter.hbc.httpclient.auth.OAuth1
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig.*
import org.apache.kafka.clients.producer.ProducerRecord
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.util.*
import java.util.concurrent.BlockingQueue
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit.SECONDS

class TwitterProducer {

    companion object {
        val logger: Logger = LoggerFactory.getLogger(TwitterProducer::class.java.name)
    }

    fun run() {
        val msgQueue = LinkedBlockingQueue<String>(1000)
        val twitterClient = createTwitterClient(msgQueue)

        twitterClient.connect()

        val kafkaProducer = createKafkaProducer()

        while (!twitterClient.isDone) {
            val msg = try {
                msgQueue.poll(5, SECONDS)
            } catch (e: InterruptedException) {
                twitterClient.stop()
                null
            }
            msg.let {
                logger.info(it)
                kafkaProducer.send(ProducerRecord("twitter_tweets", it)) { _, exception ->
                    exception?.let {
                        logger.error("Error happened", exception)
                    }
                }
            }
        }
        logger.info("End of app")
    }

    private fun createKafkaProducer(): KafkaProducer<String, String> {
        val properties = Properties().apply {
            setProperty(BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
            setProperty(KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
            setProperty(VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")

            //safe producer
            setProperty(ENABLE_IDEMPOTENCE_CONFIG, "true")
            setProperty(ACKS_CONFIG, "all")
            setProperty(RETRIES_CONFIG, Int.MAX_VALUE.toString())
            setProperty(MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5")

            setProperty(LINGER_MS_CONFIG, "20")
            setProperty(BATCH_SIZE_CONFIG, (32 * 1024).toString())
        }

        return KafkaProducer<String, String>(properties)
    }

    private fun createTwitterClient(msgQueue: BlockingQueue<String>): Client {
        val hosebirdHosts = HttpHosts(Constants.STREAM_HOST)
        val hosebirdEndpoint = StatusesFilterEndpoint()
        val terms = arrayListOf("kafka", "COVID-19", "news")
        hosebirdEndpoint.trackTerms(terms)

        val hosebirdAuth = OAuth1(
            "hidden",
            "hidden",
            "hidden",
            "hidden")

        return ClientBuilder()
            .name("Hosebird-Client-01")
            .hosts(hosebirdHosts)
            .authentication(hosebirdAuth)
            .endpoint(hosebirdEndpoint)
            .processor(StringDelimitedProcessor(msgQueue))
            .build()
    }
}