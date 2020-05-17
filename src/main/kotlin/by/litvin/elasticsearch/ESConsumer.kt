package by.litvin.elasticsearch

import com.google.gson.JsonParser
import org.apache.http.HttpHost
import org.apache.http.auth.AuthScope
import org.apache.http.auth.UsernamePasswordCredentials
import org.apache.http.impl.client.BasicCredentialsProvider
import org.apache.kafka.clients.consumer.ConsumerConfig.*
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer
import org.elasticsearch.action.bulk.BulkRequest
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.RequestOptions
import org.elasticsearch.client.RestClient
import org.elasticsearch.client.RestHighLevelClient
import org.elasticsearch.common.xcontent.XContentType
import java.time.Duration
import java.util.*

class ESConsumer {

    companion object {
        fun createClient(): RestHighLevelClient {
            val hostname = "hidden"
            val username = "hidden"
            val password = "hidden"

            val credentialsProvider = BasicCredentialsProvider()
            credentialsProvider.setCredentials(AuthScope.ANY, UsernamePasswordCredentials(username, password))

            val builder = RestClient.builder(HttpHost(hostname, 443, "https"))
                .setHttpClientConfigCallback { it.setDefaultCredentialsProvider(credentialsProvider) }

            return RestHighLevelClient(builder)
        }

        fun createKafkaConsumer(topic: String): KafkaConsumer<String, String> {
            val properties = Properties().apply {
                setProperty(BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
                setProperty(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java.name)
                setProperty(VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java.name)
                setProperty(GROUP_ID_CONFIG, "kafka-demo-elasticsearch")
                setProperty(AUTO_OFFSET_RESET_CONFIG, "earliest")
                setProperty(ENABLE_AUTO_COMMIT_CONFIG, "false")
                setProperty(MAX_POLL_RECORDS_CONFIG, "100")
            }

            return KafkaConsumer<String, String>(properties).also {
                it.subscribe(listOf(topic))
            }
        }
    }
}

fun main() {
    val client = ESConsumer.createClient()

    val consumer = ESConsumer.createKafkaConsumer("twitter_tweets")
    while (true) {
        val consumerRecords = consumer.poll(Duration.ofMillis(100))

        val count = consumerRecords.count()
        println("Received $count records")
        val bulkRequest = BulkRequest()

        consumerRecords.forEach { consumerRecord ->
//            val id = with(consumerRecord) { "${topic()}_${partition()}_${offset()}" }
            val id = extractIdFromTweets(consumerRecord.value())

            val indexRequest = IndexRequest("twitter")
                .id(id)
                .source(consumerRecord.value(), XContentType.JSON)

            bulkRequest.add(indexRequest)
        }
        if (count > 0) {
            val bulkResponse = client.bulk(bulkRequest, RequestOptions.DEFAULT)
            println("Committing offsets...")
            consumer.commitSync()
            println("Offsets have been committed")
            Thread.sleep(1000)
        }
    }


}

fun extractIdFromTweets(twitJson: String): String {
    return JsonParser.parseString(twitJson)
        .asJsonObject["id_str"]
        .asString
}
