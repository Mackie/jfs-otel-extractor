package com.pdig.streams.otel.processing

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.pdig.streams.otel.config.json.serde.JacksonSerde
import com.pdig.streams.otel.events.OpenTelemetryEvent
import mu.KotlinLogging
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.kstream.Consumed
import org.apache.kafka.streams.kstream.KStream
import org.apache.kafka.streams.kstream.Produced
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.config.TopicBuilder
import org.springframework.kafka.core.KafkaAdmin

@Configuration
class Stream {

    private val logger = KotlinLogging.logger {}

    @Bean
    fun appTopics(): KafkaAdmin.NewTopics {
        return KafkaAdmin.NewTopics(
            TopicBuilder.name(MAIN).build(),
            TopicBuilder.name(OUTPUT).build(),
        )
    }

    companion object {
        const val MAIN = "otel_traces"
        const val OUTPUT = "tracking_events_raw"
    }

    @Bean
    fun topology(
        streamsBuilder: StreamsBuilder,
        processorSupplier: ProcessorSupplier,
        jackson: ObjectMapper
    ): KStream<String, OpenTelemetryEvent> {
        val serdeKey = Serdes.String()
        val serdeValueJson = JacksonSerde(jackson, JsonNode::class.java)

        val consumedWith: Consumed<String, String> = Consumed.with(serdeKey, Serdes.String())
        val producedWith = Produced.with(serdeKey, serdeValueJson)

        val input = streamsBuilder.stream(MAIN, consumedWith)
        val transformed =
            input.mapValues { readOnlyKey, value -> jackson.readValue(value, OpenTelemetryEvent::class.java) }

        transformed.peek { key, value -> logger.info("Consume msg with key $key and value $value") }

        transformed.filter { _, value -> value != null }
        val out = transformed.process(processorSupplier)

        out.peek { key, value -> logger.info("Receive msg with key $key and value $value") }

        out.to(OUTPUT, producedWith)
        return transformed
    }
}
