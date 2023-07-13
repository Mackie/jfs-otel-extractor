package com.pdig.vehicle.streams.vehiclestreamprocessor

import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.pdig.streams.otel.config.json.JacksonConfig
import com.pdig.streams.otel.config.json.serde.JacksonSerde
import com.pdig.streams.otel.events.EventProcessor
import com.pdig.streams.otel.events.OpenTelemetryEvent
import com.pdig.streams.otel.processing.ProcessorSupplier
import com.pdig.streams.otel.processing.Stream
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.TestInputTopic
import org.apache.kafka.streams.TestOutputTopic
import org.apache.kafka.streams.TopologyTestDriver
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.io.File

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class OpenTelemetryExtractorStreamProcessorTests {

    private val objectMapper = JacksonConfig().jackson()
    private val eventProcessor = EventProcessor(objectMapper)
    private lateinit var testDriver: TopologyTestDriver
    private lateinit var mainTopic: TestInputTopic<String, OpenTelemetryEvent>
    private lateinit var outputTopic: TestOutputTopic<String, JsonNode>

    @BeforeEach
    fun before() {

        val streamsBuilder = StreamsBuilder()
        Stream().topology(streamsBuilder, ProcessorSupplier(eventProcessor), objectMapper)
        testDriver = TopologyTestDriver(streamsBuilder.build())

        mainTopic = testDriver.createInputTopic(
            Stream.MAIN,
            Serdes.String().serializer(),
            JacksonSerde(objectMapper, OpenTelemetryEvent::class.java).serializer()
        )
        outputTopic = testDriver.createOutputTopic(
            Stream.OUTPUT,
            Serdes.String().deserializer(),
            JacksonSerde(objectMapper, JsonNode::class.java).deserializer()
        )
    }

    @AfterEach
    fun afterEach() {
        testDriver.close()
    }

    @Test
    fun `processor emits correct output event if details topic for vin is received`() {

        val key = null
        val objectMapper = jacksonObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
        val event = objectMapper.readValue(File("src/test/resources/test-event.json"), OpenTelemetryEvent::class.java)
        mainTopic.pipeInput(key, event)

        assertFalse(outputTopic.isEmpty)
        outputTopic.readKeyValue().let { keyValue ->
            assertEquals("15656", keyValue.key)
            assertEquals(
                keyValue.value,
                objectMapper.readTree(
                    """
                    {
                       "traceId":"ce5c8581f8f011831ae5cb67803f0820",
                       "eventTime":"2022-12-14T22:22:49.406Z",
                       "productId":"15656",
                       "client":{
                          "device":"Mac",
                          "os":{
                             "family":"Mac OS X",
                             "version":"10.15.15"
                          },
                          "agent":{
                             "family":"Chrome",
                             "version":"108.0.0"
                          }
                       },
                       "type":"view_product/1"
                    }
                """.trimIndent()
                )
            )
        }
    }
}
