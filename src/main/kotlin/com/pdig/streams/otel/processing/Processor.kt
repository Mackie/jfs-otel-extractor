package com.pdig.streams.otel.processing

import com.fasterxml.jackson.databind.JsonNode
import com.pdig.streams.otel.events.EventProcessor
import com.pdig.streams.otel.events.OpenTelemetryEvent
import com.pdig.streams.otel.events.OpenTelemetryMatcher
import com.pdig.streams.otel.events.OtelAttributeConstants
import org.apache.kafka.streams.processor.api.Processor
import org.apache.kafka.streams.processor.api.ProcessorContext
import org.apache.kafka.streams.processor.api.Record

class Processor(private val eventProcessor: EventProcessor) : Processor<String, OpenTelemetryEvent, String, JsonNode> {

    private var partition: Int? = null
    private lateinit var context: ProcessorContext<String, JsonNode>

    override fun init(context: ProcessorContext<String, JsonNode>) {
        this.context = context
        partition = if (context.recordMetadata().isPresent) context.recordMetadata().get().partition() else null
    }

    override fun process(record: Record<String, OpenTelemetryEvent>) {
        if(record.value() != null) {
            val result = OpenTelemetryMatcher(record.value()).match(
                OpenTelemetryMatcher.OpenTelemetrySpanQuery(
                    serviceName = null,
                    attributeMatch = listOf(
                        OpenTelemetryMatcher.OpenTelemetrySpanQuery.AttributeQuery(
                            OpenTelemetryMatcher.Attribute(OtelAttributeConstants.ATTR_HTTP_TARGET, "/"),
                            OpenTelemetryMatcher.OpenTelemetrySpanQuery.AttributeQuery.Operation.STARTS_WITH
                        )
                    ),
                    attributeFetch = listOf(
                        OtelAttributeConstants.ATTR_HTTP_TARGET,
                        OtelAttributeConstants.ATTR_HTTP_USER_AGENT
                    )
                )
            )
            if (result != null) {
                val value = eventProcessor.process(result)
                val productId = value?.get("productId")?.asText()
                if (value != null && productId != null) {
                    context.forward(Record<String, JsonNode>(productId, eventProcessor.process(result), record.timestamp()))
                }
            }
        }
    }

    override fun close() {}
}