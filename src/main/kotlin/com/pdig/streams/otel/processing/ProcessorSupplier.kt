package com.pdig.streams.otel.processing

import com.fasterxml.jackson.databind.JsonNode
import com.pdig.streams.otel.events.EventProcessor
import com.pdig.streams.otel.events.OpenTelemetryEvent
import org.apache.kafka.streams.processor.api.Processor
import org.apache.kafka.streams.processor.api.ProcessorSupplier
import org.springframework.stereotype.Component

@Component
class ProcessorSupplier(
    private val eventProcessor: EventProcessor
) : ProcessorSupplier<String, OpenTelemetryEvent, String, JsonNode> {
    override fun get(): Processor<String, OpenTelemetryEvent, String, JsonNode> = Processor(eventProcessor)
}