package com.pdig.streams.otel.events

data class OpenTelemetryEvent(
    val resourceSpans: List<ResourceSpan>
) {
    data class ResourceSpan(
        val resource: Resource,
        val scopeSpans: List<ScopeSpan>
    ) {
        data class Resource(
            val attributes: List<Attribute>
        )

        data class ScopeSpan(
            val scope: Scope,
            val spans: List<Span>
        ) {
            data class Scope(
                val name: String,
                val version: String
            )

            data class Span(
                val name: String,
                val traceId: String,
                val spanId: String,
                val parentSpanId: String,
                val kind: String,
                val startTimeUnixNano: String,
                val endTimeUnixNano: String,
                val attributes: List<Attribute>,
            )
        }
    }

    data class Attribute(
        val key: String,
        val value: Value
    ) {
        data class Value(
            val stringValue: String?
        )
    }
}