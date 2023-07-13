package com.pdig.streams.otel.events

import java.time.Instant
import java.util.concurrent.TimeUnit

class OpenTelemetryMatcher(private val event: OpenTelemetryEvent) {
    fun match(query: OpenTelemetrySpanQuery): QueryResult? {
        if (matchServiceName(query.serviceName).not()) return null
        return matchAttribute(query.attributeMatch, query.attributeFetch)
    }

    private fun matchAttribute(
        attributeMatch: List<OpenTelemetrySpanQuery.AttributeQuery>,
        attributeFetch: List<String>,
    ): QueryResult? {
        var ingestionTime: Instant
        event.resourceSpans.forEach { resourceSpan ->
            resourceSpan.scopeSpans.forEach { scopeSpan ->
                scopeSpan.spans.forEach { span ->
                    ingestionTime = if (span.parentSpanId.isEmpty()) {
                        Instant.ofEpochMilli(TimeUnit.NANOSECONDS.toMillis(span.startTimeUnixNano.toLong()))
                    } else {
                        Instant.now()
                    }
                    val matches = mutableSetOf<String>()
                    span.attributes.forEach { attr ->
                        attributeMatch.find { it.attr.key == attr.key }?.let { matchedAttr ->
                            if (attr.value.stringValue != null && attributeMatch(
                                    attr.value.stringValue,
                                    matchedAttr.attr.value,
                                    matchedAttr.operation
                                )
                            ) {
                                matches.add(attr.key)
                            }
                        }
                    }
                    if (attributeMatch.all { matches.contains(it.attr.key) }) {
                        return QueryResult(
                            traceId = span.traceId,
                            ingestionTime = ingestionTime,
                            attributes = span.attributes.filter { attributeFetch.contains(it.key) && it.value.stringValue != null }
                                .map { Attribute(it.key, it.value.stringValue!!) }
                        )
                    }
                }
            }
        }
        return null
    }

    private fun attributeMatch(
        acutalValue: String,
        queryValue: String,
        operation: OpenTelemetrySpanQuery.AttributeQuery.Operation
    ) = when (operation) {
        OpenTelemetrySpanQuery.AttributeQuery.Operation.EQUAL -> acutalValue == queryValue
        OpenTelemetrySpanQuery.AttributeQuery.Operation.STARTS_WITH -> acutalValue.startsWith(queryValue)
        OpenTelemetrySpanQuery.AttributeQuery.Operation.ENDS_WITH -> acutalValue.endsWith(queryValue)
        OpenTelemetrySpanQuery.AttributeQuery.Operation.CONTAINS -> acutalValue.contains(queryValue)
    }

    private fun matchServiceName(serviceName: String?): Boolean {
        if (serviceName == null) return true
        return (event.resourceSpans.any { resourceSpan ->
            resourceSpan.resource.attributes.any { attr ->
                attr.key == "service.name" && attr.value.stringValue == serviceName
            }
        })
    }

    data class OpenTelemetrySpanQuery(
        val serviceName: String?, val attributeMatch: List<AttributeQuery>, val attributeFetch: List<String>
    ) {
        data class AttributeQuery(val attr: Attribute, val operation: Operation) {
            enum class Operation { EQUAL, STARTS_WITH, ENDS_WITH, CONTAINS }
        }
    }

    data class QueryResult(
        val traceId: String,
        val ingestionTime: Instant,
        val attributes: List<Attribute>
    )

    data class Attribute(val key: String, val value: String)
}