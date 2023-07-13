package com.pdig.streams.otel.config.json.matchers

import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.pdig.streams.otel.events.OpenTelemetryEvent
import com.pdig.streams.otel.events.OpenTelemetryMatcher
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import java.io.File

class OpenTelemetryMatcherTest {

    private val event: OpenTelemetryEvent
    init {
        val objectMapper = jacksonObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
        event = objectMapper.readValue(File("src/test/resources/test-event.json"), OpenTelemetryEvent::class.java)
    }

    @Test
    fun `span for start_with operation is found and fields attached`() {
        val matcher = OpenTelemetryMatcher(event)
        val result = matcher.match(
            OpenTelemetryMatcher.OpenTelemetrySpanQuery(
                serviceName = "my_service",
                attributeFetch = listOf("net.host.ip"),
                attributeMatch = listOf(
                    OpenTelemetryMatcher.OpenTelemetrySpanQuery.AttributeQuery(
                        attr = OpenTelemetryMatcher.Attribute("http.route", "/pd"),
                        operation = OpenTelemetryMatcher.OpenTelemetrySpanQuery.AttributeQuery.Operation.STARTS_WITH
                    )
                ),
            )
        )
        assertThat(result).isNotNull
        assertThat(result?.attributes?.size).isEqualTo(1)
        assertThat(result?.attributes).anyMatch { it.key == "net.host.ip" && it.value == "172.20.0.5" }
    }

    @Test
    fun `span for equal operation is found and 2 fields attached`() {
        val matcher = OpenTelemetryMatcher(event)
        val result = matcher.match(
            OpenTelemetryMatcher.OpenTelemetrySpanQuery(
                serviceName = "my_service",
                attributeFetch = listOf("net.host.ip", "http.user_agent"),
                attributeMatch = listOf(
                    OpenTelemetryMatcher.OpenTelemetrySpanQuery.AttributeQuery(
                        attr = OpenTelemetryMatcher.Attribute("http.route", "/pdp/"),
                        operation = OpenTelemetryMatcher.OpenTelemetrySpanQuery.AttributeQuery.Operation.EQUAL
                    )
                ),
            )
        )
        assertThat(result).isNotNull
        assertThat(result?.attributes?.size).isEqualTo(2)
        assertThat(result?.attributes).anyMatch { it.key == "net.host.ip" }
        assertThat(result?.attributes).anyMatch { it.key == "http.user_agent" }
    }

    @Test
    fun `span for ends_with operation is found and 2 fields attached`() {
        val matcher = OpenTelemetryMatcher(event)
        val result = matcher.match(
            OpenTelemetryMatcher.OpenTelemetrySpanQuery(
                serviceName = "my_service",
                attributeFetch = listOf("net.host.ip", "http.user_agent"),
                attributeMatch = listOf(
                    OpenTelemetryMatcher.OpenTelemetrySpanQuery.AttributeQuery(
                        attr = OpenTelemetryMatcher.Attribute("http.route", "dp/"),
                        operation = OpenTelemetryMatcher.OpenTelemetrySpanQuery.AttributeQuery.Operation.ENDS_WITH
                    )
                ),
            )
        )
        assertThat(result).isNotNull
        assertThat(result?.attributes?.size).isEqualTo(2)
    }

    @Test
    fun `span for contains operation is found and 2 fields attached`() {
        val matcher = OpenTelemetryMatcher(event)
        val result = matcher.match(
            OpenTelemetryMatcher.OpenTelemetrySpanQuery(
                serviceName = "my_service",
                attributeFetch = listOf("net.host.ip", "http.user_agent"),
                attributeMatch = listOf(
                    OpenTelemetryMatcher.OpenTelemetrySpanQuery.AttributeQuery(
                        attr = OpenTelemetryMatcher.Attribute("http.route", "dp"),
                        operation = OpenTelemetryMatcher.OpenTelemetrySpanQuery.AttributeQuery.Operation.CONTAINS
                    )
                ),
            )
        )
        assertThat(result).isNotNull
        assertThat(result?.attributes?.size).isEqualTo(2)
    }

    @Test
    fun `span operation returns succeeds if attribute matches and servicename query is null`() {
        val matcher = OpenTelemetryMatcher(event)
        val result = matcher.match(
            OpenTelemetryMatcher.OpenTelemetrySpanQuery(
                serviceName = null,
                attributeFetch = listOf("net.host.ip", "http.user_agent"),
                attributeMatch = listOf(
                    OpenTelemetryMatcher.OpenTelemetrySpanQuery.AttributeQuery(
                        attr = OpenTelemetryMatcher.Attribute("http.route", "dp"),
                        operation = OpenTelemetryMatcher.OpenTelemetrySpanQuery.AttributeQuery.Operation.CONTAINS
                    )
                ),
            )
        )
        assertThat(result).isNotNull
    }

    @Test
    fun `span operation fails if attribute matches but servicename is wrong`() {
        val matcher = OpenTelemetryMatcher(event)
        val result = matcher.match(
            OpenTelemetryMatcher.OpenTelemetrySpanQuery(
                serviceName = "lol",
                attributeFetch = listOf("net.host.ip", "http.user_agent"),
                attributeMatch = listOf(
                    OpenTelemetryMatcher.OpenTelemetrySpanQuery.AttributeQuery(
                        attr = OpenTelemetryMatcher.Attribute("http.route", "dp"),
                        operation = OpenTelemetryMatcher.OpenTelemetrySpanQuery.AttributeQuery.Operation.CONTAINS
                    )
                ),
            )
        )
        assertThat(result).isNull()
    }

    @Test
    fun `span operation returns null if attribute doesnt match`() {
        val matcher = OpenTelemetryMatcher(event)
        val result = matcher.match(
            OpenTelemetryMatcher.OpenTelemetrySpanQuery(
                serviceName = "my_service",
                attributeFetch = listOf("net.host.ip", "http.user_agent"),
                attributeMatch = listOf(
                    OpenTelemetryMatcher.OpenTelemetrySpanQuery.AttributeQuery(
                        attr = OpenTelemetryMatcher.Attribute("http.route", "lol"),
                        operation = OpenTelemetryMatcher.OpenTelemetrySpanQuery.AttributeQuery.Operation.CONTAINS
                    )
                ),
            )
        )
        assertThat(result).isNull()
    }


}