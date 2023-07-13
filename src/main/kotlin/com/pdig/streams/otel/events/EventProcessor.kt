package com.pdig.streams.otel.events

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.pdig.streams.otel.events.OtelAttributeConstants.ATTR_HTTP_USER_AGENT
import org.springframework.stereotype.Service
import ua_parser.Parser

@Service
class EventProcessor(private val jacksonObjectMapper: ObjectMapper) {

    private val parser = Parser()
    private val PATH_KEY = "http.target"

    fun process(queryResult: OpenTelemetryMatcher.QueryResult): JsonNode? {
        queryResult.attributes.forEach { attribute ->
            if (isProductViewEvent(attribute)) {
                val productId = getProductId(attribute.value)
                return if (productId != null) {
                    jacksonObjectMapper.valueToTree(
                        ViewProductEvent(
                            traceId = queryResult.traceId,
                            eventTime = queryResult.ingestionTime,
                            productId = productId,
                            client = clientFrom(queryResult)
                        )
                    )
                } else null
            } else if (isAddToCartEvent(attribute)) {
                val productId = getProductId(attribute.value)
                return if (productId != null) {
                    jacksonObjectMapper.valueToTree(
                        AddToCartEvent(
                            traceId = queryResult.traceId,
                            eventTime = queryResult.ingestionTime,
                            productId = productId,
                            client = clientFrom(queryResult)
                        )
                    )
                } else null
            }
        }
        return null
    }

    private fun isProductViewEvent(attribute: OpenTelemetryMatcher.Attribute): Boolean =
        attribute.key == PATH_KEY && attribute.value.startsWith("/pdp")


    private fun isAddToCartEvent(attribute: OpenTelemetryMatcher.Attribute): Boolean =
        attribute.key == PATH_KEY && attribute.value.startsWith("/cart/add")

    private fun getProductId(path: String): String? {
        return path.split("/").find { Regex("\\d{4,7}").matches(it) }
    }

    private fun clientFrom(queryResult: OpenTelemetryMatcher.QueryResult): Client? {
        val userAgent = queryResult.attributes.find { it.key == ATTR_HTTP_USER_AGENT }?.value
        return if (userAgent != null) {
            val client = parser.parse(userAgent)
            return Client(
                device = client.device.family,
                os = client.os?.let { Client.OS(it.family, "${it.major}.${it.minor}.${it.minor}") },
                agent = client.userAgent?.let { Client.Agent(it.family, "${it.major}.${it.minor}.${it.minor}") }
            )
        } else null
    }

}