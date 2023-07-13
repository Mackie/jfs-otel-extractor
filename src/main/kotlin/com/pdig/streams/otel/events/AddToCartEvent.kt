package com.pdig.streams.otel.events

import java.time.Instant

data class AddToCartEvent(
    val traceId: String?,
    val eventTime: Instant?,
    val productId: String?,
    val client: Client?,
) {
    val type: String = "add_to_cart/1"
}