package com.pdig.streams.otel.config

import org.apache.kafka.streams.KafkaStreams.State
import org.apache.kafka.streams.KafkaStreams.StateListener
import org.springframework.boot.actuate.health.Health
import org.springframework.boot.actuate.health.ReactiveHealthIndicator
import org.springframework.stereotype.Component
import reactor.kotlin.core.publisher.toMono

@Component
class KafkaStreamsHealthIndicator : StateListener, ReactiveHealthIndicator {

    private var healthState: Health = Health.unknown().build()

    override fun onChange(newState: State?, oldState: State?) {
        healthState = when (newState) {
            State.CREATED,
            State.REBALANCING -> Health.unknown().build()

            State.ERROR,
            State.NOT_RUNNING,
            State.PENDING_ERROR,
            State.PENDING_SHUTDOWN -> Health.down().build()

            State.RUNNING -> Health.up().build()
            else -> Health.unknown().build()
        }
    }

    override fun health() = healthState.toMono()
}
