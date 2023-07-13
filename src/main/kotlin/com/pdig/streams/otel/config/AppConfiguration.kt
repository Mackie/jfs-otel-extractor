package com.pdig.streams.otel.config

import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.util.unit.DataSize

@ConfigurationProperties(prefix = "app")
data class AppConfiguration(
    val kafka: KafkaConfig,
    val stream: StreamConfig
) {
    data class KafkaConfig(
        val applicationId: String,
        val bootstrapServers: List<String>,
        val consumerGroupId: String,
        val security: Security
    ) {
        data class Security(
            val enabled: Boolean,
            val username: String?,
            val password: String?
        )
    }

    data class StreamConfig(
        val totalOffHeapSize: DataSize,
        val totalMemTable: DataSize,
    )
}