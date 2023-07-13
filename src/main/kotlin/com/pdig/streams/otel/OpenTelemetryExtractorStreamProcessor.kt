package com.pdig.streams.otel

import com.pdig.streams.otel.config.AppConfiguration
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.boot.runApplication
import org.springframework.kafka.annotation.EnableKafkaStreams

@EnableKafkaStreams
@SpringBootApplication
@EnableConfigurationProperties(AppConfiguration::class)
class OpenTelemetryExtractorStreamProcessor

fun main(args: Array<String>) {
    runApplication<OpenTelemetryExtractorStreamProcessor>(*args)
}
