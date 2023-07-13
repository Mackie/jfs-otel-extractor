package com.pdig.streams.otel.config.json.serde

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.kafka.common.serialization.Deserializer
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.common.serialization.Serializer

class JacksonSerde<T>(objectMapper: ObjectMapper, type: Class<T>) : Serde<T> {

    private val serializer: Serializer<T>

    private val deserializer: Deserializer<T>

    init {
        serializer = JacksonSerdeSerializer(objectMapper)
        deserializer = JacksonSerdeDeserializer(objectMapper, type)
    }

    override fun serializer(): Serializer<T> = serializer

    override fun deserializer(): Deserializer<T> = deserializer
}