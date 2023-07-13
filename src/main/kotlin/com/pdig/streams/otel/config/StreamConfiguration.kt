package com.pdig.streams.otel.config

import com.pdig.streams.otel.config.json.serde.JacksonSerde
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.config.SaslConfigs
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.StreamsConfig
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.config.KafkaStreamsConfiguration
import org.springframework.kafka.config.StreamsBuilderFactoryBean
import org.springframework.kafka.config.StreamsBuilderFactoryBeanConfigurer
import org.springframework.kafka.core.KafkaAdmin


@Configuration
class StreamConfiguration(
    val stateListener: KafkaStreamsHealthIndicator, val appConfiguration: AppConfiguration
) {

    @Bean
    fun configurer(): StreamsBuilderFactoryBeanConfigurer {
        return StreamsBuilderFactoryBeanConfigurer { fb: StreamsBuilderFactoryBean ->
            fb.setStateListener(stateListener)
        }
    }

    @Bean
    fun kafkaAdmin(): KafkaAdmin {
        val props: MutableMap<String, Any> = HashMap()
        props[StreamsConfig.BOOTSTRAP_SERVERS_CONFIG] = appConfiguration.kafka.bootstrapServers.joinToString(",")
        props[CommonClientConfigs.SECURITY_PROTOCOL_CONFIG] = "SASL_SSL"
        props[SaslConfigs.SASL_MECHANISM] = "SCRAM-SHA-512"
        props[SaslConfigs.SASL_JAAS_CONFIG] =
            config(appConfiguration.kafka.security.username, appConfiguration.kafka.security.password)

        return KafkaAdmin(props)
    }

    @Bean
    fun defaultKafkaStreamsConfig(): KafkaStreamsConfiguration {
        val props: MutableMap<String, Any> = HashMap()
        props[ConsumerConfig.GROUP_ID_CONFIG] = appConfiguration.kafka.consumerGroupId

        props[StreamsConfig.BOOTSTRAP_SERVERS_CONFIG] = appConfiguration.kafka.bootstrapServers.joinToString(",")
        props[StreamsConfig.APPLICATION_ID_CONFIG] = appConfiguration.kafka.applicationId
        props[StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG] = Serdes.String().javaClass.name
        props[StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG] = JacksonSerde::class.java

        props[CommonClientConfigs.CLIENT_ID_CONFIG] = appConfiguration.kafka.applicationId
        props[CommonClientConfigs.SECURITY_PROTOCOL_CONFIG] = "SASL_SSL"
        props[SaslConfigs.SASL_MECHANISM] = "SCRAM-SHA-512"
        props[SaslConfigs.SASL_JAAS_CONFIG] =
            config(appConfiguration.kafka.security.username, appConfiguration.kafka.security.password)

        props[StreamsConfig.ROCKSDB_CONFIG_SETTER_CLASS_CONFIG] = RocksDbConfig::class.java
        return KafkaStreamsConfiguration(props)
    }

    fun config(username: String?, password: String?) =
        "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"$username\" password=\"$password\";"

}