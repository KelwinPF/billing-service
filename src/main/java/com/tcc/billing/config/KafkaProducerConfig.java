package com.tcc.billing.config;

import com.tcc.billing.event.OrderCompletedEvent;
import com.tcc.billing.event.OrderFailedEvent;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.util.HashMap;
import java.util.Map;

/**
 * Configuração do Kafka Producer
 *
 * Define como eventos são serializados e enviados ao Kafka.
 *
 * Tópicos publicados:
 * - orders.completed: quando faturamento é bem-sucedido
 * - orders.failed: quando faturamento falha
 *
 * Decisões de configuração:
 * - Serialização: JSON (legível, debug fácil)
 * - Acks: all (garante replicação antes de confirmar)
 * - Idempotence: true (evita duplicatas em retry)
 */
@Configuration
public class KafkaProducerConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    /**
     * Configurações do producer
     */
    @Bean
    public Map<String, Object> producerConfigs() {
        Map<String, Object> props = new HashMap<>();

        // Endereço do Kafka broker
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        // Serializers: String para chave (orderId), JSON para valor (evento)
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);

        // Garantia de entrega: all = espera confirmação de todos os replicas
        props.put(ProducerConfig.ACKS_CONFIG, "all");

        // Idempotência: evita duplicatas em caso de retry
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);

        // Retries: número de tentativas em caso de falha
        props.put(ProducerConfig.RETRIES_CONFIG, 3);

        // Batching: agrupa mensagens para melhor throughput (em ms)
        props.put(ProducerConfig.LINGER_MS_CONFIG, 10);

        // Tamanho do batch
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);

        return props;
    }

    /**
     * Factory para criar producers de OrderCompletedEvent
     */
    @Bean
    public ProducerFactory<String, OrderCompletedEvent> orderCompletedProducerFactory() {
        return new DefaultKafkaProducerFactory<>(producerConfigs());
    }

    /**
     * Template para enviar eventos OrderCompletedEvent ao Kafka
     */
    @Bean
    public KafkaTemplate<String, OrderCompletedEvent> orderCompletedKafkaTemplate() {
        return new KafkaTemplate<>(orderCompletedProducerFactory());
    }

    /**
     * Factory para criar producers de OrderFailedEvent
     */
    @Bean
    public ProducerFactory<String, OrderFailedEvent> orderFailedProducerFactory() {
        return new DefaultKafkaProducerFactory<>(producerConfigs());
    }

    /**
     * Template para enviar eventos OrderFailedEvent ao Kafka
     */
    @Bean
    public KafkaTemplate<String, OrderFailedEvent> orderFailedKafkaTemplate() {
        return new KafkaTemplate<>(orderFailedProducerFactory());
    }
}
