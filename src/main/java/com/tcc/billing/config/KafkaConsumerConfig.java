package com.tcc.billing.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.tcc.billing.event.OrderCreatedEvent;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.support.serializer.JsonDeserializer;

import java.util.HashMap;
import java.util.Map;

/**
 * Configuração do Kafka Consumer
 *
 * Define como eventos são consumidos e desserializados do Kafka.
 *
 * Decisões de configuração:
 * - Desserialização: JSON (compatível com o produtor)
 * - Offset: earliest (lê desde o início se não houver offset salvo)
 * - Auto-commit: true (simplifica, mas pode perder mensagens em caso de falha)
 *
 * Para produção, adicionar:
 * - Manual offset commit (maior controle)
 * - Error handling e retry policies
 * - Dead Letter Queue para mensagens com erro
 */
@EnableKafka
@Configuration
public class KafkaConsumerConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${spring.kafka.consumer.group-id}")
    private String groupId;

    @Autowired
    private ObjectMapper objectMapper;

    /**
     * Configurações do consumer
     */
    @Bean
    public Map<String, Object> consumerConfigs() {
        Map<String, Object> props = new HashMap<>();

        // Endereço do Kafka broker
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        // Group ID: identifica este consumidor
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);

        // Deserializers: String para chave
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        // Offset: earliest = lê desde o início se não houver offset salvo
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // Auto-commit de offsets
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);

        // Intervalo de auto-commit
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 1000);

        return props;
    }

    /**
     * Factory para criar consumers
     */
    @Bean
    public ConsumerFactory<String, OrderCreatedEvent> consumerFactory() {
        // Cria deserializador JSON com ObjectMapper configurado para JavaTimeModule
        JsonDeserializer<OrderCreatedEvent> deserializer = new JsonDeserializer<>(OrderCreatedEvent.class, objectMapper);
        deserializer.setRemoveTypeHeaders(false);
        deserializer.addTrustedPackages("*");
        deserializer.setUseTypeHeaders(false);

        return new DefaultKafkaConsumerFactory<>(
                consumerConfigs(),
                new StringDeserializer(),
                deserializer
        );
    }

    /**
     * Container factory para listeners Kafka
     *
     * Usado pelo @KafkaListener no BillingService
     */
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, OrderCreatedEvent> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, OrderCreatedEvent> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());

        // Número de threads consumidoras (paralelismo)
        factory.setConcurrency(3);

        return factory;
    }
}
