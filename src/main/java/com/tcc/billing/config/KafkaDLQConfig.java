package com.tcc.billing.config;

import com.tcc.billing.event.OrderCreatedEvent;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.util.backoff.FixedBackOff;

import java.util.HashMap;
import java.util.Map;

/**
 * Configuração de Dead Letter Queue (DLQ) e Retry para Kafka
 * 
 * Implementa mecanismos de tolerância a falhas:
 * - Retry automático com backoff fixo
 * - DLQ para mensagens que falharam após todas as tentativas
 * - Métricas de retry e DLQ para observabilidade
 * 
 * Tópico DLQ: orders.created.dlq
 */
@Slf4j
@Configuration
public class KafkaDLQConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    // Configurações de retry
    @Value("${kafka.retry.max-attempts:3}")
    private int maxAttempts;

    @Value("${kafka.retry.backoff-ms:1000}")
    private long backoffMs;

    // Tópico DLQ
    private static final String DLQ_TOPIC = "orders.created.dlq";

    // Contadores de métricas
    private Counter retryCounter;
    private Counter dlqCounter;

    /**
     * Inicializa métricas customizadas
     */
    @Bean
    public Counter retryCounter(MeterRegistry meterRegistry) {
        this.retryCounter = Counter.builder("kafka.consumer.retry.total")
                .description("Total de retries no consumer Kafka")
                .tag("service", "billing-service")
                .tag("topic", "orders.created")
                .register(meterRegistry);
        return retryCounter;
    }

    @Bean
    public Counter dlqCounter(MeterRegistry meterRegistry) {
        this.dlqCounter = Counter.builder("kafka.consumer.dlq.total")
                .description("Total de mensagens enviadas para DLQ")
                .tag("service", "billing-service")
                .tag("topic", "orders.created")
                .register(meterRegistry);
        return dlqCounter;
    }

    /**
     * ProducerFactory para enviar mensagens para DLQ
     */
    @Bean
    public ProducerFactory<String, Object> dlqProducerFactory() {
        Map<String, Object> configProps = new HashMap<>();
        configProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        configProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        return new DefaultKafkaProducerFactory<>(configProps);
    }

    /**
     * KafkaTemplate para enviar mensagens para DLQ
     */
    @Bean
    public KafkaTemplate<String, Object> dlqKafkaTemplate() {
        return new KafkaTemplate<>(dlqProducerFactory());
    }

    /**
     * DeadLetterPublishingRecoverer - envia mensagens para o tópico DLQ após esgotar retries
     */
    @Bean
    public DeadLetterPublishingRecoverer deadLetterPublishingRecoverer(KafkaTemplate<String, Object> dlqKafkaTemplate) {
        return new DeadLetterPublishingRecoverer(
                (KafkaOperations<?, ?>) dlqKafkaTemplate,
                (record, exception) -> {
                    // Incrementa contador de DLQ
                    if (dlqCounter != null) {
                        dlqCounter.increment();
                    }
                    
                    log.error("");
                    log.error("╔══════════════════════════════════════════════════════════════╗");
                    log.error("║  [BILLING-SERVICE] ☠️ MENSAGEM ENVIADA PARA DLQ            ║");
                    log.error("╠══════════════════════════════════════════════════════════════╣");
                    log.error("║  Topic Original: {}  ", record.topic());
                    log.error("║  DLQ Topic:      {}  ", DLQ_TOPIC);
                    log.error("║  Key:            {}  ", record.key());
                    log.error("║  Partition:      {}  ", record.partition());
                    log.error("║  Offset:         {}  ", record.offset());
                    log.error("║  Erro:           {}  ", exception.getMessage());
                    log.error("╚══════════════════════════════════════════════════════════════╝");
                    log.error("");
                    
                    return new TopicPartition(DLQ_TOPIC, record.partition() % 3);
                }
        );
    }

    /**
     * CommonErrorHandler com retry e DLQ
     * 
     * Comportamento:
     * 1. Tenta processar a mensagem
     * 2. Se falhar, aguarda backoffMs e tenta novamente
     * 3. Repete até maxAttempts vezes
     * 4. Se ainda falhar, envia para DLQ
     */
    @Bean
    public CommonErrorHandler errorHandler(DeadLetterPublishingRecoverer recoverer, MeterRegistry meterRegistry) {
        // FixedBackOff: intervalo fixo entre tentativas
        // maxAttempts - 1 porque o primeiro processamento não conta como retry
        FixedBackOff backOff = new FixedBackOff(backoffMs, maxAttempts - 1);
        
        DefaultErrorHandler errorHandler = new DefaultErrorHandler(recoverer, backOff);
        
        // Configura listener para contar retries
        errorHandler.setRetryListeners((record, ex, deliveryAttempt) -> {
            // Incrementa contador de retry
            if (retryCounter != null) {
                retryCounter.increment();
            }
            
            log.warn("");
            log.warn("╔══════════════════════════════════════════════════════════════╗");
            log.warn("║  [BILLING-SERVICE] 🔄 RETRY #{} de {}                      ║", deliveryAttempt, maxAttempts);
            log.warn("╠══════════════════════════════════════════════════════════════╣");
            log.warn("║  Topic:  {}  ", record.topic());
            log.warn("║  Key:    {}  ", record.key());
            log.warn("║  Erro:   {}  ", ex.getMessage());
            log.warn("║  Próxima tentativa em: {}ms  ", backoffMs);
            log.warn("╚══════════════════════════════════════════════════════════════╝");
            log.warn("");
        });
        
        return errorHandler;
    }
}
