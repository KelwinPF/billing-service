package com.tcc.billing.service;

import com.tcc.billing.config.BillingMetrics;
import com.tcc.billing.entity.Billing;
import com.tcc.billing.event.OrderCompletedEvent;
import com.tcc.billing.event.OrderCreatedEvent;
import com.tcc.billing.event.OrderFailedEvent;
import com.tcc.billing.repository.BillingRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

/**
 * Serviço de negócio para Billing - 100% Event-Driven
 *
 * Responsabilidades:
 * 1. Consumir eventos OrderCreatedEvent do Kafka (orders.created)
 * 2. Simular processamento de faturamento com latência configurável
 * 3. Calcular imposto (10% do valor do pedido)
 * 4. Persistir fatura no banco de dados
 * 5. Publicar eventos de resultado:
 *    - orders.completed (sucesso)
 *    - orders.failed (falha)
 * 6. Permitir simulação controlada de falhas
 *
 * Arquitetura:
 * - Event-driven: sem endpoints HTTP de negócio
 * - Desacoplamento: comunicação apenas via Kafka
 * - Observabilidade: logs estruturados com timestamps e métricas
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class BillingService {

    private final BillingRepository billingRepository;
    private final KafkaTemplate<String, OrderCompletedEvent> orderCompletedKafkaTemplate;
    private final KafkaTemplate<String, OrderFailedEvent> orderFailedKafkaTemplate;
    private final BillingMetrics billingMetrics;

    // Tópicos Kafka
    private static final String TOPIC_ORDERS_COMPLETED = "orders.completed";
    private static final String TOPIC_ORDERS_FAILED = "orders.failed";

    // Taxa de imposto: 10%
    private static final BigDecimal TAX_RATE = new BigDecimal("0.10");

    // Configurações de simulação (via application.properties)
    @Value("${billing.simulation.failure.enabled:false}")
    private boolean simulateFailure;

    @Value("${billing.simulation.latency.enabled:false}")
    private boolean simulateLatency;

    @Value("${billing.simulation.latency.ms:1000}")
    private long latencyMs;

    /**
     * Listener Kafka: consome eventos de pedidos criados
     *
     * Topic: orders.created
     * Group: billing-service-group
     *
     * Fluxo:
     * 1. Recebe evento de pedido criado
     * 2. Simula latência (se configurado)
     * 3. Valida idempotência
     * 4. Processa faturamento
     * 5. Simula falha (se configurado)
     * 6. Persiste fatura
     * 7. Publica evento de sucesso ou falha
     *
     * @param event evento de pedido criado
     */
    @KafkaListener(topics = "orders.created", groupId = "billing-service-group")
    @Transactional
    public void handleOrderCreated(OrderCreatedEvent event) {
        // ========================================
        // PONTO DE MEDIÇÃO 1: Início do processamento
        // ========================================
        long startTime = System.currentTimeMillis();
        Instant receivedAt = Instant.now();
        
        // Registra início nas métricas
        billingMetrics.recordProcessingStart();

        log.info("");
        log.info("╔══════════════════════════════════════════════════════════════╗");
        log.info("║  [BILLING-SERVICE] EVENTO KAFKA RECEBIDO                   ║");
        log.info("╠══════════════════════════════════════════════════════════════╣");
        log.info("║  Topic:      orders.created  ");
        log.info("║  OrderId:    {}  ", event.getOrderId());
        log.info("║  Customer:   {}  ", event.getCustomerId());
        log.info("║  Valor:      R$ {}  ", event.getAmount());
        log.info("╚══════════════════════════════════════════════════════════════╝");

        BigDecimal tax = BigDecimal.ZERO;
        try {
            // ========================================
            // SIMULAÇÃO DE LATÊNCIA
            // ========================================
            if (simulateLatency) {
                log.info("[BILLING-SERVICE] ⏳ Simulando latência de {}ms...", latencyMs);
                Thread.sleep(latencyMs);
            }

            // ========================================
            // VALIDAÇÃO DE IDEMPOTÊNCIA
            // ========================================
            if (billingRepository.existsByOrderId(event.getOrderId())) {
                log.warn("[BILLING-SERVICE] ⚠️ Fatura já existe para orderId={}, ignorando evento duplicado", 
                        event.getOrderId());
                return;
            }

            // ========================================
            // INÍCIO DO PROCESSAMENTO
            // ========================================
            log.info("[BILLING-SERVICE] 💳 ETAPA 1: Processando faturamento...");

            // ========================================
            // SIMULAÇÃO DE FALHA
            // ========================================
            if (simulateFailure) {
                log.error("[BILLING-SERVICE] 💥 Simulando falha para orderId={}", event.getOrderId());
                throw new RuntimeException("SIMULATED_FAILURE: Billing processing failed intentionally");
            }

            // ========================================
            // CÁLCULO DO FATURAMENTO
            // ========================================
            String billingId = UUID.randomUUID().toString();
            BigDecimal amount = event.getAmount();
            tax = amount.multiply(TAX_RATE).setScale(2, RoundingMode.HALF_UP);
            BigDecimal totalAmount = amount.add(tax);

            log.info("[BILLING-SERVICE] 🧮 ETAPA 2: Calculando impostos...");
            log.info("[BILLING-SERVICE]    ├── Valor base:  R$ {}", amount);
            log.info("[BILLING-SERVICE]    ├── Imposto 10%: R$ {}", tax);
            log.info("[BILLING-SERVICE]    └── Total:       R$ {}", totalAmount);

            // ========================================
            // PERSISTÊNCIA NO BANCO
            // ========================================
            log.info("[BILLING-SERVICE] 💾 ETAPA 3: Salvando fatura no banco de dados...");
            Billing billing = Billing.builder()
                    .id(billingId)
                    .orderId(event.getOrderId())
                    .customerId(event.getCustomerId())
                    .product(event.getProduct())
                    .amount(amount)
                    .tax(tax)
                    .totalAmount(totalAmount)
                    .status("COMPLETED")
                    .orderCreatedAt(event.getCreatedAt())
                    .build();

            billingRepository.save(billing);
            log.info("[BILLING-SERVICE] ✅ Fatura salva no PostgreSQL: billingId={}", billingId);

            // ========================================
            // PONTO DE MEDIÇÃO 2: Processamento concluído
            // ========================================
            long endTime = System.currentTimeMillis();
            long processingTime = endTime - startTime;
            Instant processedAt = Instant.now();

            // ========================================
            // PUBLICAÇÃO DO EVENTO DE SUCESSO
            // ========================================
            log.info("[BILLING-SERVICE] 📤 ETAPA 4: Publicando evento no Kafka (topic: orders.completed)...");
            OrderCompletedEvent completedEvent = OrderCompletedEvent.builder()
                    .orderId(event.getOrderId())
                    .customerId(event.getCustomerId())
                    .billingId(billingId)
                    .amount(amount)
                    .tax(tax)
                    .totalAmount(totalAmount)
                    .correlationId(event.getCorrelationId())
                    .eventType("OrderCompleted")
                    .eventTime(Instant.now())
                    .processedAt(processedAt)
                    .processingTimeMs(processingTime)
                    .build();

            publishCompletedEvent(completedEvent);
            
            // Registra sucesso nas métricas
            billingMetrics.recordProcessingSuccess(processingTime, tax.doubleValue());

            log.info("");
            log.info("╔══════════════════════════════════════════════════════════════╗");
            log.info("║  [BILLING-SERVICE] ✅ PROCESSAMENTO CONCLUÍDO              ║");
            log.info("╠══════════════════════════════════════════════════════════════╣");
            log.info("║  OrderId:   {}  ", event.getOrderId());
            log.info("║  BillingId: {}  ", billingId);
            log.info("║  Tempo:     {}ms  ", processingTime);
            log.info("╚══════════════════════════════════════════════════════════════╝");
            log.info("");

        } catch (Exception e) {
            // ========================================
            // TRATAMENTO DE FALHA
            // ========================================
            long endTime = System.currentTimeMillis();
            long processingTime = endTime - startTime;
            Instant failedAt = Instant.now();
            
            // Registra falha nas métricas
            billingMetrics.recordProcessingFailure(processingTime);

            log.error("");
            log.error("╔══════════════════════════════════════════════════════════════╗");
            log.error("║  [BILLING-SERVICE] ❌ FALHA NO PROCESSAMENTO              ║");
            log.error("╠══════════════════════════════════════════════════════════════╣");
            log.error("║  OrderId: {}  ", event.getOrderId());
            log.error("║  Erro:    {}  ", e.getMessage());
            log.error("║  Tempo:   {}ms  ", processingTime);
            log.error("╚══════════════════════════════════════════════════════════════╝");
            log.error("");

            // ========================================
            // PUBLICAÇÃO DO EVENTO DE FALHA
            // ========================================
            OrderFailedEvent failedEvent = OrderFailedEvent.builder()
                    .orderId(event.getOrderId())
                    .customerId(event.getCustomerId())
                    .amount(event.getAmount())
                    .correlationId(event.getCorrelationId())
                    .errorMessage(e.getMessage())
                    .errorType(e.getClass().getSimpleName())
                    .eventType("OrderFailed")
                    .eventTime(Instant.now())
                    .failedAt(failedAt)
                    .processingTimeMs(processingTime)
                    .build();

            publishFailedEvent(failedEvent);

            // Re-lança exceção para acionar retry do Kafka (se configurado)
            throw new RuntimeException("Billing processing failed for orderId: " + event.getOrderId(), e);
        }
    }

    /**
     * Publica evento de sucesso no Kafka
     *
     * @param event evento de pedido concluído
     */
    private void publishCompletedEvent(OrderCompletedEvent event) {
        CompletableFuture<SendResult<String, OrderCompletedEvent>> future =
                orderCompletedKafkaTemplate.send(TOPIC_ORDERS_COMPLETED, event.getOrderId(), event);

        future.whenComplete((result, ex) -> {
            if (ex == null) {
                log.info("[BILLING] Event published: topic={}, orderId={}, partition={}, offset={}",
                        TOPIC_ORDERS_COMPLETED,
                        event.getOrderId(),
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset());
            } else {
                log.error("[BILLING] Failed to publish completed event: orderId={}, error={}",
                        event.getOrderId(), ex.getMessage(), ex);
            }
        });
    }

    /**
     * Publica evento de falha no Kafka
     *
     * @param event evento de pedido falhado
     */
    private void publishFailedEvent(OrderFailedEvent event) {
        CompletableFuture<SendResult<String, OrderFailedEvent>> future =
                orderFailedKafkaTemplate.send(TOPIC_ORDERS_FAILED, event.getOrderId(), event);

        future.whenComplete((result, ex) -> {
            if (ex == null) {
                log.info("[BILLING] Event published: topic={}, orderId={}, partition={}, offset={}",
                        TOPIC_ORDERS_FAILED,
                        event.getOrderId(),
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset());
            } else {
                log.error("[BILLING] Failed to publish failed event: orderId={}, error={}",
                        event.getOrderId(), ex.getMessage(), ex);
            }
        });
    }
}
