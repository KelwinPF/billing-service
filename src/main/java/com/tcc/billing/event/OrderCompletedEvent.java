package com.tcc.billing.event;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.time.Instant;

/**
 * Evento publicado no Kafka quando o faturamento é concluído com sucesso
 *
 * Consumido por: Notification Service
 * Tópico: orders.completed
 *
 * Representa o resultado bem-sucedido do processamento de faturamento
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class OrderCompletedEvent {

    private String orderId;
    private String customerId;
    private String billingId;
    private BigDecimal amount;
    private BigDecimal tax;
    private BigDecimal totalAmount;
    
    // Metadados do evento
    private String eventType;       // "OrderCompleted"
    private Instant eventTime;      // Momento da publicação
    private Instant processedAt;    // Momento do processamento
    private Long processingTimeMs;  // Tempo de processamento em ms
}
