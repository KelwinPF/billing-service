package com.tcc.billing.event;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.time.Instant;

/**
 * Evento publicado no Kafka quando o faturamento falha
 *
 * Consumido por: Notification Service, sistemas de monitoramento
 * Tópico: orders.failed
 *
 * Representa uma falha no processamento de faturamento
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class OrderFailedEvent {

    private String orderId;
    private String customerId;
    private BigDecimal amount;
    
    // Rastreamento end-to-end
    private String correlationId;
    
    // Informações da falha
    private String errorMessage;
    private String errorType;
    
    // Metadados do evento
    private String eventType;       // "OrderFailed"
    private Instant eventTime;      // Momento da publicação
    private Instant failedAt;       // Momento da falha
    private Long processingTimeMs;  // Tempo até a falha em ms
}
