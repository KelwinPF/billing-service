package com.tcc.billing.event;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.time.Instant;

/**
 * Evento consumido do Kafka quando um pedido é criado
 *
 * Publicado por: Orders Service
 * Tópico: orders.created
 *
 * Este serviço consome este evento para gerar automaticamente
 * uma fatura para o pedido.
 *
 * Decisão: Event-carried state transfer
 * O evento contém todos os dados necessários para processar
 * sem necessidade de chamar o Orders Service novamente.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class OrderCreatedEvent {

    private String orderId;
    private String customerId;
    private String product;
    private BigDecimal amount;
    private Instant createdAt;

    // Metadados do evento
    private String eventType;    // Tipo: "OrderCreated"
    private Instant eventTime;   // Momento da publicação
}
