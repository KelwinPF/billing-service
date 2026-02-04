package com.tcc.billing.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.time.Instant;

/**
 * DTO de resposta para consulta de fatura
 *
 * Retorna ao cliente:
 * - billingId: identificador único da fatura
 * - orderId: identificador do pedido relacionado
 * - valores: amount, tax, totalAmount
 * - status: estado da fatura
 * - timestamps: quando o pedido foi criado e quando a fatura foi gerada
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class BillingResponse {

    private String billingId;
    private String orderId;
    private String customerId;
    private String product;
    private BigDecimal amount;
    private BigDecimal tax;
    private BigDecimal totalAmount;
    private String status;
    private Instant orderCreatedAt;
    private Instant createdAt;
}
