package com.tcc.billing.entity;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.hibernate.annotations.CreationTimestamp;
import org.hibernate.annotations.UpdateTimestamp;

import java.math.BigDecimal;
import java.time.Instant;

/**
 * Entidade JPA para Billing
 *
 * Representa uma fatura persistida no PostgreSQL
 * Schema: billing
 * Table: billings
 *
 * Decisão: UUID como ID para compatibilidade com eventos distribuídos
 * Relacionamento: Uma fatura por pedido (1:1 com Order)
 */
@Entity
@Table(name = "billings", schema = "billing")
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Billing {

    @Id
    @Column(name = "id", nullable = false, length = 36)
    private String id;  // UUID como String para simplicidade

    @Column(name = "order_id", nullable = false, length = 36, unique = true)
    private String orderId;  // Referência ao pedido original

    @Column(name = "customer_id", nullable = false, length = 100)
    private String customerId;

    @Column(name = "product", nullable = false, length = 255)
    private String product;

    @Column(name = "amount", nullable = false, precision = 10, scale = 2)
    private BigDecimal amount;

    @Column(name = "tax", nullable = false, precision = 10, scale = 2)
    private BigDecimal tax;  // Imposto calculado

    @Column(name = "total_amount", nullable = false, precision = 10, scale = 2)
    private BigDecimal totalAmount;  // amount + tax

    @Column(name = "status", nullable = false, length = 50)
    private String status;  // GENERATED, PAID, CANCELLED

    @Column(name = "order_created_at")
    private Instant orderCreatedAt;  // Quando o pedido foi criado

    @CreationTimestamp
    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt;  // Quando a fatura foi gerada

    @UpdateTimestamp
    @Column(name = "updated_at")
    private Instant updatedAt;
}
