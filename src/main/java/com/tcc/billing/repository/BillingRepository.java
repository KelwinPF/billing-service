package com.tcc.billing.repository;

import com.tcc.billing.entity.Billing;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.Optional;

/**
 * Repository para Billing
 *
 * Spring Data JPA fornece implementação automática para operações CRUD
 *
 * Métodos customizados:
 * - findByOrderId: busca fatura pelo ID do pedido relacionado
 */
@Repository
public interface BillingRepository extends JpaRepository<Billing, String> {

    /**
     * Busca fatura por ID do pedido
     *
     * @param orderId ID do pedido
     * @return fatura relacionada ao pedido
     */
    Optional<Billing> findByOrderId(String orderId);

    /**
     * Verifica se já existe fatura para um pedido
     *
     * @param orderId ID do pedido
     * @return true se já existe fatura
     */
    boolean existsByOrderId(String orderId);
}
