package com.tcc.billing;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Billing Service - Serviço de Faturamento
 *
 * Responsabilidades:
 * - Consumir eventos "orders.created" do Kafka
 * - Gerar faturas automaticamente para cada pedido
 * - Persistir faturas no PostgreSQL
 * - Expor APIs REST para consulta de faturas
 *
 * Fluxo:
 * Kafka (orders.created) -> Billing Service -> PostgreSQL
 *                              |
 *                         REST API (consulta)
 */
@SpringBootApplication
public class BillingApplication {

        public static void main(String[] args) {
                SpringApplication.run(BillingApplication.class, args);
        }

}
