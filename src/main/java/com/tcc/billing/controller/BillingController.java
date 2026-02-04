package com.tcc.billing.controller;

import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

/**
 * REST Controller para Billing Service
 *
 * ARQUITETURA 100% EVENT-DRIVEN:
 * Este serviço NÃO expõe endpoints HTTP para regras de negócio.
 * O processamento de faturamento é acionado exclusivamente via eventos Kafka.
 *
 * Endpoints disponíveis:
 * - GET /health: Health check simples (para monitoramento básico)
 * - Actuator endpoints: /actuator/health, /actuator/prometheus (configurados via application.properties)
 *
 * Justificativa:
 * - Maximiza desacoplamento entre serviços
 * - Facilita análise de resiliência e tolerância a falhas
 * - Adequado para arquitetura orientada a eventos
 */
@Slf4j
@RestController
@RequestMapping
public class BillingController {

    /**
     * Health check simples
     *
     * Endpoint básico para verificar se o serviço está ativo.
     * Para health checks mais detalhados, use /actuator/health
     *
     * @return status OK com mensagem
     */
    @GetMapping("/health")
    public ResponseEntity<String> health() {
        log.debug("[BILLING] Health check requested");
        return ResponseEntity.ok("Billing Service is running - Event-Driven Architecture");
    }
}
