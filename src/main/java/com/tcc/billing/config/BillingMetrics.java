package com.tcc.billing.config;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Métricas customizadas para o Billing Service
 * 
 * Métricas coletadas para o experimento do TCC:
 * - billing_processing_total: Total de faturas processadas
 * - billing_processing_success_total: Total de sucessos
 * - billing_processing_failure_total: Total de falhas
 * - billing_processing_time_seconds: Tempo de processamento
 * - billing_tax_calculated_total: Total de impostos calculados (em R$)
 * - billing_active_processing: Processamentos ativos no momento
 */
@Slf4j
@Component
@Getter
public class BillingMetrics {

    private final Counter processingTotal;
    private final Counter processingSuccess;
    private final Counter processingFailure;
    private final Timer processingTime;
    private final Counter taxCalculatedTotal;
    private final AtomicLong activeProcessing;

    public BillingMetrics(MeterRegistry meterRegistry) {
        // Contador total de processamentos
        this.processingTotal = Counter.builder("billing.processing.total")
                .description("Total de faturas processadas")
                .tag("service", "billing-service")
                .register(meterRegistry);

        // Contador de sucessos
        this.processingSuccess = Counter.builder("billing.processing.success.total")
                .description("Total de faturas processadas com sucesso")
                .tag("service", "billing-service")
                .register(meterRegistry);

        // Contador de falhas
        this.processingFailure = Counter.builder("billing.processing.failure.total")
                .description("Total de faturas que falharam no processamento")
                .tag("service", "billing-service")
                .register(meterRegistry);

        // Timer para medir tempo de processamento
        this.processingTime = Timer.builder("billing.processing.time")
                .description("Tempo de processamento de faturas")
                .tag("service", "billing-service")
                .publishPercentiles(0.5, 0.95, 0.99) // p50, p95, p99
                .publishPercentileHistogram()
                .register(meterRegistry);

        // Contador de impostos calculados (em centavos para evitar problemas com decimais)
        this.taxCalculatedTotal = Counter.builder("billing.tax.calculated.total")
                .description("Total de impostos calculados (em centavos)")
                .tag("service", "billing-service")
                .register(meterRegistry);

        // Gauge para processamentos ativos
        this.activeProcessing = new AtomicLong(0);
        Gauge.builder("billing.processing.active", activeProcessing, AtomicLong::get)
                .description("Número de processamentos ativos no momento")
                .tag("service", "billing-service")
                .register(meterRegistry);

        log.info("[BILLING-METRICS] Métricas customizadas registradas com sucesso");
    }

    /**
     * Registra início de processamento
     */
    public void recordProcessingStart() {
        processingTotal.increment();
        activeProcessing.incrementAndGet();
    }

    /**
     * Registra fim de processamento com sucesso
     */
    public void recordProcessingSuccess(long processingTimeMs, double taxAmount) {
        processingSuccess.increment();
        activeProcessing.decrementAndGet();
        processingTime.record(java.time.Duration.ofMillis(processingTimeMs));
        
        // Registra imposto em centavos
        taxCalculatedTotal.increment(taxAmount * 100);
    }

    /**
     * Registra fim de processamento com falha
     */
    public void recordProcessingFailure(long processingTimeMs) {
        processingFailure.increment();
        activeProcessing.decrementAndGet();
        processingTime.record(java.time.Duration.ofMillis(processingTimeMs));
    }

    /**
     * Timer.Sample para medir tempo automaticamente
     */
    public Timer.Sample startTimer() {
        return Timer.start();
    }

    public void stopTimer(Timer.Sample sample) {
        sample.stop(processingTime);
    }
}
