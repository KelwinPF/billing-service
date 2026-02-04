# Billing Service

Serviço de faturamento 100% event-driven da arquitetura híbrida de microserviços.

## Descrição

O Billing Service é um microsserviço **100% orientado a eventos**, responsável por:
- ✅ Consumir eventos de pedidos criados do Kafka (topic: `orders.created`)
- ✅ Processar faturamento com cálculo de impostos (10%)
- ✅ Persistir dados de faturamento no PostgreSQL
- ✅ Publicar eventos de resultado no Kafka:
  - `orders.completed` - quando o faturamento é bem-sucedido
  - `orders.failed` - quando o faturamento falha
- ✅ Permitir simulação controlada de **falhas** e **latência** para testes experimentais

## Arquitetura Event-Driven

**Este serviço NÃO expõe endpoints HTTP para regras de negócio.**

```
Orders Service -> Kafka (orders.created) -> Billing Service
                                                  |
                                            PostgreSQL
                                                  |
                                            Kafka (orders.completed/failed) -> Notification Service
```

### Comunicação

- **Entrada**: Apache Kafka - Consumer do tópico `orders.created`
- **Saída**: Apache Kafka - Producer dos tópicos `orders.completed` e `orders.failed`
- **HTTP**: Apenas `/health` e `/actuator/*` (monitoramento)

## Tecnologias

- Spring Boot 4.0.1
- Java 21
- Spring Kafka (Consumer + Producer)
- PostgreSQL
- Spring Data JPA
- Lombok
- Micrometer + Prometheus

## Como executar

### Requisitos
- Java 21
- Maven 3.9+
- Docker (para Kafka e PostgreSQL)

### Executar

```bash
# Build
./mvnw clean package

# Run
./mvnw spring-boot:run

# Run com simulação de latência (1 segundo)
./mvnw spring-boot:run -Dspring-boot.run.arguments="--billing.simulation.latency.enabled=true"

# Run com simulação de falha
./mvnw spring-boot:run -Dspring-boot.run.arguments="--billing.simulation.failure.enabled=true"
```

O serviço estará disponível em: http://localhost:8081

## Endpoints

**Nota**: Este serviço não expõe endpoints de negócio, apenas monitoramento:

- `GET /health` - Health check simples
- `GET /actuator/health` - Health check detalhado
- `GET /actuator/prometheus` - Métricas

## Simulação Experimental

Para permitir análise de resiliência e tolerância a falhas, o serviço oferece:

### 1. Simulação de Latência

Adiciona delay artificial no processamento:

```properties
billing.simulation.latency.enabled=true
billing.simulation.latency.ms=1000  # 1 segundo
```

**Casos de uso**:
- Testar comportamento sob alta latência
- Analisar impacto em throughput
- Verificar timeouts

### 2. Simulação de Falha

Força falha proposital no processamento:

```properties
billing.simulation.failure.enabled=true
```

**Casos de uso**:
- Testar retry policies do Kafka
- Verificar Dead Letter Queues (DLQ)
- Analisar recuperação de falhas

## Variáveis de Ambiente

Ver arquivo `.env.example` para configurações necessárias.

### Configurações Principais

```properties
# Kafka
spring.kafka.bootstrap-servers=localhost:9092

# Database
spring.datasource.url=jdbc:postgresql://localhost:5432/hybrid_db

# Simulação
billing.simulation.failure.enabled=false
billing.simulation.latency.enabled=false
billing.simulation.latency.ms=1000
```

## Fluxo de Processamento

1. **Recebimento**: Consome evento `OrderCreatedEvent` do Kafka
2. **Validação**: Verifica idempotência (evita duplicatas)
3. **Simulação**: Aplica latência artificial (se configurado)
4. **Cálculo**: Calcula imposto (10%) e valor total
5. **Simulação**: Verifica se deve falhar propositalmente
6. **Persistência**: Salva fatura no PostgreSQL
7. **Publicação**: Publica `OrderCompletedEvent` ou `OrderFailedEvent`

## Observabilidade

### Logs Estruturados

Todos os logs incluem:
- Timestamps ISO8601
- `orderId` para rastreamento
- Tempo de processamento em ms
- Delimitadores visuais para facilitar análise

Exemplo:
```
[BILLING] ========================================
[BILLING] Event received: orderId=abc123, amount=100.00, timestamp=2026-01-20T10:30:45Z
[BILLING] Starting billing processing: orderId=abc123
[BILLING] SUCCESS: orderId=abc123, billingId=xyz789, processingTime=1250ms
[BILLING] ========================================
```

### Métricas

- **Prometheus**: `/actuator/prometheus`
- **Health**: `/actuator/health`

## Decisões Arquiteturais

### Event-Driven
✅ Serviço acionado exclusivamente por eventos Kafka  
✅ Sem comunicação síncrona entre serviços  
✅ Maximiza desacoplamento

### Idempotência
✅ Verifica se fatura já existe antes de processar  
✅ Evita duplicatas em caso de retry

### Event-Carried State Transfer
✅ Eventos contêm todos dados necessários  
✅ Elimina necessidade de chamadas síncronas

### Observabilidade
✅ Logs estruturados com timestamps  
✅ Métricas Prometheus  
✅ Tempo de processamento em cada log

### Simulação Experimental
✅ Falhas controláveis via configuração  
✅ Latência ajustável para testes  
✅ Adequado para protótipo de TCC/MBA

## Para Produção

Este é um **protótipo experimental**. Para produção, adicionar:

- [ ] Retry policies configuráveis
- [ ] Dead Letter Queue (DLQ)
- [ ] Compensação de transações
- [ ] Circuit breakers
- [ ] Autenticação e autorização
- [ ] Testes de integração completos
- [ ] Monitoramento avançado (tracing distribuído)

