# Billing Service Agent

Este agente auxilia no desenvolvimento e manutenção do Billing Service - **Arquitetura 100% Event-Driven**.

## Contexto do Serviço

O Billing Service é parte de uma arquitetura híbrida de microserviços, responsável por:

- **Consumir eventos**: Escuta o tópico Kafka `orders.created` para receber notificações de novos pedidos
- **Processar faturamento**: Calcula impostos (10% do valor) e cria faturas automaticamente
- **Persistência**: Armazena faturas no PostgreSQL (schema: billing)
- **Publicar resultados**: Publica eventos no Kafka:
  - `orders.completed` - quando faturamento é bem-sucedido
  - `orders.failed` - quando faturamento falha
- **Simulação experimental**: Permite simular falhas e latência para testes de resiliência

## Arquitetura Event-Driven

**IMPORTANTE**: Este serviço NÃO expõe endpoints HTTP para regras de negócio.

```
Kafka (orders.created) -> Billing Service -> Kafka (orders.completed/failed)
                               |
                          PostgreSQL
```

## Tecnologias

- Spring Boot 4.0.1
- Java 21
- Spring Kafka (Consumer + Producer)
- PostgreSQL
- Spring Data JPA
- Lombok

## Estrutura do Projeto

```
src/main/java/com/tcc/billing/
├── BillingApplication.java          # Classe principal
├── config/
│   ├── KafkaConsumerConfig.java     # Configuração Kafka Consumer
│   └── KafkaProducerConfig.java     # Configuração Kafka Producer
├── controller/
│   └── BillingController.java       # Apenas /health (sem negócio)
├── dto/
│   └── BillingResponse.java         # DTO de resposta (não usado em event-driven)
├── entity/
│   └── Billing.java                 # Entidade JPA
├── event/
│   ├── OrderCreatedEvent.java       # Evento Kafka consumido
│   ├── OrderCompletedEvent.java     # Evento Kafka publicado (sucesso)
│   └── OrderFailedEvent.java        # Evento Kafka publicado (falha)
├── repository/
│   └── BillingRepository.java       # Repository JPA
└── service/
    └── BillingService.java          # Lógica de negócio + Kafka Listener
```

## Fluxo de Funcionamento

1. **Orders Service** cria um pedido e publica evento no Kafka (`orders.created`)
2. **BillingService** consome o evento via `@KafkaListener`
3. Simula latência (se configurado)
4. Valida idempotência (evita duplicatas)
5. Simula falha (se configurado)
6. Calcula imposto (10%) e valor total
7. Persiste fatura no PostgreSQL
8. Publica evento de resultado:
   - `orders.completed` (sucesso)
   - `orders.failed` (falha)
9. **Notification Service** consome os eventos de resultado

## Comandos Úteis

```bash
# Build
./mvnw clean package

# Run normal
./mvnw spring-boot:run

# Run com simulação de latência (1 segundo)
./mvnw spring-boot:run -Dspring-boot.run.arguments="--billing.simulation.latency.enabled=true,--billing.simulation.latency.ms=1000"

# Run com simulação de falha
./mvnw spring-boot:run -Dspring-boot.run.arguments="--billing.simulation.failure.enabled=true"

# Run testes
./mvnw test
```

## Endpoints

**Apenas monitoramento** (sem regras de negócio):

- `GET /health` - Health check simples
- `GET /actuator/health` - Health check detalhado
- `GET /actuator/prometheus` - Métricas

## Tópicos Kafka

### Consumer (Entrada)
- **orders.created** - Escuta eventos de pedidos criados
  - Group: `billing-service-group`
  - Offset: `earliest`

### Producer (Saída)
- **orders.completed** - Publica quando faturamento é bem-sucedido
- **orders.failed** - Publica quando faturamento falha

## Configurações Importantes

### Aplicação
- **Porta**: 8081 (diferente do orders-service:8080)
- **Database Schema**: billing
- **Tax Rate**: 10% (configurado no BillingService)

### Simulação Experimental

```properties
# Simulação de falha (força erro proposital)
billing.simulation.failure.enabled=false

# Simulação de latência (adiciona delay)
billing.simulation.latency.enabled=false
billing.simulation.latency.ms=1000
```

**Casos de uso**:
- Testar retry policies do Kafka
- Analisar Dead Letter Queues (DLQ)
- Medir impacto de latência
- Verificar recuperação de falhas

## Decisões Arquiteturais

### 1. Event-Driven 100%
❌ Sem endpoints HTTP de negócio  
✅ Processamento acionado exclusivamente por eventos Kafka  
✅ Maximiza desacoplamento entre serviços

### 2. Idempotência
✅ Verifica se já existe fatura para um pedido antes de criar nova  
✅ Evita duplicatas em caso de retry do Kafka

### 3. Event-Carried State Transfer
✅ Eventos contêm todos os dados necessários  
✅ Evita chamadas síncronas ao Orders Service

### 4. Separate Schema
✅ Usa schema separado no PostgreSQL (billing)  
✅ Isolamento de dados entre serviços

### 5. Observabilidade
✅ Logs estruturados com timestamps ISO8601  
✅ Prefixo `[BILLING]` em todos os logs  
✅ Métricas Prometheus  
✅ Tempo de processamento em ms

### 6. Simulação Experimental
✅ Falhas controláveis via configuração  
✅ Latência ajustável para testes  
✅ Adequado para protótipo de TCC/MBA

## Logs Estruturados

Exemplo de saída de logs:

```
[BILLING] ========================================
[BILLING] Event received: orderId=abc-123, customerId=customer-1, amount=100.00, timestamp=2026-01-20T10:30:45.123Z
[BILLING] Starting billing processing: orderId=abc-123
[BILLING] Calculating billing: billingId=xyz-789, amount=100.00, tax=10.00, total=110.00
[BILLING] Billing persisted in database: billingId=xyz-789
[BILLING] Event published: topic=orders.completed, orderId=abc-123, partition=0, offset=42
[BILLING] SUCCESS: orderId=abc-123, billingId=xyz-789, processingTime=1250ms, timestamp=2026-01-20T10:30:46.373Z
[BILLING] ========================================
```

## Para Desenvolvimento

### Adicionar nova funcionalidade
1. Eventos sempre em `event/`
2. Lógica de negócio em `BillingService`
3. Persistência via `BillingRepository`
4. Nunca adicionar endpoints HTTP de negócio

### Testar localmente
1. Subir Kafka e PostgreSQL (docker-compose)
2. Executar Orders Service
3. Executar Billing Service
4. Criar pedido via Orders Service
5. Verificar logs do Billing Service
6. Verificar eventos no Kafka

### Debug
- Logs em nível DEBUG: `logging.level.com.tcc.billing=DEBUG`
- Kafka Consumer logs: `logging.level.org.apache.kafka=DEBUG`
- JPA SQL logs: `spring.jpa.show-sql=true`
