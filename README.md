# 💳 Billing Service

Serviço de faturamento **100% event-driven** da arquitetura híbrida de microsserviços para TCC de MBA.

## 📋 Descrição

O **Billing Service** é um microsserviço responsável pelo processamento de faturamento e cálculo de impostos. Diferentemente do Order Service que expõe APIs REST, este serviço é **100% orientado a eventos**, sendo acionado exclusivamente através de mensagens do Apache Kafka.

Este design maximiza o desacoplamento entre serviços e demonstra o padrão de arquitetura event-driven em sua forma mais pura.

## 🎯 Responsabilidades

- ✅ Consumir eventos `OrderCreatedEvent` do Kafka (topic: `orders.created`)
- ✅ Validar idempotência (evitar processamento duplicado)
- ✅ Calcular impostos (10% do valor do pedido)
- ✅ Gerar fatura com valor total (valor + imposto)
- ✅ Persistir fatura no PostgreSQL
- ✅ Publicar eventos de resultado no Kafka:
  - `orders.completed` - faturamento bem-sucedido
  - `orders.failed` - falha no processamento
- ✅ Permitir simulação de **falhas** e **latência** para testes experimentais

## 🏗️ Arquitetura

```
┌─────────────────────────────────────────────────────────────────────────────────────────────┐
│                                    BILLING SERVICE                                          │
├─────────────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                             │
│                        ┌────────────────────────────────────────────┐                       │
│                        │            Kafka Consumer                  │                       │
│                        │         (orders.created)                   │                       │
│                        └────────────────────┬───────────────────────┘                       │
│                                             │                                               │
│                                             ▼                                               │
│                        ┌────────────────────────────────────────────┐                       │
│                        │            BillingService                  │                       │
│                        │                                            │                       │
│                        │  ┌──────────────────────────────────────┐  │                       │
│                        │  │ 1. Verificar idempotência            │  │                       │
│                        │  │ 2. Simular latência (opcional)       │  │                       │
│                        │  │ 3. Calcular imposto (10%)            │  │                       │
│                        │  │ 4. Simular falha (opcional)          │  │                       │
│                        │  │ 5. Gerar fatura                      │  │                       │
│                        │  │ 6. Persistir no banco                │  │                       │
│                        │  │ 7. Publicar evento resultado         │  │                       │
│                        │  └──────────────────────────────────────┘  │                       │
│                        └───────────────────┬────────────────────────┘                       │
│                                            │                                                │
│              ┌─────────────────────────────┼─────────────────────────────┐                  │
│              │                             │                             │                  │
│              ▼                             ▼                             ▼                  │
│   ┌───────────────────┐        ┌───────────────────┐        ┌───────────────────┐          │
│   │    PostgreSQL     │        │  Kafka Producer   │        │  Kafka Producer   │          │
│   │  (schema:billing) │        │ orders.completed  │        │  orders.failed    │          │
│   └───────────────────┘        └───────────────────┘        └───────────────────┘          │
│                                                                                             │
└─────────────────────────────────────────────────────────────────────────────────────────────┘
```

> **⚠️ Importante**: Este serviço **NÃO** expõe endpoints HTTP para regras de negócio. Apenas endpoints de monitoramento (`/health`, `/actuator/*`) estão disponíveis.

## 🔄 Fluxo de Processamento

```
1️⃣  Evento OrderCreatedEvent recebido via Kafka
         │
         ▼
2️⃣  Verificação de idempotência (já processou este pedido?)
         │ Não
         ▼
3️⃣  Simulação de latência (se habilitada)
         │
         ▼
4️⃣  Cálculo do faturamento:
         ├── Valor base: amount
         ├── Imposto: amount × 10%
         └── Total: amount + imposto
         │
         ▼
5️⃣  Simulação de falha (se habilitada)
         │
         ├── Sucesso ──────────────────────┐
         │                                 │
         ▼                                 ▼
6️⃣  Persistir fatura no PostgreSQL   Publicar OrderCompletedEvent
         │
         └── Falha ───────────────────────▶ Publicar OrderFailedEvent
```

## 📊 Eventos

### Evento Consumido: OrderCreatedEvent (Topic: `orders.created`)

```json
{
  "orderId": "550e8400-e29b-41d4-a716-446655440000",
  "customerId": "CUST123",
  "product": "Laptop Dell XPS 15",
  "amount": 8999.90,
  "createdAt": "2024-01-15T10:30:00Z",
  "eventType": "OrderCreated",
  "eventTime": "2024-01-15T10:30:00.123Z"
}
```

### Evento Publicado (Sucesso): OrderCompletedEvent (Topic: `orders.completed`)

```json
{
  "orderId": "550e8400-e29b-41d4-a716-446655440000",
  "customerId": "CUST123",
  "billingId": "660e9500-f39c-52e5-b827-557766551111",
  "amount": 8999.90,
  "tax": 899.99,
  "totalAmount": 9899.89,
  "eventType": "OrderCompleted",
  "eventTime": "2024-01-15T10:30:05.456Z",
  "processedAt": "2024-01-15T10:30:05.456Z",
  "processingTimeMs": 150
}
```

### Evento Publicado (Falha): OrderFailedEvent (Topic: `orders.failed`)

```json
{
  "orderId": "550e8400-e29b-41d4-a716-446655440000",
  "customerId": "CUST123",
  "amount": 8999.90,
  "errorMessage": "SIMULATED_FAILURE: Billing processing failed intentionally",
  "errorType": "PROCESSING_ERROR",
  "eventType": "OrderFailed",
  "eventTime": "2024-01-15T10:30:05.456Z",
  "failedAt": "2024-01-15T10:30:05.456Z",
  "processingTimeMs": 150
}
```

## 🚀 Stack Técnica

| Tecnologia | Versão | Descrição |
|------------|--------|-----------|
| Java | 21 | Linguagem de programação |
| Spring Boot | 4.0.1 | Framework principal |
| Spring Kafka | - | Consumer + Producer |
| Spring Data JPA | - | Persistência |
| PostgreSQL | 16 | Banco de dados |
| Lombok | - | Redução de boilerplate |
| Micrometer | - | Métricas |

## 📦 Estrutura do Projeto

```
billing-service/
├── src/main/java/com/tcc/billing/
│   ├── BillingApplication.java         # Classe principal
│   ├── config/
│   │   ├── JacksonConfig.java          # Configuração JSON
│   │   ├── KafkaConsumerConfig.java    # Configuração Consumer
│   │   └── KafkaProducerConfig.java    # Configuração Producer
│   ├── controller/
│   │   └── BillingController.java      # Apenas health check
│   ├── dto/
│   │   └── BillingResponse.java        # DTO de resposta
│   ├── entity/
│   │   └── Billing.java                # Entidade JPA
│   ├── event/
│   │   ├── OrderCreatedEvent.java      # Evento consumido
│   │   ├── OrderCompletedEvent.java    # Evento de sucesso
│   │   └── OrderFailedEvent.java       # Evento de falha
│   ├── repository/
│   │   └── BillingRepository.java      # Interface JPA
│   └── service/
│       └── BillingService.java         # Lógica de negócio + Listeners
└── src/main/resources/
    ├── application.properties          # Configurações
    └── logback-spring.xml              # Configuração de logs
```

## 🌐 Endpoints (Apenas Monitoramento)

| Método | Endpoint | Descrição |
|--------|----------|-----------|
| `GET` | `/health` | Health check simples |
| `GET` | `/actuator/health` | Health check detalhado |
| `GET` | `/actuator/prometheus` | Métricas Prometheus |

## ⚙️ Configuração

### application.properties

```properties
# Servidor
server.port=8081

# Kafka Consumer
spring.kafka.bootstrap-servers=localhost:9092
spring.kafka.consumer.group-id=billing-service-group

# Kafka Producer
spring.kafka.producer.acks=all

# PostgreSQL
spring.datasource.url=jdbc:postgresql://localhost:5432/hybrid_db
spring.jpa.properties.hibernate.default_schema=billing

# Simulação (Experimental)
billing.simulation.failure.enabled=false
billing.simulation.latency.enabled=false
billing.simulation.latency.ms=1000
```

### Variáveis de Ambiente

| Variável | Descrição | Default |
|----------|-----------|---------|
| `SERVER_PORT` | Porta do servidor | `8081` |
| `SPRING_KAFKA_BOOTSTRAP_SERVERS` | Broker Kafka | `localhost:9092` |
| `BILLING_SIMULATION_FAILURE_ENABLED` | Habilita simulação de falha | `false` |
| `BILLING_SIMULATION_LATENCY_ENABLED` | Habilita simulação de latência | `false` |
| `BILLING_SIMULATION_LATENCY_MS` | Latência em ms | `1000` |

## 🐳 Como Executar

### Pré-requisitos
- Java 21
- Maven 3.9+
- Docker e Docker Compose (para Kafka e PostgreSQL)

### Passos

```powershell
# 1. Subir infraestrutura
docker-compose up -d

# 2. Compilar aplicação
./mvnw clean package -DskipTests

# 3. Executar serviço
./mvnw spring-boot:run

# 4. Executar com simulação de latência
./mvnw spring-boot:run -Dspring-boot.run.arguments="--billing.simulation.latency.enabled=true"

# 5. Executar com simulação de falha
./mvnw spring-boot:run -Dspring-boot.run.arguments="--billing.simulation.failure.enabled=true"
```

## 🧪 Simulação Experimental

Para análise de resiliência e tolerância a falhas:

### 1. Simulação de Latência

```properties
billing.simulation.latency.enabled=true
billing.simulation.latency.ms=2000  # 2 segundos
```

**Casos de uso:**
- Testar comportamento sob alta latência
- Analisar impacto em throughput do sistema
- Verificar timeouts em serviços downstream

### 2. Simulação de Falha

```properties
billing.simulation.failure.enabled=true
```

**Casos de uso:**
- Testar políticas de retry do Kafka
- Verificar Dead Letter Queues (DLQ)
- Analisar fluxo de notificação de erros

## 📈 Observabilidade

### Logs Estruturados

```
╔══════════════════════════════════════════════════════════════╗
║  [BILLING-SERVICE] EVENTO KAFKA RECEBIDO                   ║
╠══════════════════════════════════════════════════════════════╣
║  Topic:      orders.created
║  OrderId:    550e8400-e29b-41d4-a716-446655440000
║  Customer:   CUST123
║  Valor:      R$ 8999.90
╚══════════════════════════════════════════════════════════════╝

[BILLING-SERVICE] 💳 ETAPA 1: Processando faturamento...
[BILLING-SERVICE] 🧮 ETAPA 2: Calculando impostos...
[BILLING-SERVICE]    ├── Valor base:  R$ 8999.90
[BILLING-SERVICE]    ├── Imposto 10%: R$ 899.99
[BILLING-SERVICE]    └── Total:       R$ 9899.89
[BILLING-SERVICE] 💾 ETAPA 3: Salvando fatura no banco de dados...
[BILLING-SERVICE] ✅ Fatura salva no PostgreSQL
[BILLING-SERVICE] 📤 ETAPA 4: Publicando evento no Kafka...
```

### Métricas Prometheus

Disponíveis em `/actuator/prometheus`:
- `kafka_consumer_records_consumed_total`
- `kafka_producer_record_send_total`
- `billing_processing_time_ms`

## 📐 Modelo de Dados

### Tabela: `billing.billings`

| Coluna | Tipo | Descrição |
|--------|------|-----------|
| `id` | VARCHAR(36) | UUID da fatura (PK) |
| `order_id` | VARCHAR(36) | UUID do pedido (UK) |
| `customer_id` | VARCHAR(100) | ID do cliente |
| `product` | VARCHAR(255) | Nome do produto |
| `amount` | DECIMAL(10,2) | Valor original |
| `tax` | DECIMAL(10,2) | Imposto calculado |
| `total_amount` | DECIMAL(10,2) | Valor total |
| `status` | VARCHAR(50) | Status (COMPLETED, FAILED) |
| `order_created_at` | TIMESTAMP | Quando o pedido foi criado |
| `created_at` | TIMESTAMP | Data de criação da fatura |
| `updated_at` | TIMESTAMP | Última atualização |

## 🔗 Integração com Outros Serviços

| Serviço | Integração | Direção |
|---------|------------|---------|
| **Order Service** | Kafka (`orders.created`) | Order → Billing |
| **Notification Service** | Kafka (`orders.completed`, `orders.failed`) | Billing → Notification |
| **PostgreSQL** | JDBC | Leitura/Escrita |
| **Prometheus** | HTTP Pull | Métricas |

## 🏛️ Decisões Arquiteturais

| Decisão | Justificativa |
|---------|---------------|
| **100% Event-Driven** | Maximiza desacoplamento; sem dependência HTTP |
| **Idempotência** | Evita duplicatas em caso de retry |
| **Event-Carried State** | Eventos contêm todos os dados necessários |
| **Simulação** | Permite testes de resiliência controlados |

## 📚 Documentação Adicional

- [Guia de Testes](TESTING.md)

---

**Projeto de TCC - MBA** | Arquitetura Híbrida de Microsserviços

