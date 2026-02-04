# Como Testar o Billing Service

Este guia mostra como testar as funcionalidades do Billing Service.

## Pré-requisitos

- Kafka rodando em `localhost:9092`
- PostgreSQL rodando em `localhost:5432`
- Orders Service rodando em `localhost:8080`
- Billing Service rodando em `localhost:8081`

## 1. Teste Básico - Processamento Normal

### Passo 1: Iniciar o Billing Service

```bash
./mvnw spring-boot:run
```

### Passo 2: Verificar Health Check

```bash
curl http://localhost:8081/health
```

Resposta esperada:
```
Billing Service is running - Event-Driven Architecture
```

### Passo 3: Criar um pedido no Orders Service

```bash
curl -X POST http://localhost:8080/orders \
  -H "Content-Type: application/json" \
  -d '{
    "customerId": "customer-123",
    "product": "Notebook Dell",
    "amount": 5000.00
  }'
```

### Passo 4: Verificar logs do Billing Service

Você deverá ver logs similares a:

```
[BILLING] ========================================
[BILLING] Event received: orderId=abc-123, customerId=customer-123, amount=5000.00
[BILLING] Starting billing processing: orderId=abc-123
[BILLING] Calculating billing: amount=5000.00, tax=500.00, total=5500.00
[BILLING] Billing persisted in database: billingId=xyz-789
[BILLING] Event published: topic=orders.completed, orderId=abc-123
[BILLING] SUCCESS: orderId=abc-123, processingTime=250ms
[BILLING] ========================================
```

## 2. Teste com Simulação de Latência

### Passo 1: Parar o Billing Service (Ctrl+C)

### Passo 2: Reiniciar com latência de 2 segundos

```bash
./mvnw spring-boot:run -Dspring-boot.run.arguments="--billing.simulation.latency.enabled=true,--billing.simulation.latency.ms=2000"
```

Ou configurar no `application.properties`:
```properties
billing.simulation.latency.enabled=true
billing.simulation.latency.ms=2000
```

### Passo 3: Criar um pedido

```bash
curl -X POST http://localhost:8080/orders \
  -H "Content-Type: application/json" \
  -d '{
    "customerId": "customer-456",
    "product": "Mouse Logitech",
    "amount": 150.00
  }'
```

### Passo 4: Verificar logs

Você deverá ver:

```
[BILLING] Event received: orderId=def-456
[BILLING] Simulating latency: 2000ms
[BILLING] Latency simulation completed
[BILLING] Starting billing processing: orderId=def-456
[BILLING] SUCCESS: orderId=def-456, processingTime=2250ms  <-- Note o tempo maior
```

## 3. Teste com Simulação de Falha

### Passo 1: Parar o Billing Service

### Passo 2: Reiniciar com simulação de falha

```bash
./mvnw spring-boot:run -Dspring-boot.run.arguments="--billing.simulation.failure.enabled=true"
```

Ou configurar no `application.properties`:
```properties
billing.simulation.failure.enabled=true
```

### Passo 3: Criar um pedido

```bash
curl -X POST http://localhost:8080/orders \
  -H "Content-Type: application/json" \
  -d '{
    "customerId": "customer-789",
    "product": "Teclado Mecânico",
    "amount": 300.00
  }'
```

### Passo 4: Verificar logs de falha

Você deverá ver:

```
[BILLING] Event received: orderId=ghi-789
[BILLING] Starting billing processing: orderId=ghi-789
[BILLING] Simulating failure for orderId=ghi-789
[BILLING] ========================================
[BILLING] FAILURE: orderId=ghi-789, error=SIMULATED_FAILURE: Billing processing failed intentionally
[BILLING] ========================================
[BILLING] Event published: topic=orders.failed, orderId=ghi-789
```

## 4. Verificar Eventos no Kafka

### Consumir eventos de sucesso

```bash
kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic orders.completed \
  --from-beginning
```

### Consumir eventos de falha

```bash
kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic orders.failed \
  --from-beginning
```

## 5. Verificar Dados no PostgreSQL

```sql
-- Conectar ao banco
psql -h localhost -U hybrid_user -d hybrid_db

-- Ver faturas criadas
SELECT * FROM billing.billings ORDER BY created_at DESC;

-- Ver fatura específica por order_id
SELECT * FROM billing.billings WHERE order_id = 'abc-123';

-- Ver total de faturas por status
SELECT status, COUNT(*) as total FROM billing.billings GROUP BY status;
```

## 6. Métricas Prometheus

Acesse as métricas:

```bash
curl http://localhost:8081/actuator/prometheus
```

Procure por métricas relacionadas a:
- `kafka_consumer_fetch_manager_records_consumed_total`
- `kafka_producer_record_send_total`
- `jvm_memory_used_bytes`

## 7. Health Check Detalhado

```bash
curl http://localhost:8081/actuator/health | jq .
```

Resposta esperada:
```json
{
  "status": "UP",
  "components": {
    "db": {
      "status": "UP",
      "details": {
        "database": "PostgreSQL",
        "validationQuery": "isValid()"
      }
    },
    "diskSpace": {
      "status": "UP"
    },
    "ping": {
      "status": "UP"
    }
  }
}
```

## 8. Teste de Idempotência

### Passo 1: Criar um pedido

```bash
curl -X POST http://localhost:8080/orders \
  -H "Content-Type: application/json" \
  -d '{
    "customerId": "customer-999",
    "product": "Monitor LG",
    "amount": 1200.00
  }'
```

Anote o `orderId` retornado (ex: `jkl-999`).

### Passo 2: Republicar o mesmo evento manualmente no Kafka

Use um producer Kafka para enviar o mesmo evento novamente.

### Passo 3: Verificar logs

Você deverá ver:

```
[BILLING] Event received: orderId=jkl-999
[BILLING] Billing already exists for orderId=jkl-999, skipping processing
```

### Passo 4: Verificar no banco

```sql
SELECT COUNT(*) FROM billing.billings WHERE order_id = 'jkl-999';
-- Deve retornar 1 (não duplicou)
```

## 9. Teste de Carga (Opcional)

Criar múltiplos pedidos em sequência:

```bash
for i in {1..10}; do
  curl -X POST http://localhost:8080/orders \
    -H "Content-Type: application/json" \
    -d "{
      \"customerId\": \"customer-$i\",
      \"product\": \"Product $i\",
      \"amount\": $((i * 100)).00
    }"
  echo "Pedido $i criado"
done
```

Verificar que todos foram processados:

```sql
SELECT COUNT(*) FROM billing.billings;
-- Deve retornar 10
```

## Troubleshooting

### Billing Service não consome eventos

1. Verificar se Kafka está rodando: `docker ps`
2. Verificar se o tópico existe: `kafka-topics.sh --list --bootstrap-server localhost:9092`
3. Verificar logs de conexão Kafka no Billing Service
4. Verificar `spring.kafka.bootstrap-servers` no `application.properties`

### Erros de conexão com banco de dados

1. Verificar se PostgreSQL está rodando
2. Verificar credenciais em `application.properties`
3. Criar schema manualmente se necessário:
   ```sql
   CREATE SCHEMA IF NOT EXISTS billing;
   ```

### Eventos não aparecem no Kafka

1. Verificar se Orders Service publicou o evento
2. Verificar logs do Orders Service
3. Listar tópicos: `kafka-topics.sh --list --bootstrap-server localhost:9092`
4. Ver mensagens: `kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic orders.created --from-beginning`

## Referências

- [README.md](../README.md) - Documentação principal
- [billing.agent.md](.github/agents/billing.agent.md) - Guia do desenvolvedor
- [application.properties](src/main/resources/application.properties) - Configurações
