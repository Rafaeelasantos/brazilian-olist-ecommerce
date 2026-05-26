# Olist E-Commerce — Especificação do Pipeline Lakeflow (PySpark)

**Versão:** 2.0 — Maio 2025

| Campo | Valor |
|---|---|
| **Nome do Projeto** | Olist E-Commerce Analytics |
| **Catalog** | `workspace` |
| **Schema (Bronze / Silver / Gold)** | `bronze` / `silver` / `gold` |
| **Caminho Base do Volume** | `/Volumes/workspace/default/ecommerce_raw_volume` |
| **Nome do Pipeline** | `olist_ecommerce_dev_lakeflow` |
| **Ambiente** | dev |

> **Templates de código:** Todos os padrões de código PySpark / DLT estão definidos em `templates/pipeline_templates.mdc`. Siga esses templates rigorosamente ao gerar ou revisar arquivos de pipeline.

---

## 1. Arquitetura de Dados — Medallion

Todas as tabelas utilizam Unity Catalog no formato `catalog.schema.table`.

| Camada | Nome | Finalidade |
|---|---|---|
| Bronze | Camada Bruta | Ingestão imutável de arquivos CSV via Auto Loader (`cloudFiles`). Campos de metadados adicionados aqui. |
| Silver | Camada Curada | Dados limpos e padronizados. SCD Type 2 aplicado em Customers via `dlt.apply_changes()`. Campos derivados calculados aqui. |
| Gold | Camada de Negócio | Star schema: **1 tabela fato** (`fct_order_items`, grain = item de pedido) no centro + **2 dimensões** (`dim_customers`, `dim_products`). `fct_order_items` consolida atributos de pedido e item em uma única tabela, eliminando a necessidade de duas facts. |

---

## 2. Fontes de Dados — Ingestão Bronze

> **Crítico — sintaxe Python do Auto Loader:** Em pipelines Python/PySpark, o Auto Loader é invocado via `spark.readStream.format("cloudFiles")` com as opções `cloudFiles.format`, `cloudFiles.schemaLocation`, etc. O `schemaLocation` **deve ser declarado explicitamente** no código Python — diferente do SQL, onde é gerenciado automaticamente pelo runtime.

### Caminhos das Fontes

> **Crítico — estrutura de diretórios do Volume:** No Unity Catalog Volume deste workspace, cada dataset CSV é armazenado **dentro de um diretório**, não como um arquivo avulso. O nome do diretório inclui a extensão `.csv` (ex.: `olist_products_dataset.csv/` é um diretório, não um arquivo). O Auto Loader deve apontar para o **diretório** (com barra final `/`), não para um caminho de arquivo. Passar um caminho de arquivo inexistente causa `FileNotFoundException`.
>
> Estrutura real no volume:
> ```
> /Volumes/workspace/default/ecommerce_raw_volume/
> ├── olist_orders_dataset.csv/
> │   └── olist_orders_dataset.csv          ← arquivo dentro do diretório
> ├── olist_order_items_dataset.csv/
> │   └── olist_order_items_dataset.csv
> ├── olist_customers_dataset.csv/
> │   └── olist_customers_dataset.csv
> ├── olist_products_dataset.csv/
> │   └── olist_products_dataset.csv
> └── product_category/
>     └── product_category_name_translation.csv
> ```

| Entidade | Diretório (alvo do Auto Loader) | Caminho da Fonte |
|---|---|---|
| `orders` | `olist_orders_dataset.csv/` | `/Volumes/workspace/default/ecommerce_raw_volume/olist_orders_dataset.csv/` |
| `order_items` | `olist_order_items_dataset.csv/` | `/Volumes/workspace/default/ecommerce_raw_volume/olist_order_items_dataset.csv/` |
| `customers` | `olist_customers_dataset.csv/` | `/Volumes/workspace/default/ecommerce_raw_volume/olist_customers_dataset.csv/` |
| `products` | `olist_products_dataset.csv/` | `/Volumes/workspace/default/ecommerce_raw_volume/olist_products_dataset.csv/` |
| `product_category` | `product_category/` | `/Volumes/workspace/default/ecommerce_raw_volume/product_category/` |

### Opções `cloudFiles` — CSV (Python)

| Opção | Valor | Observação |
|---|---|---|
| `cloudFiles.format` | `"csv"` | Obrigatório |
| `header` | `"true"` | Primeira linha como nomes de colunas |
| `delimiter` | `","` | Separador de campos |
| `inferSchema` | `"false"` | Usar schema declarado para estabilidade em produção |
| `cloudFiles.schemaLocation` | `"/Volumes/workspace/default/ecommerce_raw_volume/_schemas/<entidade>"` | Obrigatório em Python; gerenciado por entidade. **Nunca use `/tmp/` — DBFS root está desabilitado neste workspace. Sempre usar Unity Catalog Volume.** |
| `cloudFiles.schemaEvolutionMode` | `"none"` | Recomendado para produção |

> `_metadata.file_path` é uma coluna de metadados exposta pelo Auto Loader. Ela deve ser referenciada via `col("_metadata.file_path")` antes de qualquer transformação que remova colunas de metadados.

> **Template:** Use o **Template 1** de `templates/pipeline_templates.mdc` para todas as tabelas Bronze.

> **Crítico — parâmetro `schema=` em `@dlt.table()` / `dlt.create_streaming_table()`:** O parâmetro `schema` nessas funções é reservado para **definição de colunas DDL** (ex.: `"order_id STRING, price DOUBLE"`). Ele **não** define onde a tabela é publicada. Passar `schema="workspace.bronze"` (ou qualquer valor `catalog.schema`) faz o runtime tentar interpretar a string como DDL de colunas, causando um erro de sintaxe SQL. A localização de publicação (catalog + schema) é controlada pelos campos `catalog` e `target` no `databricks.yml`. **Nunca passe `schema=` com valores `catalog.schema` nos decoradores DLT.**

---

## 3. Entidades

### 3.1 Orders (Pedidos)

| Atributo | Valor |
|---|---|
| **Tipo** | Fonte de Fato |
| **Domínio** | `orders` |
| **Nível de PII** | Baixo |

#### Campos

| Campo | Tipo | Descrição | PII / Tags |
|---|---|---|---|
| `order_id` | StringType | Identificador único do pedido (PK) | — |
| `customer_id` | StringType | Chave para o dataset de clientes | — |
| `order_status` | StringType | Status do pedido (`delivered`, `shipped`, etc.) | — |
| `order_purchase_timestamp` | TimestampType | Timestamp da compra | — |
| `order_approved_at` | TimestampType | Timestamp de aprovação do pagamento | — |
| `order_delivered_carrier_date` | TimestampType | Data de postagem — quando entregue ao parceiro logístico | — |
| `order_delivered_customer_date` | TimestampType | Data real de entrega ao cliente | — |
| `order_estimated_delivery_date` | TimestampType | Data estimada de entrega exibida ao cliente no momento da compra | — |

#### Campos Derivados — Silver

| Campo | Lógica |
|---|---|
| `delivery_delay_days` | `datediff(col("order_delivered_customer_date"), col("order_estimated_delivery_date"))` — positivo = atrasado |
| `order_processing_days` | `datediff(col("order_approved_at"), col("order_purchase_timestamp"))` |
| `is_late_delivery` | `when(col("delivery_delay_days") > 0, True).otherwise(False)` |

#### Restrições de Qualidade de Dados

| Restrição | Expressão |
|---|---|
| `valid_order_id` | `order_id IS NOT NULL` |
| `valid_customer_id` | `customer_id IS NOT NULL` |
| `valid_status` | `order_status IS NOT NULL` |

> **Template:** Use o **Template 2** de `templates/pipeline_templates.mdc` para `silver_orders`.

---

### 3.2 Order Items (Itens do Pedido)

| Atributo | Valor |
|---|---|
| **Tipo** | Fonte de Fato |
| **Domínio** | `order_items` |
| **Nível de PII** | Baixo |

#### Campos

| Campo | Tipo | Descrição | PII / Tags |
|---|---|---|---|
| `order_id` | StringType | Identificador único do pedido (FK → orders) | — |
| `order_item_id` | IntegerType | Número sequencial do item dentro do pedido | — |
| `product_id` | StringType | Identificador único do produto (FK → products) | — |
| `seller_id` | StringType | Identificador único do vendedor | — |
| `shipping_limit_date` | TimestampType | Data limite de envio pelo vendedor | — |
| `price` | DoubleType | Preço do item | — |
| `freight_value` | DoubleType | Valor do frete do item | — |

#### Campos Derivados — Silver

| Campo | Lógica |
|---|---|
| `total_item_value` | `col("price") + col("freight_value")` |

#### Restrições de Qualidade de Dados

| Restrição | Expressão |
|---|---|
| `valid_order_id` | `order_id IS NOT NULL` |
| `valid_product_id` | `product_id IS NOT NULL` |
| `valid_price` | `price >= 0` |

> **Template:** Use o **Template 2** de `templates/pipeline_templates.mdc` para `silver_order_items`.

---

### 3.3 Customers (Clientes — Dimensão SCD Type 2)

| Atributo | Valor |
|---|---|
| **Tipo** | Dimensão — SCD Type 2 |
| **Domínio** | `customers` |
| **Nível de PII** | Alto |

#### Campos

| Campo | Tipo | Descrição | PII / Tags |
|---|---|---|---|
| `customer_id` | StringType | Chave para o dataset de pedidos — única por pedido | — |
| `customer_unique_id` | StringType | Identificador único do cliente (pessoa) | — |
| `customer_zip_code_prefix` | StringType | Primeiros cinco dígitos do CEP do cliente | PII |
| `customer_city` | StringType | Nome da cidade do cliente | — |
| `customer_state` | StringType | Estado do cliente | — |

#### Campos Derivados — Silver

| Campo | Lógica |
|---|---|
| `customer_location` | `concat(col("customer_city"), lit(", "), col("customer_state"))` |

#### Restrições de Qualidade de Dados

Aplicadas na view de pré-processamento antes do `apply_changes`:

| Restrição | Expressão |
|---|---|
| `valid_customer_id` | `customer_id IS NOT NULL` |
| `valid_unique_id` | `customer_unique_id IS NOT NULL` |

> **Template:** Use o **Template 3** de `templates/pipeline_templates.mdc` para `silver_customers`.

---

### 3.4 Products (Produtos)

| Atributo | Valor |
|---|---|
| **Tipo** | Dimensão |
| **Domínio** | `products` |
| **Nível de PII** | Baixo |

#### Campos

| Campo | Tipo | Descrição | PII / Tags |
|---|---|---|---|
| `product_id` | StringType | Identificador único do produto (PK) | — |
| `product_category_name` | StringType | Nome da categoria raiz em português (FK → product_category) | — |
| `product_name_lenght` | IntegerType | Número de caracteres no nome do produto | — |
| `product_description_lenght` | IntegerType | Número de caracteres na descrição do produto | — |
| `product_photos_qty` | IntegerType | Número de fotos publicadas do produto | — |
| `product_weight_g` | DoubleType | Peso do produto em gramas | — |
| `product_length_cm` | DoubleType | Comprimento do produto em centímetros | — |
| `product_height_cm` | DoubleType | Altura do produto em centímetros | — |
| `product_width_cm` | DoubleType | Largura do produto em centímetros | — |

#### Campos Derivados — Silver

| Campo | Lógica |
|---|---|
| `product_volume_cm3` | `col("product_length_cm") * col("product_height_cm") * col("product_width_cm")` |
| `product_category_name_english` | Join com `workspace.silver.silver_product_category` em `product_category_name` |

#### Restrições de Qualidade de Dados

| Restrição | Expressão |
|---|---|
| `valid_product_id` | `product_id IS NOT NULL` |
| `valid_weight` | `product_weight_g > 0` |

> **Template:** Use o **Template 2** de `templates/pipeline_templates.mdc` para `silver_products`.

---

### 3.5 Product Category (Categoria do Produto — Referência / Lookup)

| Atributo | Valor |
|---|---|
| **Tipo** | Referência / Lookup |
| **Domínio** | `product_category` |
| **Nível de PII** | Baixo |

#### Campos

| Campo | Tipo | Descrição | PII / Tags |
|---|---|---|---|
| `product_category_name` | StringType | Nome da categoria em português (PK) | — |
| `product_category_name_english` | StringType | Nome da categoria em inglês | — |

> Dataset estático de lookup — não requer SCD Type 2. Carregue como streaming table no Silver e faça join com products para enriquecer com o nome em inglês.

> **Template:** Use o **Template 2** de `templates/pipeline_templates.mdc` para `silver_product_category`.

---

## 4. Camada Silver — SCD Type 2 (Customers)

> **Crítico:** Em PySpark, o SCD Type 2 é implementado com `dlt.apply_changes()`. Isso requer **três definições separadas**:
> 1. `@dlt.view` — view de pré-processamento para filtros e campos derivados.
> 2. `dlt.create_streaming_table()` — declara a tabela alvo.
> 3. `dlt.apply_changes()` — aplica as mudanças CDC.
>
> Todas no mesmo arquivo Python, mas são chamadas independentes — não decoradores aninhados.

> `apply_changes()` não aceita filtros diretamente. Se precisar de filtragem ou validação de qualidade de dados antes do CDC, use uma `@dlt.view` intermediária — conforme o Template 3.

---

## 5. Camada Gold — Star Schema

### 5.1 dim_customers (Dimensão de Clientes)

| Atributo | Valor |
|---|---|
| **Tipo** | Dimensão (SCD Type 2 — snapshot atual) |
| **Fonte** | `workspace.silver.silver_customers` |
| **PK** | `customer_id` |

#### Campos

| Campo | Origem |
|---|---|
| `customer_id` | `silver_customers.customer_id` |
| `customer_unique_id` | `silver_customers.customer_unique_id` |
| `customer_zip_code_prefix` | `silver_customers.customer_zip_code_prefix` |
| `customer_city` | `silver_customers.customer_city` |
| `customer_state` | `silver_customers.customer_state` |
| `customer_location` | `silver_customers.customer_location` |
| `_dimension_refresh_timestamp` | `current_timestamp()` |

> Filtrar `__END_AT IS NULL` para obter apenas o registro atual de cada cliente (snapshot ativo do SCD Type 2).

> **Template:** Use o **Template 4** de `templates/pipeline_templates.mdc`.

---

### 5.2 dim_products (Dimensão de Produtos)

| Atributo | Valor |
|---|---|
| **Tipo** | Dimensão |
| **Fonte** | `workspace.silver.silver_products` |
| **PK** | `product_id` |

#### Campos

| Campo | Origem |
|---|---|
| `product_id` | `silver_products.product_id` |
| `product_category_name` | `silver_products.product_category_name` |
| `product_category_name_english` | `silver_products.product_category_name_english` |
| `product_weight_g` | `silver_products.product_weight_g` |
| `product_length_cm` | `silver_products.product_length_cm` |
| `product_height_cm` | `silver_products.product_height_cm` |
| `product_width_cm` | `silver_products.product_width_cm` |
| `product_volume_cm3` | `silver_products.product_volume_cm3` |
| `_dimension_refresh_timestamp` | `current_timestamp()` |

> **Template:** Use o **Template 4** de `templates/pipeline_templates.mdc`.

---

### 5.3 fct_order_items (Tabela Fato — Única)

> **Star Schema:** Esta é a **única tabela fato** do modelo. Grain = 1 linha por item de pedido. Ela consolida atributos de nível de item (`silver_order_items`), atributos de nível de pedido (`silver_orders`) e atributos desnormalizados de ambas as dimensões (`dim_customers`, `dim_products`). `fct_orders` não existe — seus campos são absorvidos aqui.

| Atributo | Valor |
|---|---|
| **Tipo** | Fato (única fact table do star schema) |
| **Fonte principal** | `workspace.silver.silver_order_items` |
| **Joins** | `workspace.silver.silver_orders` em `order_id` · `workspace.gold.dim_customers` em `customer_id` · `workspace.gold.dim_products` em `product_id` |
| **PK** | `order_id` + `order_item_id` |

#### Campos

| Campo | Origem | Grupo |
|---|---|---|
| `order_id` | `silver_order_items.order_id` | Chave |
| `order_item_id` | `silver_order_items.order_item_id` | Chave |
| `product_id` | `silver_order_items.product_id` | FK → dim_products |
| `customer_id` | `silver_orders.customer_id` | FK → dim_customers |
| `seller_id` | `silver_order_items.seller_id` | Atributo |
| `order_status` | `silver_orders.order_status` | Atributo de pedido |
| `order_purchase_timestamp` | `silver_orders.order_purchase_timestamp` | Atributo de pedido |
| `order_approved_at` | `silver_orders.order_approved_at` | Atributo de pedido |
| `order_delivered_customer_date` | `silver_orders.order_delivered_customer_date` | Atributo de pedido |
| `order_estimated_delivery_date` | `silver_orders.order_estimated_delivery_date` | Atributo de pedido |
| `shipping_limit_date` | `silver_order_items.shipping_limit_date` | Atributo de item |
| `delivery_delay_days` | `silver_orders.delivery_delay_days` | Métrica de pedido |
| `order_processing_days` | `silver_orders.order_processing_days` | Métrica de pedido |
| `is_late_delivery` | `silver_orders.is_late_delivery` | Métrica de pedido |
| `price` | `silver_order_items.price` | Métrica de item |
| `freight_value` | `silver_order_items.freight_value` | Métrica de item |
| `total_item_value` | `silver_order_items.total_item_value` | Métrica de item |
| `customer_city` | `dim_customers.customer_city` | Desnorm. dim_customers |
| `customer_state` | `dim_customers.customer_state` | Desnorm. dim_customers |
| `customer_location` | `dim_customers.customer_location` | Desnorm. dim_customers |
| `product_category_name` | `dim_products.product_category_name` | Desnorm. dim_products |
| `product_category_name_english` | `dim_products.product_category_name_english` | Desnorm. dim_products |
| `product_volume_cm3` | `dim_products.product_volume_cm3` | Desnorm. dim_products |
| `_fact_processing_timestamp` | `current_timestamp()` | Auditoria |

> **Template:** Use o **Template 5** de `templates/pipeline_templates.mdc`.

---

## 6. Estrutura de Arquivos do Pipeline

```
olist_ecommerce/
├── databricks.yml
├── README.md
└── src/
    ├── bronze/
    │   ├── bronze_orders.py
    │   ├── bronze_order_items.py
    │   ├── bronze_customers.py
    │   ├── bronze_products.py
    │   └── bronze_product_category.py
    ├── silver/
    │   ├── silver_orders.py
    │   ├── silver_order_items.py
    │   ├── silver_customers.py
    │   ├── silver_products.py
    │   └── silver_product_category.py
    └── gold/
        ├── dim_customers.py
        ├── dim_products.py
        └── fct_order_items.py
```

---

## 7. Regras Gerais

1. **Nomenclatura** — sempre `catalog.schema.table` com 3 partes. Nunca `LIVE.*`.
2. **Bronze** — somente `spark.readStream.format("cloudFiles")`. Nunca transforme dados nesta camada.
3. **Silver** — aplique limpeza, cast de tipos, campos derivados e restrições de qualidade (`@dlt.expect_or_drop`).
4. **Gold** — leitura streaming (`dlt.read_stream()`). Isso garante que o DLT crie **Streaming Tables** (tabelas Delta reais) no Unity Catalog, em vez de **Materialized Views**. O DLT cria Materialized View quando detecta leitura batch (`dlt.read()`), e Streaming Table quando detecta `dlt.read_stream()` — mesmo que a função use `@dlt.table`. CDF desabilitado. **Star schema com 1 fact table (`fct_order_items`) e 2 dimensões (`dim_customers`, `dim_products`)** — nunca crie mais de uma tabela fato; consolide em `fct_order_items`.
5. **SCD Type 2** — somente para `customers`. Três chamadas separadas no mesmo arquivo Python.
6. **`schemaLocation`** — sempre em Unity Catalog Volume (`/Volumes/...`). Nunca `/tmp/`.
7. **`delta.enableChangeDataFeed`** — `"true"` para Bronze e Silver; `"false"` para Gold.
8. **Timestamps de auditoria** — Bronze: `_ingest_timestamp`. Silver: `_processing_timestamp`. Gold dimensão: `_dimension_refresh_timestamp`. Gold fato: `_fact_processing_timestamp`.
9. **Deploy** — via `databricks bundle deploy` com o perfil correto configurado no `~/.databrickscfg`.
