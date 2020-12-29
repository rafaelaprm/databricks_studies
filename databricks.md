## Databricks Workspace

Anotações feitas a partir da documentação oficial do Azure Databricks. Disponível em [Databricks Workspace user](https://docs.databricks.com/getting-started/quick-start.html).

O Databricks oferece dois ambientes para o desenvolvimento de aplicativos com uso intensivo de dados: **Databricks SQL Analytics** e **Databricks Workspace**.

1. **SQL Analytics** 

O SQL Analytics fornece uma plataforma fácil de usar para analistas que desejam executar consultas SQL em seus data lake, criar vários tipos de visualização para explorar os resultados da consulta de diferentes perspectivas e construir e compartilhar painéis. 

2. **Workspace** 

O Workspace fornece uma plataforma analítica unificada e colaborativa para engenheiros de dados, cientistas de dados e engenheiros de ML, que oferece suporte a ativos de código (notebooks, projetos e modelos) e recursos computacionais (clusters, jobs). 

### Crie um cluster, um notebook e em seguida, crie uma tabela.

Opção 1: crie uma tabela Spark a partir dos dados CSV

```sql
DROP TABLE IF EXISTS diamonds;

CREATE TABLE diamonds USING CSV OPTIONS (path "/databricks-datasets/Rdatasets/data-001/csv/ggplot2/diamonds.csv", header "true")
```

Opção 2: Grave os dados CSV no formato Delta Lake e crie uma tabela Delta

Delta Lake oferece uma camada de armazenamento transacional poderosa que permite leituras rápidas e outros benefícios. O formato Delta Lake consiste em arquivos Parquet mais um registro de transações. Use esta opção para obter o melhor desempenho em operações futuras.

- Leia os dados CSV em um DataFrame e grave no formato Delta Lake.

```python
%python #permite intercalar linguagens diferentes

diamonds = spark.read.csv("/databricks-datasets/Rdatasets/data-001/csv/ggplot2/diamonds.csv", header="true", inferSchema="true")

diamonds.write.format("delta").save("/mnt/delta/diamonds")
```

- Crie uma tabela Delta no local armazenado.

```sql
DROP TABLE IF EXISTS diamonds;

CREATE TABLE diamonds USING DELTA LOCATION '/mnt/delta/diamonds/'
```

### Consultar tabela

Execute uma instrução SQL para consultar a tabela para o preço médio do diamante por cor.

```sql
SELECT color, avg(price) AS price FROM diamonds GROUP BY color ORDER BY COLOR
```

### Exibir os dados em forma de gráfico

- Clique no ícone do gráfico de barras;
- Clique em Opções de plotagem: Arraste a cor para a caixa Keys,  arraste o preço para a caixa Values e no menu Agg, selecione AVG.


### API Python DataFrame

Para passar instruções de comando para um interpretador Python, inclua o comando mágico `% python`. O próximo comando cria um DataFrame a partir de um Databricks dataset.

```python
%python
diamonds = spark.read.csv("/databricks-datasets/Rdatasets/data-001/csv/ggplot2/diamonds.csv", header="true", inferSchema="true")
```

O próximo comando manipula os dados e exibe os resultados.

```python
%python
from pyspark.sql.functions import avg

display(diamonds.select("color","price").groupBy("color").agg(avg("price")).sort("color"))
```

## 
