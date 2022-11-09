# Databricks notebook source
from pyspark.sql.functions import col

# COMMAND ----------

driver = "org.postgresql.Driver"

database_host = "pgresql-fabiomelo-001.postgres.database.azure.com"
database_port = "5432" # update if you use a non-default port
database_name = "teste" # eg. postgres
table = "cartao" # if your table is in a non-default schema, set as <schema>.<table-name>
user = "fabiomelo"
password = "#######"

url = f"jdbc:postgresql://{database_host}:{database_port}/{database_name}"

print(url)

# COMMAND ----------

# MAGIC %sh
# MAGIC nc -vz "pgresql-fabiomelo-001.postgres.database.azure.com" 5432

# COMMAND ----------

dfTableCartao = (spark.read.format("jdbc").option("driber",driver).option("url", url).option("dbtable", "cartao").option("user",user).option("password",password).load())
dfTableAssociado = (spark.read.format("jdbc").option("driber",driver).option("url", url).option("dbtable", "associado").option("user",user).option("password",password).load())
dfTableConta = (spark.read.format("jdbc").option("driber",driver).option("url", url).option("dbtable", "conta").option("user",user).option("password",password).load())
dfTableMovimento = (spark.read.format("jdbc").option("driber",driver).option("url", url).option("dbtable", "movimento").option("user",user).option("password",password).load())

# COMMAND ----------

dfMovimentoFlatFile = (dfTableMovimento.alias("m").join(dfTableCartao.alias("c"), col("m.id_cartao")==col("c.id"),"inner" ).join(dfTableConta.alias("ct"), col("c.id_conta")==col("ct.id"),"inner").join(dfTableAssociado.alias("a"), col("c.id_associado")==col("a.id"),"inner").select(col("a.nome").alias("nome_associado"), col("a.sobrenome").alias("sobrenome_associado"), col("a.idade").alias("idade_associado"), col("m.vlr_transacao").alias("vlr_transacao_movimento"), col("m.des_transacao").alias("des_transacao_movimento"), col("m.data_movimento").alias("data_movimento"), col("c.num_cartao").alias("numero_cartao"), col("c.nom_impresso").alias("nome_impresso_cartao"), col("c.data_criacao").alias("data_criacao_cartao"),col("ct.tipo").alias("tipo_conta"), col("ct.data_criacao").alias("data_criacao_conta")))

# COMMAND ----------

dfMovimentoFlatFile.coalesce(1).write.option("header","true").csv("abfss://sacfabiomelo.dfs.core.windows.net/fabiomelo/landing/movimento_flat.csv")
