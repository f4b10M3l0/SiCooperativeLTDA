Olá, tudo bem?

Provisionei as maquinas na minha conta pessoal da azure (Cloud):

-banco de dados postgreSQL
-databricks 
-storage account 

Criei o banco de dados "teste" com o script DDL_SiCooperativeLTDA.sql. Populei os dados com o script DML_SiCooperativeLTDA.sql. Inclui a coluna "data_criacao" na tabela "cartao", visto que no flatfile foi solicitado.

Via databricks criei um cluster Standard D4s_V5 (16GB Mem e 4 Cores) com 2 workes escalável para 4 workes, com runtime: 11.0 (scala 2.12, Spark 3.3.0)

Via databricks criei um notebook para conectar via JDBC na base do postgreSQL, via pyspark fiz a leitura das tabelas e estruturei a consulta para gerar o arquivo flat.

Escrevi os dados resultantes num arquivo CSV no storage account na pasta landing.

Para automatizar (caso tivesse mais tempo) eu poderia orquestrar via Azure Data Factory, ou via pipeline com job cluster no próprio Databricks.