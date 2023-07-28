// Databricks notebook source
// MAGIC %fs
// MAGIC ls /mnt/dados/land

// COMMAND ----------

// DBTITLE 1,Lendo os dados na camada land
val path = "dbfs:/mnt/dados/land/dados_brutos_imoveis.json"
val dados = spark.read.json(path)


// COMMAND ----------

display(dados)

// COMMAND ----------

// DBTITLE 1,Removendo colunas 
val dados_anuncio = dados.drop("imagens","usuario")
display(dados_anuncio)

// COMMAND ----------

import org.apache.spark.sql.functions.col

// COMMAND ----------

// DBTITLE 1,Criando uma coluna de ID

val df_bronze = dados_anuncio.withColumn("id", col("anuncio.id"))
display(df_bronze)

// COMMAND ----------

// DBTITLE 1,Salvando na camada bronze
val path_gravacao = "dbfs:/mnt/dados/bronze/dataset_imoveis"
df_bronze.write.format("delta").mode(SaveMode.Overwrite).save(path_gravacao)
