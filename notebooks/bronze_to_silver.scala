// Databricks notebook source
val path = "dbfs:/mnt/dados/bronze/dataset_imoveis/"
val df = spark.read.format("delta").load(path)

// COMMAND ----------

display(df)

// COMMAND ----------

// DBTITLE 1,Transformando os dados json em colunas
display(df.select("anuncio.*"))

// COMMAND ----------

display(df.select("anuncio.*","anuncio.endereco.*"))

// COMMAND ----------

val dados_datalhados = df.select("anuncio.*","anuncio.endereco.*")

// COMMAND ----------

display(dados_datalhados)

// COMMAND ----------

// DBTITLE 1,Removendo colunas
val df_silver = dados_datalhados.drop("caracteristicas", "endereco")
display(df_silver)

// COMMAND ----------

// DBTITLE 1,Salvando na camada silver
val path = "dbfs:/mnt/dados/silver/dataset_imoveis"
df_silver.write.format("delta").mode("overwrite").save(path)
