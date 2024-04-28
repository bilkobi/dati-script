from pyspark.sql import SparkSession
spark=SparkSession.builder \
          .appName('EsempioSQL') \
          .getOrCreate()
df=spark.read.csv('dati-script/conto.csv', header=True, sep=';', inferSchema=True)
df.createOrReplaceTempView('conto')
risultato=spark.sql("SELECT * FROM conto WHERE codice='BA'")
risultato.write.json("query_json")

df_descr=spark.createDataFrame(
    [('AS','Accredito stipendio'),
     ('BA','Bonifico a'),
     ('BD','Bonifico da'),
     ('PC','Prelievo contanti'),
     ('PB','Pagobancomat'),
     ('BI', 'Bolli e imposte'),
     ('VS','Versamento')],
     ['codice', 'descrizione']
)

df_res=spark.sql("""SELECT codice, SUM(importo) as totale
           FROM conto
           GROUP BY codice""") \
      .join(df_descr, on='codice') \
      ['descrizione', 'totale'] \
      .orderBy('totale', ascending=False)
df_res.write.parquet("rslt_join")
spark.stop()
