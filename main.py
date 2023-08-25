from pyspark.sql import SparkSession
from pyspark.sql.functions import stddev

spark = SparkSession.builder.appName("my_app").getOrCreate()

df = spark.read.option("delimiter", ",").csv("C:/Users/USER/Downloads/CODE/TOKIO/bigdata/modulo 5/EDA/stocks_2021.csv", header=True, inferSchema=True)

df_gold = df.filter(df.ticker == 'GOLD')

df_both = df_gold.groupBy(df_gold.ticker == 'GOLD').avg('open','low').show()


# # Calculamos o desvio padrão para a coluna open
open_stddev = df_gold.select(stddev('open')).first()

# # Calculamos o desvio padrão para a coluna low
low_stddev = df_gold.select(stddev('low')).first()

# mostramos os valores de desvio padrão calculados
print(f'O desvio padrão relativo a coluna open é: {open_stddev[0]}')
print(f'O desvio padrão relativo a coluna low é: {low_stddev[0]}')











