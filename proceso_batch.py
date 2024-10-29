
from pyspark.sql import SparkSession

# Crear una sesión de Spark
spark = SparkSession.builder \
    .appName("Batch Processing") \
    .getOrCreate()

# Cargar el conjunto de datos
data_path = "C:\Users\tatia\Documents\BIG DATA\kafka_2.13-3.6.0\bin\windows\dataset.csv"  # Reemplaza con la ruta real
df = spark.read.csv(data_path, header=True, inferSchema=True)

# Operaciones de limpieza
df_cleaned = df.dropna()  # Eliminar filas con valores nulos

# Ejemplo de transformación: renombrar una columna
df_transformed = df_cleaned.withColumnRenamed("old_name", "new_name")  # Reemplaza "old_name" y "new_name"

#Analisis exploratorio de Datos EDA 
df_transformed.describe().show()  # Estadísticas descriptivas
df_transformed.groupBy("column_name").count().show()  # Reemplaza "column_name" con el nombre real

# Almacena los datos obtenidos
df_transformed.write.parquet("C:\Users\tatia\Documents\BIG DATA\kafka_2.13-3.6.0\bin\windows")  

from pyspark import SparkConf
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

# Configurar Spark Streaming
conf = SparkConf().setAppName("Streaming Example")
sc = SparkContext(conf=conf)
ssc = StreamingContext(sc, 10)  # Cada 10 segundos

# Conectar a Kafka
kafkaStream = KafkaUtils.createDirectStream(ssc, ["test_topic"], {"metadata.broker.list": "localhost:9092"})

# Procesar los datos
def process_rdd(time, rdd):
    if not rdd.isEmpty():
        # Realiza el procesamiento o análisis
        count = rdd.count()
        print(f"Contando eventos: {count}")

kafkaStream.foreachRDD(process_rdd)
ssc.start()
ssc.awaitTermination()

