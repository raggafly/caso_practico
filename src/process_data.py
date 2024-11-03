from hdfs import InsecureClient
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import os
from pyspark.sql.functions import col, when, current_timestamp
import json
from validations import apply_validations, apply_transformations
from write_sink import write_to_sink, write_to_sink_kafka
import os

class Source:
    def __init__(self, name, path, format):
        self.name = name
        self.path = path
        self.format = format

class Transformation:
    def __init__(self, name, type, params):
        self.name = name
        self.type = type
        self.params = params

class Sink:
    def __init__(self, input, name, topics=None, paths=None, format=None, saveMode=None):
        self.input = input
        self.name = name
        self.topics = topics
        self.paths = paths
        self.format = format
        self.saveMode = saveMode

class DataFlow:
    def __init__(self, name, sources, transformations, sinks):
        self.name = name
        self.sources = [Source(**source) for source in sources]
        self.transformations = [Transformation(**transformation) for transformation in transformations]
        self.sinks = [Sink(**sink) for sink in sinks]

# Clase principal que carga los metadatos
class MetaData:
    def __init__(self, dataflows):
        self.dataflows = [DataFlow(**dataflow) for dataflow in dataflows]

def convertir_ruta_a_hdfs(ruta_local):
    # Quitar el prefijo "./" si está presente
    ruta_relativa = ruta_local.lstrip("./")
    
    # Construir la ruta HDFS
    ruta_hdfs = os.path.join("/user/root", ruta_relativa)
    
    # Quitar cualquier patrón de wildcard (*) al final de la ruta
    ruta_hdfs = ruta_hdfs.rstrip("/*")
    print(f"ruta formateada:{ruta_hdfs}")
    return ruta_hdfs

# Acceder a las fuentes
def get_source_data(spark, dataflow,client):
    for source in dataflow.sources:
        print(f"Source Name: {source.name}, Path: {source.path}, Format: {source.format}")
        input_path = source.path
        raw_path = convertir_ruta_a_hdfs(input_path)
        # Leer desde HDFS = 
        # df = spark.read.json("hdfs://namenode:9000/user/root/data/input/events/person/")
        data_list = []
        # Listar archivos en la carpeta
        path = client.list(raw_path)
        
        # Iterar y procesar cada archivo JSON
        for file_name in path:
            if file_name.endswith(".json"):
                file = os.path.join (raw_path, file_name)
                print(f'>>>> Procesando ruta: {file}')
                with client.read(file) as file:
                    data = json.load(file)  # Cargar el JSON del archivo
                    data_list.extend(data)  # Agregar el JSON a la lista
        
        # Si tienes un esquema definido, lo puedes aplicar aquí (opcional)
        # schema = StructType([...])
        
        # Crear un DataFrame a partir de la lista de diccionarios (data_list)
        df = spark.createDataFrame(data_list)
        
        # Mostrar el DataFrame
        

        df_valid,df_invalid = do_transformations(dataflow, df, source.name)
    return df_valid,df_invalid


# realizar las transformaciones
def do_transformations(dataflow, df, source_name):
    for transformation in dataflow.transformations:
        print(f"Transformation: {transformation.name}, Type: {transformation.type}")
        # Validaciones
        if transformation.type == "validate_fields" and 'validations' in transformation.params and 'input' in transformation.params and  transformation.params['input'] == source_name:
            # si el input de transformations es igual al name de sources        
            for validation in transformation.params['validations']:                
                field = validation.get('field')
                rules = validation.get('validations')
                print(f"Field: {field}, Validations: {rules}")
                df_valid, df_invalid = apply_validations(df, transformation.params['validations'])
       
        if transformation.name == 'ok_with_date':           
            df_valid = apply_transformations(df_valid, transformation)

    return df_valid,df_invalid

# Crear una sesión de Spark
spark = SparkSession.builder \
    .appName("Leer JSON desde HDFS") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
    .config("spark.hadoop.fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem") \
    .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# Definir esquema
schema = StructType([
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("office", StringType(), True)
])

# Crear un cliente para acceder a HDFS
client = InsecureClient('http://namenode:9870', user='root')

# Leer el archivo JSON desde HDFS
with client.read('/user/root/metadata/metadata.json') as file:
    metadata = json.load(file)

# Cargar los metadatos en la clase MetaData
metadata_obj = MetaData(metadata['dataflows'])

dataflow = metadata_obj.dataflows[0]

print("Nombre del dataflow:", dataflow.name)

df_valid,df_invalid = get_source_data(spark, dataflow, client)


# Acceder a los sinks
for sink in dataflow.sinks:
    
    print(f"Sink Name: {sink.name}, Input: {sink.input}, Format: {sink.format}")
    if sink.input =='validation_ko':
        write_to_sink(df_invalid, sink)
    if sink.input =='ok_with_date':
        write_to_sink_kafka(df_valid, sink)

df_invalid.show()
df_valid.show()
