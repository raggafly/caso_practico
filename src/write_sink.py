def write_to_sink(df, sink):
    """
    Escribe el DataFrame en el formato y la ruta especificados en el sink.

    :param df: El DataFrame a escribir.
    :param sink: Diccionario que contiene la configuración del sink.
    """
    # Leer formato y rutas del sink
    format = sink.format
    paths = sink.paths  # Obtener paths, por defecto es una lista vacía

    # Validar formato y aplicar la escritura adecuada
    save_mode = sink.saveMode
    df.show()
    if format == "JSON":
        for path in paths:
            df.write.mode(save_mode).json(path)
    elif format == "CSV":
        for path in paths:
            df.write.mode(save_mode).csv(path)
    elif format == "PARQUET":
        for path in paths:
            df.write.mode(save_mode).parquet(path)
    else:
        raise ValueError(f"Formato no soportado: {format}")

def write_to_sink_kafka_old(df, sink):
    format = sink.format
    print(type(df))
    print(df)
    print(f"conteooo {df.count()}")
    from confluent_kafka import Producer
    
    print(df.selectExpr("CAST(name AS STRING) AS key", "to_json(struct(*)) AS value"))
    if format == 'KAFKA':
        df.selectExpr("CAST(name AS STRING) AS key", "to_json(struct(*)) AS value").write.format("kafka").option("kafka.bootstrap.servers", "localhost:9092").option("topic", "person").save()

def write_to_sink_kafka(df, sink, kafka_bootstrap_servers="broker:9092", topic="person"):
    """
    Escribe un DataFrame de PySpark en un tópico de Kafka.
    
    :param df: DataFrame de PySpark.
    :param sink: Objeto con el formato de salida (en este caso, debería ser 'KAFKA').
    :param kafka_bootstrap_servers: Dirección de los servidores de Kafka (por defecto: localhost:9092).
    :param topic: Nombre del tópico en Kafka (por defecto: 'person').
    """
    # Verificar el tipo de formato
    format = sink.format
    
    # Verifica el tipo de DataFrame y su contenido
    print(f"Tipo de df: {type(df)}")
    print(f"Contenido del df: {df.show(truncate=False)}")
    print(f"Conteo del df: {df.count()}")

    try:
        # Si el formato es Kafka, se envía el DataFrame
        if format.upper() == 'KAFKA':
            # Selecciona las columnas 'key' y 'value' para Kafka
            kafka_df = df.selectExpr("CAST(name AS STRING) AS key", "to_json(struct(*)) AS value")
            
            # Verifica el contenido antes de escribir
            print("Contenido a escribir en Kafka:")
            kafka_df.show(truncate=False)
            
            # Escritura en Kafka
            kafka_df.write \
                .format("kafka") \
                .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
                .option("topic", topic) \
                .save()

            print("Escritura en Kafka completada con éxito.")
        else:
            print(f"El formato '{format}' no es compatible con Kafka.")
    
    except Exception as e:
        print(f"Error durante la escritura en Kafka: {e}")

