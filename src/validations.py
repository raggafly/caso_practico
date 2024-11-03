from pyspark.sql import functions as F
from functools import reduce

# Definir las funciones de validación y transformación
validation_map = {
    "notNull": lambda col_name: F.col(col_name).isNotNull(),
    "notEmpty": lambda col_name: (F.col(col_name) != "") & F.col(col_name).isNotNull(),
}

transformation_map = {
    "add_fields": lambda df, params: add_fields(df, params),
}

# Mapear errores de validación con información detallada
validation_error_map = {
    "notNull": lambda col_name: F.when(F.col(col_name).isNull(), F.struct(
        F.lit(col_name).alias("field"),
        F.lit("notNull").alias("validation"),
        F.lit("Field is null").alias("error_message")
    )).otherwise(F.lit(None)),
    "notEmpty": lambda col_name: F.when((F.col(col_name) == "") | F.col(col_name).isNull(), F.struct(
        F.lit(col_name).alias("field"),
        F.lit("notEmpty").alias("validation"),
        F.lit("Field is empty").alias("error_message")
    )).otherwise(F.lit(None)),
}

# Función para agregar campos (en este caso, dt con la función current_timestamp)
def add_fields(df, params):
    for field in params['addFields']:
        if field['function'] == 'current_timestamp':
            df = df.withColumn(field['name'], F.current_timestamp())
    return df

def apply_validations(df, validations):
    """
    Aplica las validaciones dinámicamente a un DataFrame según los metadatos proporcionados.
    
    :param df: El DataFrame de entrada en el que se aplican las validaciones.
    :param validations: Lista de diccionarios que contiene las validaciones y los campos.
    :return: Un DataFrame con los registros válidos y otro con los registros inválidos.
    """
    valid_conditions = []
    invalid_conditions = []
    error_columns = []

    # Recorrer las validaciones en los metadatos
    for validation in validations:
        field = validation['field']
        validation_types = validation['validations']
        
        for validation_type in validation_types:
            if validation_type in validation_map:
                # Verificamos si está en el diccionario
                valid_conditions.append(validation_map[validation_type](field))
                invalid_conditions.append(~validation_map[validation_type](field))
                
                if validation_type in validation_error_map:
                    error_columns.append(validation_error_map[validation_type](field))

    # Combinar condiciones con `&` para válidos y `|` para inválidos
    df_valid = df.filter(reduce(lambda x, y: x & y, valid_conditions))
    df_invalid = df.filter(reduce(lambda x, y: x | y, invalid_conditions))

    # Agregar el campo arraycoderrorbyfield al DataFrame inválido
    if error_columns:
        # Agregar un campo que contenga una lista de errores de validación
        df_invalid = df_invalid.withColumn(
            "arraycoderrorbyfield",
            F.array(*error_columns).alias("arraycoderrorbyfield")
        )

    return df_valid, df_invalid

def apply_transformations(df, transformations):
    """
    Aplica las transformaciones al DataFrame según los metadatos proporcionados.
    
    :param df: El DataFrame al que se aplicarán las transformaciones.
    :param transformations: Lista de transformaciones.
    :return: El DataFrame transformado.
    """
    # if transformation['name'] == "ok_with_date":
    params = transformations.params
    df = transformation_map[transformations.type](df, params)
    
    return df
