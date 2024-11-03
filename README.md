# Proyecto Spark con Docker y HDFS

Este proyecto ejecuta un script de procesamiento de datos en un entorno Dockerizado que incluye Spark y HDFS. Utilizamos `spark-submit` para lanzar el script `process_data.py` en un contenedor Docker configurado con los servicios necesarios.

## Estructura del Proyecto

- `src/process_data.py`: Script principal de procesamiento de datos.
- `requirements.txt`: Dependencias de Python necesarias para ejecutar el script.

## Requisitos

- Docker y Docker Compose instalados.
- Python 3.x para instalar las dependencias en `requirements.txt` si corres el script fuera del contenedor.

## Configuración del Entorno

Este proyecto asume que tienes un contenedor Docker llamado `docker-hadoop-pyspark-1` configurado para ejecutar Spark, y un servicio `namenode` para HDFS en el puerto 9000.

### Instalación de Dependencias

Para instalar las dependencias de Python en tu entorno local, usa:

```bash
pip install -r requirements.txt
