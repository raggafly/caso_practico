from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 10, 19),
    'retries': 1
}

with DAG('ETL_PERSON',
         default_args=default_args,
         schedule_interval=None,  # Ejecutar bajo demanda
         catchup=False) as dag:

    # Parte 1
    levantar_zookeeper = BashOperator(
        task_id='levantar_zookeeper',
        bash_command='docker start docker-hadoop-zookeeper-1'
    )

    esperar_zookeeper = BashOperator(
        task_id='esperar_zookeeper',
        bash_command="""
        until nc -z docker-hadoop-zookeeper-1 2181; do
            echo "Esperando a que Zookeeper esté disponible..."
            sleep 5
        done
        """
    )

    levantar_broker = BashOperator(
        task_id='levantar_broker',
        bash_command='docker start 7c7b193e1f47_broker'
    )

    levantar_spark = BashOperator(
        task_id='levantar_spark',
        bash_command='docker start docker-hadoop-spark-1'
    )

    levantar_worker = BashOperator(
        task_id='levantar_worker',
        bash_command='docker start docker-hadoop-spark-worker-1'
    )

    # Parte 2
    levantar_nodemanager = BashOperator(
        task_id='levantar_nodemanager',
        bash_command='docker start nodemanager'
    )

    levantar_datanode = BashOperator(
        task_id='levantar_datanode',
        bash_command='docker start datanode'
    )

    levantar_resourcemanager = BashOperator(
        task_id='levantar_resourcemanager',
        bash_command='docker start resourcemanager'
    )

    levantar_namenode = BashOperator(
        task_id='levantar_namenode',
        bash_command='docker start namenode'
    )

    levantar_historyserver = BashOperator(
        task_id='levantar_historyserver',
        bash_command='docker start historyserver'
    )

    levantar_pyspark = BashOperator(
        task_id='levantar_pyspark',
        bash_command='docker start docker-hadoop-pyspark-1'
    )
    
    # COPIADO DE ARCHIVOS
    copiar_archivos_node = BashOperator(
        task_id='copiar_archivos_node',
        bash_command='docker cp /opt/airflow/dags/data_process/metadata/ namenode:/tmp/metadata/ && docker cp /opt/airflow/dags/data_process/person/ namenode:/tmp/person/'
    )
    # Nos aseguramos de que la carpeta origen existe antes de hacer el put
    copiar_archivos_hdfs = BashOperator(
        task_id='copiar_archivos_hdfs',
        bash_command='docker exec namenode bash -c "hdfs dfs -rm -r /user/root/metadata && hdfs dfs -mkdir -p /user/root/ && hdfs dfs -put /tmp/metadata /user/root/"'
    )

    # Nos aseguramos de que la carpeta origen existe antes de hacer el put
    copiar_archivos_hdfs_person = BashOperator(
        task_id='copiar_archivos_hdfs_person',
        bash_command='docker exec namenode bash -c "hdfs dfs -rm -r /user/root/data/input/events/person/ && hdfs dfs -mkdir -p /user/root/data/input/events/person/ && hdfs dfs -put /tmp/person /user/root/data/input/events/"'
    )
    # FIN COPIADO DE ARCHIVOS

    # Tarea para ejecutar spark-submit
    ejecutar_spark_submit = BashOperator(
        task_id='ejecutar_spark_submit',
        bash_command=(
            "docker exec docker-hadoop-pyspark-1 "
            "spark-submit --master local[*] "
            "--conf spark.hadoop.fs.defaultFS=hdfs://namenode:9000 "
            "--conf spark.hadoop.fs.hdfs.impl=org.apache.hadoop.hdfs.DistributedFileSystem "
            "--conf spark.hadoop.fs.file.impl=org.apache.hadoop.fs.LocalFileSystem "
            "--conf spark.jars.packages=org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2 "
            "/home/jovyan/caso_practico/process_data.py"
        )
    )

    # Tarea para copiar los archivos de HDFS a local
    copiar_archivos = BashOperator(
        task_id='copiar_archivos',
        bash_command='docker exec namenode bash -c "rm -rf /tmp/discards && hdfs dfs -get /data/output/discards/person /tmp/discards"'
    )

    descargar_archivos = BashOperator(
        task_id='descargar_archivos',
        bash_command='docker cp namenode:/tmp/discards /opt/airflow/dags/'
    )

    # Tarea para apagar los servicios al final
    apagar_servicios = BashOperator(
        task_id='apagar_servicios',
        bash_command=(
            "docker stop docker-hadoop-zookeeper-1 "
            "7c7b193e1f47_broker "
            "docker-hadoop-spark-1 "
            "docker-hadoop-spark-worker-1 "
            "nodemanager datanode resourcemanager namenode historyserver docker-hadoop-pyspark-1"
        )
    )

    # Definir las dependencias en orden
    # Parte 1
    levantar_spark >> levantar_worker >> levantar_zookeeper >> esperar_zookeeper >> levantar_broker
    # Parte 2
    levantar_resourcemanager >> levantar_datanode >> levantar_nodemanager >> levantar_namenode >> levantar_historyserver >> levantar_pyspark

    # Dependencias condicionales hacia ejecutar_spark_submit
    [levantar_broker, levantar_pyspark]  >> copiar_archivos_node >> copiar_archivos_hdfs >> copiar_archivos_hdfs_person >> ejecutar_spark_submit >> copiar_archivos >> descargar_archivos >> apagar_servicios
