### Let's name the script as testEMRtoS3Conn_pyWrapper.sh
#!/bin/sh

set -e
  
echo "calling spark script"

export HADOOP_USER_NAME=hdfs

# spark-submit  --driver-memory 2g --executor-memory 2g --executor-cores 2 --num-executors 2 --deploy-mode spark.py
/Users/aminu/spark/spark-3.5.0-bin-hadoop3/bin/spark-submit   /Users/aminu/airflow/dags/spark.py
