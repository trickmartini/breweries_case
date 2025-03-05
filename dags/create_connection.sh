curl -X POST "airflow-webserver:8080/api/v1/connections" \
  --user "airflow:airflow" \
  -H "Content-Type: application/json" \
  -d '{
    "connection_id": "spark-conn",
    "conn_type": "spark",
    "description": "spark connection",
    "host": "spark://spark-master",
    "port": 7077,
    "extra": "{\"deploy_mode\": \"client\", \"spark_binary\": \"spark-submit\"}"
}'