from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="instacart_spark_etl",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["bigdata", "instacart", "spark"],
) as dag:

    run_spark_etl = BashOperator(
        task_id="run_spark_etl",
        bash_command=r"""
        docker exec spark-master bash -lc "
          mkdir -p /tmp/ivy && chmod -R 777 /tmp/ivy &&
          /opt/spark/bin/spark-submit \
            --master spark://spark-master:7077 \
            --conf spark.jars.ivy=/tmp/ivy \
            --conf spark.driver.memory=2g \
            --conf spark.executor.memory=2g \
            --conf spark.executor.cores=2 \
            --conf spark.sql.shuffle.partitions=24 \
            --packages org.postgresql:postgresql:42.7.3 \
            /opt/spark_jobs/jobs/instacart_etl.py \
            --spark-master spark://spark-master:7077 \
            --data-dir /opt/data/instacart \
            --jdbc-url jdbc:postgresql://warehouse:5432/instacart \
            --db-user instacart \
            --db-pass instacart
        "
        """,
    )

    validate_data = BashOperator(
        task_id="validate_data",
        bash_command=r"""
        docker exec warehouse psql -U instacart -d instacart -c "
        SELECT 
            (SELECT COUNT(*) FROM customer_features) AS customers,
            (SELECT COUNT(*) FROM customer_segments) AS segments,
            (SELECT COUNT(*) FROM peak_order_times) AS peaks,
            (SELECT COUNT(*) FROM product_performance) AS products;
        "
        """,
    )

    run_spark_etl >> validate_data