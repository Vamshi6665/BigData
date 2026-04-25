from __future__ import annotations

import argparse
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


REQUIRED_FILES = [
    "orders.csv",
    "products.csv",
    "aisles.csv",
    "departments.csv",
    "order_products__prior.csv",
]


def assert_files_exist(data_dir: str) -> None:
    missing = [f for f in REQUIRED_FILES if not os.path.exists(os.path.join(data_dir, f))]
    if missing:
        raise FileNotFoundError(
            f"Missing required Instacart CSV(s) in {data_dir}: {missing}\n"
            f"Put Kaggle Instacart CSVs into: C:\\BigData\\InstacartProject\\data\\instacart\\"
        )


def main(args: argparse.Namespace) -> None:
    assert_files_exist(args.data_dir)

    spark = (
        SparkSession.builder.appName("InstacartETL")
        .master(args.spark_master)
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.shuffle.partitions", str(args.shuffle_partitions))
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # ---------- Load CSVs (select only needed cols to save memory) ----------
    orders = (
        spark.read.option("header", True).option("inferSchema", True)
        .csv(f"{args.data_dir}/orders.csv")
        .select(
            "order_id",
            "user_id",
            "order_dow",
            "order_hour_of_day",
            "days_since_prior_order",
        )
    )

    products = (
        spark.read.option("header", True).option("inferSchema", True)
        .csv(f"{args.data_dir}/products.csv")
        .select("product_id", "product_name", "aisle_id", "department_id")
    )

    aisles = (
        spark.read.option("header", True).option("inferSchema", True)
        .csv(f"{args.data_dir}/aisles.csv")
        .select("aisle_id", "aisle")
    )

    departments = (
        spark.read.option("header", True).option("inferSchema", True)
        .csv(f"{args.data_dir}/departments.csv")
        .select("department_id", "department")
    )

    op_prior = (
        spark.read.option("header", True).option("inferSchema", True)
        .csv(f"{args.data_dir}/order_products__prior.csv")
        .select("order_id", "product_id", "reordered")
    )

    # ---------- Customer features ----------
    customer_features = (
        orders.groupBy("user_id")
        .agg(
            F.countDistinct("order_id").alias("total_orders"),
            F.avg(F.coalesce(F.col("days_since_prior_order"), F.lit(0.0))).alias("avg_days_between_orders"),
            F.avg(F.col("order_hour_of_day")).alias("avg_order_hour"),
        )
    )

    # Basket size per order (avoid wide shuffles)
    basket_size = (
        op_prior.groupBy("order_id")
        .agg(
            F.count("*").alias("basket_size"),
            F.avg(F.col("reordered").cast("double")).alias("order_reorder_ratio"),
        )
    )

    customer_basket = (
        orders.select("order_id", "user_id")
        .join(basket_size, on="order_id", how="inner")
        .groupBy("user_id")
        .agg(
            F.avg("basket_size").alias("avg_basket_size"),
            F.avg("order_reorder_ratio").alias("reorder_ratio"),
        )
    )

    customer_features = customer_features.join(customer_basket, on="user_id", how="left")

    customer_segments = (
        customer_features.withColumn(
            "segment",
            F.when(F.col("total_orders") >= 20, F.lit("Loyal"))
            .when(F.col("total_orders").between(5, 19), F.lit("Occasional"))
            .otherwise(F.lit("Churn_Risk")),
        )
        .select("user_id", "total_orders", "avg_basket_size", "reorder_ratio", "avg_days_between_orders", "segment")
    )

    peak_order_times = (
        orders.groupBy("order_dow", "order_hour_of_day")
        .agg(F.count("*").alias("order_count"))
        .orderBy(F.desc("order_count"))
        .limit(200)  # keep this small; full table is huge and not necessary
    )

    # ---------- Product performance ----------
    product_perf_base = (
        op_prior.groupBy("product_id")
        .agg(
            F.count("*").alias("total_purchases"),
            F.avg(F.col("reordered").cast("double")).alias("reorder_rate"),
        )
    )

    # Broadcast small lookup tables to reduce shuffle
    product_performance = (
        product_perf_base
        .join(F.broadcast(products), on="product_id", how="left")
        .join(F.broadcast(aisles), on="aisle_id", how="left")
        .join(F.broadcast(departments), on="department_id", how="left")
        .select("product_id", "product_name", "department", "aisle", "total_purchases", "reorder_rate")
        .orderBy(F.desc("total_purchases"))
        .limit(5000)  # limit for first project to avoid huge writes
    )

    # ---------- Write to Warehouse Postgres ----------
    jdbc_props = {"user": args.db_user, "password": args.db_pass, "driver": "org.postgresql.Driver"}

    customer_features.write.mode("overwrite").jdbc(args.jdbc_url, "customer_features", properties=jdbc_props)
    customer_segments.write.mode("overwrite").jdbc(args.jdbc_url, "customer_segments", properties=jdbc_props)
    peak_order_times.write.mode("overwrite").jdbc(args.jdbc_url, "peak_order_times", properties=jdbc_props)
    product_performance.write.mode("overwrite").jdbc(args.jdbc_url, "product_performance", properties=jdbc_props)

    spark.stop()


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--spark-master", required=True)
    p.add_argument("--data-dir", required=True)
    p.add_argument("--jdbc-url", required=True)
    p.add_argument("--db-user", required=True)
    p.add_argument("--db-pass", required=True)
    p.add_argument("--shuffle-partitions", type=int, default=24)
    main(p.parse_args())