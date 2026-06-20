import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, round, col

from pathlib import Path


os.environ["JAVA_HOME"] = r"C:\Program Files\Eclipse Adoptium\jdk-17.0.19.10-hotspot"
os.environ["HADOOP_HOME"] = r"C:\hadoop"

os.environ["PATH"] = (
    os.environ["JAVA_HOME"] + r"\bin;"
    + os.environ["HADOOP_HOME"] + r"\bin;"
    + os.environ["PATH"]
)


BASE_DIR = Path(__file__).resolve().parents[1]
CONSUMPTION_PATH = BASE_DIR / "data" / "consumption" / "yellow_taxi_trips"


def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("ifood-case-analysis")
        .getOrCreate()
    )


def main() -> None:
    spark = create_spark_session()

    df = spark.read.parquet(str(CONSUMPTION_PATH.resolve()).replace("\\", "/"))

    print("\nPergunta 1:")
    print("Qual a média de valor total recebido em um mês considerando todos os yellow táxis da frota?")

    answer_01 = (
        df.groupBy("trip_month")
        .agg(round(avg("total_amount"), 2).alias("avg_total_amount"))
        .orderBy("trip_month")
    )

    answer_01.show(truncate=False)

    print("\nPergunta 2:")
    print("Qual a média de passageiros por cada hora do dia no mês de maio?")

    answer_02 = (
        df.filter((col("trip_year") == 2023) & (col("trip_month") == 5))
        .groupBy("pickup_hour")
        .agg(round(avg("passenger_count"), 2).alias("avg_passenger_count"))
        .orderBy("pickup_hour")
    )

    answer_02.show(24, truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()