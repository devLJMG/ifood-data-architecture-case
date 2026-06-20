import os
from functools import reduce
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, year, month, hour, to_timestamp

from config import LANDING_DIR, CONSUMPTION_DIR, YEAR


os.environ["JAVA_HOME"] = r"C:\Program Files\Eclipse Adoptium\jdk-17.0.19.10-hotspot"
os.environ["PATH"] = os.environ["JAVA_HOME"] + r"\bin;" + os.environ["PATH"]


def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("ifood-data-architecture-case")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.sql.parquet.enableVectorizedReader", "false")
        .config("spark.sql.parquet.mergeSchema", "false")
        .getOrCreate()
    )


def get_parquet_files() -> list[str]:
    base_path = LANDING_DIR / f"year={YEAR}"
    files = sorted(base_path.glob("month=*/*.parquet"))

    if not files:
        raise FileNotFoundError(f"Nenhum arquivo Parquet encontrado em: {base_path}")

    print("Arquivos encontrados na landing zone:")
    for file in files:
        print(f"- {file}")

    return [str(file.resolve()).replace("\\", "/") for file in files]


def standardize_dataframe(df: DataFrame) -> DataFrame:
    return (
        df.select(
            col("VendorID").cast("int").alias("vendor_id"),
            col("passenger_count").cast("double").alias("passenger_count"),
            col("total_amount").cast("double").alias("total_amount"),
            to_timestamp(col("tpep_pickup_datetime")).alias("pickup_datetime"),
            to_timestamp(col("tpep_dropoff_datetime")).alias("dropoff_datetime"),
        )
        .filter(col("pickup_datetime").isNotNull())
        .filter(col("dropoff_datetime").isNotNull())
        .filter(col("passenger_count").isNotNull())
        .filter(col("total_amount").isNotNull())
        .filter(col("passenger_count") >= 0)
        .filter(col("total_amount") >= 0)
        .withColumn("trip_year", year(col("pickup_datetime")))
        .withColumn("trip_month", month(col("pickup_datetime")))
        .withColumn("pickup_hour", hour(col("pickup_datetime")))
        .filter(col("trip_year") == YEAR)
        .filter(col("trip_month").between(1, 5))
    )


def main() -> None:
    spark = create_spark_session()

    input_files = get_parquet_files()
    output_path = str(CONSUMPTION_DIR.resolve()).replace("\\", "/")

    standardized_dfs = []

    for file_path in input_files:
        print(f"Lendo e padronizando arquivo: {file_path}")

        raw_df = spark.read.parquet(file_path)
        standardized_df = standardize_dataframe(raw_df)

        standardized_dfs.append(standardized_df)

    consumption_df = reduce(
        lambda df1, df2: df1.unionByName(df2),
        standardized_dfs
    )

    print("Schema da camada de consumo:")
    consumption_df.printSchema()

    total_rows = consumption_df.count()
    print(f"Total de registros tratados: {total_rows}")

    print(f"Gravando camada de consumo em: {output_path}")

    (
        consumption_df
        .write
        .mode("overwrite")
        .partitionBy("trip_year", "trip_month")
        .parquet(output_path)
    )

    print("Camada de consumo criada com sucesso.")

    spark.stop()


if __name__ == "__main__":
    main()