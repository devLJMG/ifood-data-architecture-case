import os
from pathlib import Path

import pandas as pd
import streamlit as st
import plotly.express as px
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, round, col, count


os.environ["JAVA_HOME"] = r"C:\Program Files\Eclipse Adoptium\jdk-17.0.19.10-hotspot"
os.environ["HADOOP_HOME"] = r"C:\hadoop"

os.environ["PATH"] = (
    os.environ["JAVA_HOME"] + r"\bin;"
    + os.environ["HADOOP_HOME"] + r"\bin;"
    + os.environ["PATH"]
)


BASE_DIR = Path(__file__).resolve().parents[1]
CONSUMPTION_PATH = BASE_DIR / "data" / "consumption" / "yellow_taxi_trips"


@st.cache_resource
def create_spark_session():
    return (
        SparkSession.builder
        .appName("ifood-case-dashboard")
        .config("spark.sql.parquet.enableVectorizedReader", "false")
        .getOrCreate()
    )


@st.cache_data
def load_summary_data():
    spark = create_spark_session()

    df = spark.read.parquet(str(CONSUMPTION_PATH.resolve()).replace("\\", "/"))

    total_records = df.count()

    monthly_avg = (
        df.groupBy("trip_month")
        .agg(round(avg("total_amount"), 2).alias("avg_total_amount"))
        .orderBy("trip_month")
        .toPandas()
    )

    hourly_may = (
        df.filter((col("trip_year") == 2023) & (col("trip_month") == 5))
        .groupBy("pickup_hour")
        .agg(round(avg("passenger_count"), 2).alias("avg_passenger_count"))
        .orderBy("pickup_hour")
        .toPandas()
    )

    monthly_count = (
        df.groupBy("trip_month")
        .agg(count("*").alias("trip_count"))
        .orderBy("trip_month")
        .toPandas()
    )

    sample_data = df.limit(100).toPandas()

    return total_records, monthly_avg, hourly_may, monthly_count, sample_data


def render_pipeline_view():
    st.subheader("Pipeline de dados")

    st.markdown(
        """
        ```text
        NYC TLC Parquet Files
                │
                ▼
        Landing Zone
        data/landing/yellow_taxi/year=2023/month=01..05
                │
                ▼
        PySpark Transformation
        - Leitura dos arquivos Parquet
        - Padronização de schema
        - Conversão de tipos
        - Filtros de qualidade
        - Criação de colunas analíticas
                │
                ▼
        Consumption Zone
        data/consumption/yellow_taxi_trips
                │
                ▼
        Analysis Layer
        - Média de total_amount por mês
        - Média de passenger_count por hora em maio
        ```
        """
    )

    st.info(
        "A solução foi desenhada em camadas para manter rastreabilidade, "
        "reprocessamento e separação entre dados brutos e dados prontos para consumo."
    )


def main():
    st.set_page_config(
        page_title="iFood Data Architecture Case",
        layout="wide"
    )

    st.title("iFood Data Architecture Case")
    st.caption("Yellow Taxi NYC — Janeiro a Maio de 2023")

    total_records, monthly_avg, hourly_may, monthly_count, sample_data = load_summary_data()

    tab_overview, tab_pipeline, tab_data, tab_analysis = st.tabs(
        [
            "Visão geral",
            "Processo",
            "Dados",
            "Análises"
        ]
    )

    with tab_overview:
        st.subheader("Resumo executivo")

        col1, col2, col3 = st.columns(3)

        col1.metric("Registros tratados", f"{total_records:,}".replace(",", "."))
        col2.metric("Período", "Jan–Mai/2023")
        col3.metric("Camada final", "Consumption")

        st.markdown(
            """
            Este dashboard apresenta a visão de consumo dos dados processados
            no case técnico de Data Architecture. A solução realiza ingestão,
            transformação com PySpark e disponibilização dos dados em uma camada
            analítica em Parquet.
            """
        )

        fig_count = px.bar(
            monthly_count,
            x="trip_month",
            y="trip_count",
            title="Volume de corridas por mês",
            labels={
                "trip_month": "Mês",
                "trip_count": "Quantidade de corridas"
            }
        )

        st.plotly_chart(fig_count, use_container_width=True)

    with tab_pipeline:
        render_pipeline_view()

    with tab_data:
        st.subheader("View da camada de consumo")

        st.markdown(
            """
            A camada de consumo mantém os dados tratados em granularidade de corrida,
            com as colunas obrigatórias solicitadas no case e colunas auxiliares para análise.
            """
        )

        st.dataframe(sample_data, use_container_width=True)

        st.markdown("### Schema lógico")

        schema_df = pd.DataFrame(
            [
                {"coluna": "vendor_id", "tipo": "integer", "descrição": "Identificador do fornecedor"},
                {"coluna": "passenger_count", "tipo": "double", "descrição": "Quantidade de passageiros"},
                {"coluna": "total_amount", "tipo": "double", "descrição": "Valor total da corrida"},
                {"coluna": "pickup_datetime", "tipo": "timestamp", "descrição": "Data e hora de início da corrida"},
                {"coluna": "dropoff_datetime", "tipo": "timestamp", "descrição": "Data e hora de fim da corrida"},
                {"coluna": "trip_year", "tipo": "integer", "descrição": "Ano da corrida"},
                {"coluna": "trip_month", "tipo": "integer", "descrição": "Mês da corrida"},
                {"coluna": "pickup_hour", "tipo": "integer", "descrição": "Hora de início da corrida"},
            ]
        )

        st.dataframe(schema_df, use_container_width=True)

    with tab_analysis:
        st.subheader("Pergunta 1 — Média de total_amount por mês")

        fig_monthly = px.line(
            monthly_avg,
            x="trip_month",
            y="avg_total_amount",
            markers=True,
            title="Média de total_amount por mês",
            labels={
                "trip_month": "Mês",
                "avg_total_amount": "Média de total_amount"
            }
        )

        st.plotly_chart(fig_monthly, use_container_width=True)
        st.dataframe(monthly_avg, use_container_width=True)

        st.subheader("Pergunta 2 — Média de passageiros por hora em maio")

        fig_hourly = px.bar(
            hourly_may,
            x="pickup_hour",
            y="avg_passenger_count",
            title="Média de passageiros por hora — Maio/2023",
            labels={
                "pickup_hour": "Hora do dia",
                "avg_passenger_count": "Média de passageiros"
            }
        )

        st.plotly_chart(fig_hourly, use_container_width=True)
        st.dataframe(hourly_may, use_container_width=True)


if __name__ == "__main__":
    main()