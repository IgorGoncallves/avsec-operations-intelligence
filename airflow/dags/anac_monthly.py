from datetime import datetime

from airflow.sdk import dag, task

from src.extract.vra import ExtratorVRA
from src.extract.rima import ExtratorRIMA
from src.extract.estatisticos import ExtratorEstatisticos


@dag(
    dag_id="anac_monthly",
    schedule="0 6 5 * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["anac", "monthly", "avsec"],
)
def anac_monthly():

    @task
    def extrair_vra():
        extrator = ExtratorVRA()
        dataframe = extrator.extrair()

        return {
            "fonte": "vra",
            "registros": dataframe.height,
        }

    @task
    def extrair_rima():
        extrator = ExtratorRIMA()
        dataframe = extrator.extrair()

        return {
            "fonte": "rima",
            "registros": dataframe.height,
        }

    @task
    def extrair_estatisticos():
        extrator = ExtratorEstatisticos()
        dataframe = extrator.extrair()

        return {
            "fonte": "estatisticos",
            "registros": dataframe.height,
        }

    @task
    def finalizar(vra, rima, estatisticos):
        print("Extrações mensais concluídas.")
        print(f"VRA: {vra}")
        print(f"RIMA: {rima}")
        print(f"Estatísticos: {estatisticos}")

    vra = extrair_vra()
    rima = extrair_rima()
    estatisticos = extrair_estatisticos()

    finalizar(
        vra=vra,
        rima=rima,
        estatisticos=estatisticos,
    )


anac_monthly()