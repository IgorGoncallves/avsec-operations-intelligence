from datetime import datetime

from airflow.sdk import dag, task

from src.extract.vra import ExtratorVRA
from src.extract.rima import ExtratorRIMA
from src.extract.estatisticos import ExtratorEstatisticos
from src.transform.vra import VraTransformer


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
    def transformar_vra(extracao):
        print(f"Resultado da extração: {extracao}")

        transformer = VraTransformer()
        transformer.run()

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
    def finalizar(
        vra_transformada,
        rima,
        estatisticos,
    ):
        print("Pipeline mensal concluído.")

        print(
            f"VRA transformada: "
            f"{vra_transformada}"
        )

        print(
            f"RIMA: {rima}"
        )

        print(
            f"Estatísticos: "
            f"{estatisticos}"
        )

    vra = extrair_vra()

    vra_transformada = transformar_vra(
        vra
    )

    rima = extrair_rima()

    estatisticos = extrair_estatisticos()

    finalizar(
        vra_transformada=vra_transformada,
        rima=rima,
        estatisticos=estatisticos,
    )


anac_monthly()