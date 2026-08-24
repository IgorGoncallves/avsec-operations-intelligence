from datetime import datetime

from airflow.sdk import dag, task

from src.extract.vra import ExtratorVRA


@dag(
    dag_id="anac_monthly",
    schedule="0 6 5 * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["anac", "monthly", "avsec"],
)
def anac_monthly():

    @task
    def extract_vra():
        extractor = ExtratorVRA()
        df = extractor.extrair()

        return {
            "source": "vra",
            "records": df.height,
        }

    @task
    def extract_rima():
        print("Extração RIMA ainda não implementada.")

        return {
            "source": "rima",
            "records": 0,
        }

    @task
    def extract_estatisticos():
        print("Extração de Dados Estatísticos ainda não implementada.")

        return {
            "source": "estatisticos",
            "records": 0,
        }

    @task
    def finish(vra, rima, estatisticos):
        print("Extrações mensais concluídas.")
        print(f"VRA: {vra}")
        print(f"RIMA: {rima}")
        print(f"Estatísticos: {estatisticos}")

    vra = extract_vra()
    rima = extract_rima()
    estatisticos = extract_estatisticos()

    finish(
        vra=vra,
        rima=rima,
        estatisticos=estatisticos,
    )


anac_monthly()