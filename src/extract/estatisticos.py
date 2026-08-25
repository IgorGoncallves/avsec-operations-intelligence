import logging
from pathlib import Path

import httpx
import polars as pl
import json


logger = logging.getLogger(__name__)


class ExtratorEstatisticos:
    def __init__(self):
        self.url = (
            "https://sistemas.anac.gov.br/dadosabertos/"
            "Voos%20e%20opera%C3%A7%C3%B5es%20a%C3%A9reas/"
            "Dados%20Estat%C3%ADsticos%20do%20Transporte%20A%C3%A9reo/"
            "Dados_Estatisticos_2021_a_2030.json"
        )

        self.diretorio_raw = Path(
            "/opt/airflow/data/raw/estatisticos"
        )

        self.diretorio_processado = Path(
            "/opt/airflow/data/processed"
        )

        self.diretorio_raw.mkdir(
            parents=True,
            exist_ok=True,
        )

        self.diretorio_processado.mkdir(
            parents=True,
            exist_ok=True,
        )

    def extrair(self):
        caminho_arquivo = self.baixar_arquivo()

        dataframe = self.ler_arquivo(
            caminho_arquivo
        )

        logger.info(
            "Dados Estatísticos carregados - %d registros",
            dataframe.height,
        )

        self.salvar_parquet(
            dataframe
        )

        return dataframe

    def baixar_arquivo(self):
        nome_arquivo = "Dados_Estatisticos_2021_a_2030.json"

        caminho_saida = (
            self.diretorio_raw
            / nome_arquivo
        )

        logger.info(
            "Baixando Dados Estatísticos da ANAC: %s",
            self.url,
        )

        with httpx.stream(
            "GET",
            self.url,
            timeout=300.0,
            follow_redirects=True,
        ) as resposta:

            resposta.raise_for_status()

            with open(caminho_saida, "wb") as arquivo:
                for bloco in resposta.iter_bytes():
                    arquivo.write(bloco)

        logger.info(
            "Arquivo salvo em: %s",
            caminho_saida,
        )

        return caminho_saida

    def ler_arquivo(self, caminho_arquivo):
        

        logger.info(
            "Lendo Dados Estatísticos: %s",
            caminho_arquivo,
        )

        with open(
            caminho_arquivo,
            "r",
            encoding="utf-8",
        ) as arquivo:
            conteudo = arquivo.read()

        decoder = json.JSONDecoder()

        posicao = 0
        dataframes = []

        while posicao < len(conteudo):

            while (
                posicao < len(conteudo)
                and conteudo[posicao].isspace()
            ):
                posicao += 1

            if posicao >= len(conteudo):
                break

            dados, nova_posicao = decoder.raw_decode(
                conteudo,
                posicao,
            )

            dataframe = pl.DataFrame(
                dados,
                infer_schema_length=None,
            )

            dataframes.append(
                dataframe
            )

            logger.info(
                "Bloco JSON processado - %d registros",
                dataframe.height,
            )

            posicao = nova_posicao

        resultado = pl.concat(
            dataframes,
            how="diagonal_relaxed",
        )

        logger.info(
            "Dados Estatísticos consolidados - %d registros",
            resultado.height,
        )

        return resultado

    def salvar_parquet(self, dataframe):
        caminho_saida = (
            self.diretorio_processado
            / "estatisticos.parquet"
        )

        logger.info(
            "Salvando Dados Estatísticos em Parquet: %s",
            caminho_saida,
        )

        dataframe.write_parquet(
            caminho_saida,
            compression="zstd",
        )

        logger.info(
            "Parquet salvo com sucesso - %d registros",
            dataframe.height,
        )

        return caminho_saida