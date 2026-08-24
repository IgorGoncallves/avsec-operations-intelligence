from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
import logging
from pathlib import Path

import httpx
import polars as pl


logger = logging.getLogger(__name__)


class ExtratorVRA:
    def __init__(self):
        self.url_base = "https://siros.anac.gov.br/siros/registros/diversos/vra"

        self.diretorio_raw = Path("/opt/airflow/data/raw/vra")
        self.diretorio_processado = Path("/opt/airflow/data/processed/vra")

        self.diretorio_raw.mkdir(parents=True, exist_ok=True)
        self.diretorio_processado.mkdir(parents=True, exist_ok=True)

    def extrair(self):
        data_atual = datetime.now()

        ano_atual = data_atual.year
        mes_atual = data_atual.month

        periodos = self._gerar_periodos(ano_atual, mes_atual)

        logger.info(
            "Iniciando extração paralela de %d períodos do VRA",
            len(periodos),
        )

        dataframes = []

        with ThreadPoolExecutor(max_workers=5) as executor:
            futuros = {
                executor.submit(
                    self._extrair_periodo,
                    ano,
                    mes,
                ): (ano, mes)
                for ano, mes in periodos
            }

            for futuro in as_completed(futuros):
                ano, mes = futuros[futuro]

                try:
                    dataframe = futuro.result()

                    if dataframe is not None:
                        dataframes.append(dataframe)

                except Exception:
                    logger.exception(
                        "Erro ao processar VRA - %02d/%d",
                        mes,
                        ano,
                    )
                    raise

        if not dataframes:
            logger.warning(
                "Nenhum arquivo VRA foi encontrado para processamento."
            )
            return pl.DataFrame()

        resultado = pl.concat(
            dataframes,
            how="diagonal_relaxed",
        )

        logger.info(
            "Extração VRA concluída - total de %d registros",
            resultado.height,
        )

        self.salvar_parquet(
            resultado,
            ano_atual,
            mes_atual,
        )

        return resultado

    def _extrair_periodo(self, ano, mes):
        logger.info(
            "Iniciando extração VRA - %02d/%d",
            mes,
            ano,
        )

        caminho_arquivo = self.baixar_arquivo(
            ano,
            mes,
        )

        if caminho_arquivo is None:
            logger.warning(
                "Arquivo VRA não encontrado - %02d/%d",
                mes,
                ano,
            )
            return None

        dataframe = self.ler_arquivo(
            caminho_arquivo
        )

        logger.info(
            "Arquivo VRA processado - %02d/%d - %d registros",
            mes,
            ano,
            dataframe.height,
        )

        return dataframe

    def baixar_arquivo(self, ano, mes):
        nome_arquivo = self._montar_nome_arquivo(
            ano,
            mes,
        )

        url = f"{self.url_base}/{ano}/{nome_arquivo}"
        caminho_saida = self.diretorio_raw / nome_arquivo

        logger.info(
            "Baixando arquivo VRA: %s",
            url,
        )

        resposta = httpx.get(
            url,
            timeout=60.0,
            follow_redirects=True,
        )

        if resposta.status_code == 404:
            return None

        resposta.raise_for_status()

        caminho_saida.write_bytes(
            resposta.content
        )

        logger.info(
            "Arquivo salvo em: %s",
            caminho_saida,
        )

        return caminho_saida

    def ler_arquivo(self, caminho_arquivo):
        logger.info(
            "Lendo arquivo VRA: %s",
            caminho_arquivo,
        )

        return pl.read_csv(
            caminho_arquivo,
            separator=";",
            infer_schema=False,
            encoding="utf8-lossy",
        )

    def salvar_parquet(self, dataframe, ano_atual, mes_atual):
        nome_arquivo = (
            f"vra_{ano_atual - 1}_{ano_atual}_{mes_atual:02d}.parquet"
        )

        caminho_saida = self.diretorio_processado / nome_arquivo

        logger.info(
            "Salvando VRA consolidado em Parquet: %s",
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

    def _gerar_periodos(self, ano_atual, mes_atual):
        periodos = []
        ano_anterior = ano_atual - 1

        for mes in range(1, 13):
            periodos.append(
                (ano_anterior, mes)
            )

        for mes in range(1, mes_atual + 1):
            periodos.append(
                (ano_atual, mes)
            )

        return periodos

    def _montar_nome_arquivo(self, ano, mes):
        return f"vra_{ano}_{mes:02d}.csv"