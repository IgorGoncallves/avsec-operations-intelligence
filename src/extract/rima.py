from concurrent.futures import ThreadPoolExecutor, as_completed
import logging
from pathlib import Path

import httpx
import polars as pl


logger = logging.getLogger(__name__)


class ExtratorRIMA:
    def __init__(self):
        self.ano = 2025

        self.url_base = (
            "https://sistemas.anac.gov.br/dadosabertos/"
            "Operador%20Aeroportu%C3%A1rio/"
            "Dados%20de%20Movimenta%C3%A7%C3%A3o%20Aeroportu%C3%A1rias"
        )

        self.diretorio_raw = Path("/opt/airflow/data/raw/rima")
        self.diretorio_processado = Path("/opt/airflow/data/processed")

        self.diretorio_raw.mkdir(parents=True, exist_ok=True)
        self.diretorio_processado.mkdir(parents=True, exist_ok=True)

    def extrair(self):
        meses = range(1, 13)

        logger.info(
            "Iniciando extração paralela de %d períodos da RIMA",
            len(meses),
        )

        dataframes = []

        with ThreadPoolExecutor(max_workers=5) as executor:
            futuros = {
                executor.submit(
                    self._extrair_periodo,
                    mes,
                ): mes
                for mes in meses
            }

            for futuro in as_completed(futuros):
                mes = futuros[futuro]

                try:
                    dataframe = futuro.result()

                    if dataframe is not None:
                        dataframes.append(dataframe)

                except Exception:
                    logger.exception(
                        "Erro ao processar RIMA - %02d/%d",
                        mes,
                        self.ano,
                    )
                    raise

        if not dataframes:
            logger.warning(
                "Nenhum arquivo RIMA foi encontrado para processamento."
            )
            return pl.DataFrame()

        resultado = pl.concat(
            dataframes,
            how="diagonal_relaxed",
        )

        logger.info(
            "Extração RIMA concluída - total de %d registros",
            resultado.height,
        )

        self.salvar_parquet(resultado)

        return resultado

    def _extrair_periodo(self, mes):
        logger.info(
            "Iniciando extração RIMA - %02d/%d",
            mes,
            self.ano,
        )

        caminho_arquivo = self.baixar_arquivo(mes)

        if caminho_arquivo is None:
            logger.warning(
                "Arquivo RIMA não encontrado - %02d/%d",
                mes,
                self.ano,
            )
            return None

        dataframe = self.ler_arquivo(caminho_arquivo)

        logger.info(
            "Arquivo RIMA processado - %02d/%d - %d registros",
            mes,
            self.ano,
            dataframe.height,
        )

        return dataframe

    def baixar_arquivo(self, mes):
        nome_arquivo = self._montar_nome_arquivo(mes)

        url = f"{self.url_base}/{self.ano}/{nome_arquivo}"
        caminho_saida = self.diretorio_raw / nome_arquivo

        logger.info(
            "Baixando arquivo RIMA: %s",
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
            "Lendo arquivo RIMA: %s",
            caminho_arquivo,
        )

        return pl.read_csv(
            caminho_arquivo,
            separator=";",
            infer_schema=False,
            encoding="utf8-lossy",
            truncate_ragged_lines=True,
        )

    def salvar_parquet(self, dataframe):
        nome_arquivo = "rima.parquet"

        caminho_saida = self.diretorio_processado / nome_arquivo

        logger.info(
            "Salvando RIMA consolidado em Parquet: %s",
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

    def _montar_nome_arquivo(self, mes):
        return f"Movimentacoes_Aeroportuarias_{self.ano}{mes:02d}.csv"