from pathlib import Path

import polars as pl


class VraTransformer:

    AEROPORTO_BRASILIA = "SBBR"

    def __init__(
        self,
        input_dir: str | Path = "/opt/airflow/data/processed",
        output_dir: str | Path = "/opt/airflow/data/processed",
    ):
        self.input_dir = Path(input_dir)
        self.output_dir = Path(output_dir)

        self.output_dir.mkdir(
            parents=True,
            exist_ok=True,
        )

    def carregar(self) -> pl.LazyFrame:
        arquivos = sorted(
            self.input_dir.glob("*vra.parquet")
        )

        if not arquivos:
            raise FileNotFoundError(
                f"Nenhum parquet encontrado em {self.input_dir}"
            )

        return pl.scan_parquet(
            [str(arquivo) for arquivo in arquivos]
        )

    def filtrar_brasilia(
        self,
        df: pl.LazyFrame,
    ) -> pl.LazyFrame:

        return df.filter(
            (
                pl.col("Sigla ICAO Aeroporto Origem")
                == self.AEROPORTO_BRASILIA
            )
            |
            (
                pl.col("Sigla ICAO Aeroporto Destino")
                == self.AEROPORTO_BRASILIA
            )
        )

    def transformar_datas(
        self,
        df: pl.LazyFrame,
    ) -> pl.LazyFrame:

        colunas = [
            "Partida Prevista",
            "Partida Real",
            "Chegada Prevista",
            "Chegada Real",
        ]

        return df.with_columns(
            [
                pl.col(coluna)
                .str.strptime(
                    pl.Datetime,
                    format="%d/%m/%Y %H:%M",
                    strict=False,
                )
                .alias(coluna)
                for coluna in colunas
            ]
        )

    def criar_sentido(
        self,
        df: pl.LazyFrame,
    ) -> pl.LazyFrame:

        origem_brasilia = (
            pl.col("Sigla ICAO Aeroporto Origem")
            == self.AEROPORTO_BRASILIA
        )

        return df.with_columns(
            pl.when(origem_brasilia)
            .then(pl.lit("SAIDA"))
            .otherwise(pl.lit("ENTRADA"))
            .alias("sentido"),

            pl.when(origem_brasilia)
            .then(
                pl.col("Sigla ICAO Aeroporto Destino")
            )
            .otherwise(
                pl.col("Sigla ICAO Aeroporto Origem")
            )
            .alias("aeroporto_contraparte"),
        )

    def criar_rota(
        self,
        df: pl.LazyFrame,
    ) -> pl.LazyFrame:

        return df.with_columns(
            pl.concat_str(
                [
                    pl.col("Sigla ICAO Aeroporto Origem"),
                    pl.col("Sigla ICAO Aeroporto Destino"),
                ],
                separator="-",
            ).alias("rota")
        )

    def criar_atrasos(
        self,
        df: pl.LazyFrame,
    ) -> pl.LazyFrame:

        return df.with_columns(
            (
                pl.col("Partida Real")
                - pl.col("Partida Prevista")
            )
            .dt.total_minutes()
            .alias("atraso_partida_min"),

            (
                pl.col("Chegada Real")
                - pl.col("Chegada Prevista")
            )
            .dt.total_minutes()
            .alias("atraso_chegada_min"),
        )

    def criar_flags(
        self,
        df: pl.LazyFrame,
    ) -> pl.LazyFrame:

        return df.with_columns(
            (
                pl.col("Situação Voo") == "CANCELADO"
            ).alias("cancelado"),

            (
                pl.col("atraso_partida_min") > 15
            ).alias("partida_atrasada"),

            (
                pl.col("atraso_chegada_min") > 15
            ).alias("chegada_atrasada"),
        )

    def criar_dimensoes_tempo(
        self,
        df: pl.LazyFrame,
    ) -> pl.LazyFrame:

        return df.with_columns(
            pl.col("Partida Prevista")
            .dt.date()
            .alias("data_voo"),

            pl.col("Partida Prevista")
            .dt.year()
            .alias("ano"),

            pl.col("Partida Prevista")
            .dt.month()
            .alias("mes"),

            pl.col("Partida Prevista")
            .dt.day()
            .alias("dia"),

            pl.col("Partida Prevista")
            .dt.hour()
            .alias("hora_partida_prevista"),
        )

    def selecionar_colunas(
        self,
        df: pl.LazyFrame,
    ) -> pl.LazyFrame:

        return df.select(
            [
                "Sigla ICAO Empresa Aérea",
                "Empresa Aérea",
                "Número Voo",
                "Código DI",
                "Código Tipo Linha",
                "Modelo Equipamento",
                "Número de Assentos",

                "Sigla ICAO Aeroporto Origem",
                "Descrição Aeroporto Origem",

                "Sigla ICAO Aeroporto Destino",
                "Descrição Aeroporto Destino",

                "Partida Prevista",
                "Partida Real",
                "Chegada Prevista",
                "Chegada Real",

                "Situação Voo",
                "Situação Partida",
                "Situação Chegada",

                "Codeshare",

                "sentido",
                "aeroporto_contraparte",
                "rota",

                "atraso_partida_min",
                "atraso_chegada_min",

                "cancelado",
                "partida_atrasada",
                "chegada_atrasada",

                "data_voo",
                "ano",
                "mes",
                "dia",
                "hora_partida_prevista",
            ]
        )

    def transformar(self) -> pl.LazyFrame:

        df = self.carregar()

        df = self.filtrar_brasilia(df)
        df = self.transformar_datas(df)
        df = self.criar_sentido(df)
        df = self.criar_rota(df)
        df = self.criar_atrasos(df)
        df = self.criar_flags(df)
        df = self.criar_dimensoes_tempo(df)
        df = self.selecionar_colunas(df)

        return df

    def salvar(
        self,
        df: pl.LazyFrame,
    ) -> Path:

        destino = (
            self.output_dir
            / "vra_sbbr.parquet"
        )

        df.sink_parquet(destino)

        return destino

    def run(self) -> Path:

        df = self.transformar()

        destino = self.salvar(df)

        print(
            "[VRA] Transformação concluída."
        )

        print(
            f"[VRA] Arquivo salvo em: {destino}"
        )

        return destino