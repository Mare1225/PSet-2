import gc
import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds
from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.snowflake import Snowflake
from os import path
import fsspec  
from datetime import datetime, timezone 
import uuid
from mage_ai.data_preparation.shared.secrets import get_secret_value


if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


@data_exporter
def export_data_to_snowflake(df_yellow: pd.DataFrame, **kwargs) -> None:
    
    database   = get_secret_value('snowflake_db')
    schema     = get_secret_value('snowflake_schema')
    table_name_yellow = get_secret_value('snowflake_yellow_table')
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'
    batch_size = 1_000_000

    # ←— Este filesystem permite a Arrow leer https:// con range requests
    http_fs = pa.fs.PyFileSystem(pa.fs.FSSpecHandler(fsspec.filesystem('https')))

    with Snowflake.with_config(ConfigFileLoader(config_path, config_profile)) as loader:
        for _, row in df_yellow.iterrows():
            url = row['url']
            y, m = int(row['year']), int(row['month'])
            try:
                dataset = ds.dataset([url], filesystem=http_fs, format='parquet')
                scanner = dataset.scanner(batch_size=batch_size)

                total_rows = 0
                batches = 0
                for rb in scanner.to_batches():
                    df_chunk = rb.to_pandas(types_mapper=pd.ArrowDtype)
                    #Metadatos
                    df_chunk["Run_id"] = str(uuid.uuid4()) 
                    df_chunk["Año"] = y
                    df_chunk["Lote/Month"] = m
                    df_chunk["Ingest_ts"] = datetime.now(timezone.utc).isoformat() 
                    df_chunk["Tamaño"] = df_chunk.memory_usage(deep=True).sum() 
                    df_chunk["Fuente"]=url
                    loader.export(
                        df_chunk,
                        table_name_yellow,
                        database,
                        schema,
                        if_exists='append',  
                    )
                    batches +=1
                    total_rows += len(df_chunk)
                    del df_chunk
                    gc.collect()

                print(f'Se ha descargado mes {y}-{m:02d} que corresponden al batch{batches} -> {total_rows} rows"')

            except Exception as e:
                print(f"Detalle del error: {e}")
                print(f"No existe disponibilidad de Parquet")
                print(f'Fallo en la descarga {y}-{m:02d} que corresponden al batch{batches}')
        