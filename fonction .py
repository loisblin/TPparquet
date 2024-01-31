import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

def dataframe_to_table(df):
    table = pa.Table.from_pandas(df)
    return table

def table_to_dataframe(table):
    df = table.to_pandas()
    return df

def table_to_parquet(table, parquet_file):
    pq.write_table(table, parquet_file)

def parquet_to_table(parquet_file):
    table = pq.read_table(parquet_file)
    return table
