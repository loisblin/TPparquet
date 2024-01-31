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


def display_table_schema(table):
    schema = table.schema
    print("Table Schema:")
    for field in schema:
        print(f"{field.name}: {field.type}")


def get_column(table, col_name):
    try:
        selected_table = table.select([col_name])
        column = selected_table.to_pandas()[col_name]
        column.name = col_name

        return column
    except KeyError:
        print(f"La colonne {col_name} n'existe pas dans la table.")
        return None
    
