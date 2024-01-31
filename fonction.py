import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.compute as pc

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
    
def compute_statistics(table, col_name):
    col = table[col_name]
    count_result = pc.count(col)
    count_distinct_result = pc.count_distinct(col)
    sum_result = pc.sum(col)
    min_result = pc.min(col)
    max_result = pc.max(col)

    # Obtenir les valeurs des résultats
    count_value = count_result.as_py()
    count_distinct_value = count_distinct_result.as_py()
    sum_value = sum_result.as_py()
    min_value = min_result.as_py()
    max_value = max_result.as_py()

    # Afficher les résultats
    print(f"Count: {count_value}")
    print(f"Count Distinct: {count_distinct_value}")
    print(f"Sum: {sum_value}")
    print(f"Min: {min_value}")
    print(f"Max: {max_value}")