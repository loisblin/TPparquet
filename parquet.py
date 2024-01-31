from fonction import *

table_academie = parquet_to_table("academie.parquet")
print(get_column(table_academie, "dep"))
print(compute_statistics())