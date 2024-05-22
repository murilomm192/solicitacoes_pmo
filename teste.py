import polars as pl

bases = pl.scan_csv('bases/*.csv', separator=';')

print(bases.drop('').fetch())

