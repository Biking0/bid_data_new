import numpy as np
import pandas as pd
import pyarrow as pa
df = pd.DataFrame({'one':['test','lisi','wangwu'], 'two': ['foo', 'bar', 'baz']})
table = pa.Table.from_pandas(df)
import pyarrow.parquet as pq
pq.write_table(table, 'example.parquet2')
pq.write_table(table, 'example.parquet2', compression='snappy')
table2 = pq.read_table('example.parquet2')
table2.to_pandas()