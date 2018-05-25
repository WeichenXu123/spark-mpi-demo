import pandas as pd
import pyarrow as pa


def save_pandas_df(pdf, filePath):
    context = pa.default_serialization_context()
    serialized_pdf = context.serialize(pdf)
    with pa.OSFile(filePath, 'wb') as f:
        f.write(serialized_pdf)


