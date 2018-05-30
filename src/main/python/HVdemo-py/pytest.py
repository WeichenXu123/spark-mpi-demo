import pyarrow as pa
import numpy as np
import pandas as pd
import time

"""
def genArrowTable(numRows, numDim, filePath):
    data = {
        'features': [np.random.rand(numDim) for _ in range(numRows)],
        'label': [np.random.rand() for _ in range(numRows)],
    }
    pdf = pd.DataFrame(data, columns=['features', 'label'])
    table = pa.Table.from_pandas(pdf)
    with pa.OSFile(filePath, "wb") as f:
        writer = pa.RecordBatchFileWriter(f, table.schema)
        writer.write_table(table)
        writer.close()
"""

if __name__ == "__main__":

    """
    test result:
    numRows: 100, to_pandas time 0.004313 s, from_pandas time 0.002873 s
    numRows: 1000, to_pandas time 0.001961 s, from_pandas time 0.019290 s
    numRows: 10000, to_pandas time 0.005849 s, from_pandas time 0.139782 s
    numRows: 100000, to_pandas time 0.039982 s, from_pandas time 1.946107 s
    """
    for numRows in [100, 1000, 10000, 100000]:
        numDim = 784
        data = {
            'features': [np.random.rand(numDim) for _ in range(numRows)],
            'label': [np.random.rand() for _ in range(numRows)],
        }
        pdf = pd.DataFrame(data, columns=['features', 'label'])
        table = pa.Table.from_pandas(pdf)
        time1 = time.time()
        pdf2 = table.to_pandas()
        time2 = time.time()
        table2 = pa.Table.from_pandas(pdf2)
        time3 = time.time()
        print("numRows: %d, to_pandas time %f s, from_pandas time %f s" % (
            numRows, time2 - time1, time3 - time2
        ))

