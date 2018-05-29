from pyspark.sql import SparkSession
from pyspark.taskcontext import TaskContext


from subprocess import Popen, PIPE
import random

from pyspark.ml.wrapper import _jvm
from pyspark.sql.functions import udf, col
from pyspark.sql.types import ArrayType, DoubleType, StringType
from pyspark.taskcontext import TaskContext

import pyarrow as pa
import numpy as np
import pandas as pd

from pyspark.sql.functions import pandas_udf, PandasUDFType

from tensorflow.contrib import predictor


class TFModel(object):

    def __init__(self, modelDir):
        self.modelDir = modelDir

    def transform(self, df):

        _modelDir = self.modelDir

        """
        def infer(iter):
            for pdf in iter:
                features = np.reshape(
                    np.array(np.concatenate(pdf['features'].values),
                             dtype=np.float32), (-1, 784))
                outPdf = pdf.copy()
                predict_fn = predictor.from_saved_model(_modelDir)
                predictions = predict_fn({"x": features})
                classes = predictions['classes']
                outPdf['prediction'] = pd.Series(classes, index=outPdf.index)
                yield outPdf

        return df.toPandasRdd().mapPartitions(infer)
        """
        @pandas_udf('double', PandasUDFType.SCALAR)
        def infer(featureSeries):
            features = np.reshape(
                np.array(np.concatenate(featureSeries.values),
                         dtype=np.float32), (-1, 784))
            predict_fn = predictor.from_saved_model(_modelDir)
            predictions = predict_fn({"x": features})
            classes = predictions['classes']
            return pd.Series(classes)

        return df.withColumn('prediction', infer(df.features))

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("PY0 inference demo") \
        .getOrCreate()

    spark.conf.set("spark.sql.execution.arrow.enabled", "true")

    # Note:
    # Change to dbfs path
    model = TFModel("/tmp/mpiExportModel1/1527583818")

    df = spark.read.parquet("/tmp/mnist_parquet").repartition(3)
    result = model.transform(df).show(10)
    spark.stop()

