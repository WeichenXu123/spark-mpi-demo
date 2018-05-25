from pyspark.sql import SparkSession
from pyspark.taskcontext import TaskContext

import os
from subprocess import Popen, PIPE
import numpy as np
import time

from pyspark.ml.wrapper import _jvm
from pyspark.sql.functions import udf, col
from pyspark.sql.types import ArrayType, DoubleType, StringType
from pyspark.taskcontext import TaskContext

import pyarrow as pa

def save_pandas_df(pdf, filePath):
    context = pa.default_serialization_context()
    serialized_pdf = context.serialize(pdf)
    with pa.OSFile(filePath, 'wb') as f:
        f.write(serialized_pdf.to_buffer())


def save_pyarrow_table(table, filePath):
    with pa.OSFile(filePath, "wb") as f:
        writer = pa.RecordBatchFileWriter(f, table.schema)
        writer.write_table(table)
        writer.close()


def runHorovodMPI(iter):
    taskCtx = TaskContext.get()
    # assume only one element in the iterator.
    # so I fix the file name for now
    dataFilePath = "/tmp/mpiInputData"
    outputFilePath = "/tmp/mpiOutputResult"
    for pdf in iter:
        table = pa.Table.from_pandas(pdf)
        # later will directly get pyarrow table from RDD.
        save_pyarrow_table(table, dataFilePath)

    taskCtx.barrier()
    partitionID = taskCtx.partitionId()
    if partitionID == 0:
        hostsList = [i.split(":")[0] for i in taskCtx.hosts()]
        localHost = hostsList[0] # need a new API
        numProc = len(hostsList)

        # move local host to be first one.
        for i in range(0, numProc):
            if localHost == hostsList[i]:
                temp = hostsList[0]
                hostsList[0] = localHost
                hostsList[i] = temp
                break

        # do not generate host file, use simpler -H param instead.
        hostsListParam = ",".join(hostsList)

        # generate rank file
        rankFilePath = "/tmp/rankfile"
        with open(rankFilePath, "w") as rf:
            for i in range(0, numProc):
                rf.write("rank %d=%s slot=0-4" % (i, hostsList[i]))

        # NOTE:
        # Remember to change to real path
        mpiProgPath = "/tmp/hvd_run_mnist_training.py"

        # NOTE: specify mpi working dir "/tmp".
        # and note the horovod estimator will generate checkpoint dir
        # `mnist_convnet_model_${RANDOM_NUMBER}`
        # in the working dir.

        # NOTE:
        # Remember to add `sudo -u ubuntu` when run on databricks cluster
        # and change python path
        mpiCmd = "mpirun --wdir %s -np %d -H %s python %s %s %s" % (
            "/tmp",
            numProc,
            hostsListParam,
            #rankFilePath,
            mpiProgPath, dataFilePath, outputFilePath
        )
        prc = Popen(mpiCmd, stdout=PIPE, stderr=PIPE, shell=True)
        stdout, stderr = prc.communicate()
        if prc.returncode != 0:
            raise Exception, "cmd:\n" + mpiCmd + "\ncmd ouput:\n" + stdout + "\ncmd err\n: " + stderr

        with open(outputFilePath, "r") as f:
            outputContent = f.read()
        taskCtx.barrier()
        return [outputContent]
    else:
        taskCtx.barrier()
        return []

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("PY0 demo") \
        .getOrCreate()

    spark.conf.set("spark.sql.execution.arrow.enabled", "true")


    # NOTE:
    # Remember to change to the worker number
    np = 1

    # NOTE:
    # Remember to change to the real path
    df = spark.read.parquet("/tmp/mnist_parquet").repartition(np)

    @udf(returnType=ArrayType(DoubleType(), False))
    def vec2arr(vec):
        return vec.toArray().tolist()

    result = df.toPandasRdd() \
        .barrier() \
        .mapPartitions(runHorovodMPI) \
        .collect()

    print("result: " + str(result))
    spark.stop()

