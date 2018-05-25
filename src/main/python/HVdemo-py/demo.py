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


def runHorovodMPI(iter):
    taskCtx = TaskContext.get()
    # assume only one element in the iterator.
    # so I fix the file name for now
    dataFilePath = "/tmp/mpiInputData"
    for pdf in iter:
        save_pandas_df(pdf, dataFilePath)

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

        mpiProgPath = "/tmp/hvd_run_mnist_training.py"
        # NOTE: specify mpi working dir "/tmp".
        mpiCmd = "mpirun --wdir %s -np %d -H %s python %s %s" % (
            "/tmp",
            numProc,
            hostsListParam,
            #rankFilePath,
            mpiProgPath, dataFilePath
        )
        prc = Popen(mpiCmd, stdout=PIPE, stderr=PIPE, shell=True)
        stdout, stderr = prc.communicate()
        if prc.returncode != 0:
            raise Exception, "cmd:\n" + mpiCmd + "\ncmd ouput:\n" + stdout + "\ncmd err\n: " + stderr

        # FOR DEBUG
        # I still read data from stdout,
        # later I will change to read from local file

        taskCtx.barrier()
        return [stdout]
    else:
        taskCtx.barrier()
        return []

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("PY0 demo") \
        .getOrCreate()

    trainingDF = spark.read.format("libsvm") \
        .option("numFeatures", "784") \
        .load("/tmp/mnist-training-data.txt")

    @udf(returnType=ArrayType(DoubleType(), False))
    def vec2arr(vec):
        return vec.toArray().tolist()

    result = trainingDF.select(vec2arr(trainingDF.features)
                               .alias('featuresData'),
                               trainingDF.label) \
        .repartition(1) \
        .toPandasRdd() \
        .barrier() \
        .mapPartitions(runHorovodMPI) \
        .collect()

    print(result)
    spark.stop()



