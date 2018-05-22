from pyspark.sql import SparkSession
from pyspark.taskcontext import TaskContext

import os
from subprocess import Popen, PIPE
import time
from pyspark.ml.wrapper import _jvm
from pspark.sql import DataFrame
from pyspark.sql.functions import udf, col
from pyspark.sql.types import StringType

def dataframeToLocalFiles(df, localPath):
    javaMethod = _jvm().xuwch.sparkmpi.demo1.Util.dataframeToLocalFile
    jdf = javaMethod(df._jdf, localPath)
    return DataFrame(jdf, df.sql_ctx)

def launchHorovodMPI(featureArrayFile, labelsFile):
    # later I will pass the two filepath args to the mpi cmd
    partitionId = TaskContext.get().partitionId()
    if partitionId == 0:
        # NOTE: MPI require every node process run in the same working directory,
        # so I add `cd /tmp/` so every process will run in `/tmp`
        # without this, the default directory may not exist on other nodes and cause error.
        mpiCmd = "cd /tmp/;mpirun -np 4 -H localhost:4 -bind-to none -map-by slot python hvd_run_mnist_training"
        prc = Popen(mpiCmd, stdout=PIPE, stderr=PIPE, shell=True)
        stdout, stderr = prc.communicate()
        if prc.returncode != 0:
        	raise Exception, "cmd:\n" + mpiCmd + "\ncmd ouput:\n" + stdout + "\ncmd err\n: " + stderr
        # I still read data from stdout,
        # later I will change to read from local file
        # but we need to address the issue that ensure mapping process-0 to the mpirun node.
        return stdout

if __name__ == "__main__":
	spark = SparkSession\
		.builder\
		.appName("PY0 demo")\
		.getOrCreate()

	trainingDF = spark.read.format("libsvm")\
	    .load("mnist-training-data.txt")

    mpi_udf = udf(launchHorovodMPI, StringType())
    result = trainingDF.select(mpi_udf(trainingDF["featureArrayFile"], trainingDF["labelsFile"])).collect()

	print(result)

	spark.stop()

