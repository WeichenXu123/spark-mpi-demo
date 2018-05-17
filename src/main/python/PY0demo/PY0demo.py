from pyspark.sql import SparkSession
from pyspark.taskcontext import TaskContext

import os
from subprocess import Popen, PIPE
import time

if __name__ == "__main__":
	spark = SparkSession\
		.builder\
		.appName("PY0 demo")\
		.getOrCreate()

	partDataPath = "/tmp/PY0demoPartData"
	hostFilePath = "/tmp/PY0demoHosts"
        outputDataPath = "/tmp/PY0demoOutput"
	mpiRunPath = "/usr/lib64/mpich/bin/mpirun"
	mpiProgPath = "/home/anders/mpisum/mpisum"
	numTasks = 3
	hosts = "hadoop0\nhadoop1\nhadoop2\n"

	rdd0 = spark.sparkContext.parallelize(range(0, 10), 3)
	
	def ff(iter):
		partitionId = TaskContext.get().partitionId()
		
		with open(partDataPath, "w") as fp:
			for i in iter:
				fp.write(str(i) + "\n")
		#we need barrier here
		#sleep 1s for now to ensure all nodes datafile generated.
		time.sleep(1)
		
		if partitionId == 0:
			with open(hostFilePath, "w") as fp:
				fp.write(hosts)
			# NOTE: MPI require every node process run in the same working directory,
			# so I add `cd /tmp/` so every process will run in `/tmp`
			# without this, the default directory may not exist on other nodes and cause error.	
			mpiCmd = "cd /tmp/;" + mpiRunPath + " -n " + str(numTasks) + " -f " +\
				hostFilePath + " " + mpiProgPath + " " +\
				partDataPath + " " + outputDataPath
			prc = Popen(mpiCmd, stdout=PIPE, stderr=PIPE, shell=True)
			stdout, stderr = prc.communicate()
			if prc.returncode != 0:
				raise Exception, "cmd:\n" + mpiCmd + "\ncmd ouput:\n" + stdout + "\ncmd err\n: " + stderr
			# I still read data from stdout,
			# later I will change to read from local file
			# but we need to address the issue that ensure mapping process-0 to the mpirun node.
			yield stdout


	result = rdd0.mapPartitions(ff).collect()
	print(result)

	spark.stop()

