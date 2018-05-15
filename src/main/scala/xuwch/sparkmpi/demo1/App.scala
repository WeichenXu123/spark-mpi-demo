package xuwch.sparkmpi.demo1

import java.io.File

import org.apache.spark.{SparkContext, TaskContext}
import xuwch.util.{ShellUtil, Util}

/**
 * MPI installed in dir /usr/lib64/mpich/ on each cluster node.
 * Prepare a binary mpi program: /home/anders/mpipi/mpi_pi (on driver node)
 *
 * compile app into demo/spark-test.jar
 * running on cluster nodes:
 *       hadoop0 (driver/worker)
 *       hadoop1 (worker)
 *       hadoop2 (worker)
 * submit spark job:
 *  bin/spark-submit --master spark://hadoop0:7077\
 *                   --class xuwch.sparkmpi.demo1.App demo/spark-test.jar
 */

object App {

  def main(args: Array[String]) {
    val sc = new SparkContext()

    val mpiProgramPath = "/home/anders/mpipi/mpi_pi"
    sc.setLocalProperty("workers", "hadoop0,hadoop1,hadoop2")
    sc.setLocalProperty("mpiProgram", Util.base64Encode(Util.readBinaryFile(mpiProgramPath)))

    val rdd0 = sc.parallelize(1 to 3, 3) // rdd0 with 3 partitions
    val result = rdd0.mapPartitions { iter =>
      val partitionId = TaskContext.get().partitionId()
      if (partitionId == 0) {
        val workers = TaskContext.get().getLocalProperty("workers")
        val workerList = workers.trim().split(",").map(_.trim())
        val mpiProgram = Util.base64Decode(TaskContext.get().getLocalProperty("mpiProgram"))

        val mpiProgDir = Util.randomString("mpiProgDir")
        val fullMpiProgDir = "/tmp/" + mpiProgDir
        new File(fullMpiProgDir).mkdirs()
        val mpiProgFile = s"${fullMpiProgDir}/mpiProgram"
        Util.writeBinaryFile(mpiProgFile, mpiProgram)
        new File(mpiProgFile).setExecutable(true)
        Util.writeTextFile(s"${fullMpiProgDir}/hostfile", workerList.mkString("\n") + "\n")
        println(s"fullMpiProgDir: ${fullMpiProgDir}")

        val runMPIDir = s"/tmp/run${mpiProgDir}"
        for (hostName <- workerList) {
          val scpCmd = s"scp -r ${fullMpiProgDir} ${hostName}:${runMPIDir}"
          val retCode = ShellUtil.exec(scpCmd, ".", "/dev/null", "/dev/null")
          if (retCode != 0) throw new RuntimeException("setup mpi cluster fail.")
        }
        val runMPIProgCmd =
          s"/usr/lib64/mpich/bin/mpirun -n ${workerList.length} -f ./hostfile ./mpiProgram"
        val retCode = ShellUtil.exec(runMPIProgCmd, runMPIDir, "./stdout", "./stderr")
        if (retCode != 0) throw new RuntimeException("run mpi program fail. stderr: "
          + Util.readFileToString(s"${runMPIDir}/stderr"))
        val result = Util.readFileToString(s"${runMPIDir}/stdout")
        Iterator(result)
      } else {
        Iterator.empty
      }
    }.collect()
    println("result: " + result(0))

    sc.stop()
  }
}
