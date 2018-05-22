package xuwch.sparkmpi.demo1

import java.io.FileWriter

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row

import org.apache.spark.ml.linalg.Vector

object Util {

  /**
    * @param df containing two column: features: Vector, label: Double
    * @param localPath the local tmp dir.
    * @return dataframe containing two column: featureArrayFile: String, labelsFile: String
    */
  def dataframeToLocalFiles(df: DataFrame, localPath: String): DataFrame = {
    df.mapPartitions { iter: Iterator[Row] =>
      val featureArrayFile = localPath + "/featureArray"
      val labelsFile = localPath + "/labels"
      val fwFeatures = new FileWriter(featureArrayFile)
      val fwLabels = new FileWriter(labelsFile)
      try {
        iter.foreach { row: Row =>
          val features = row.getAs[Vector]("features")
          var i = 0
          while (i < features.size) {
            fwFeatures.write(features(i).toString)
            if (i < features.size - 1) fwFeatures.write(",")
            else fwFeatures.write("\n")
            i += 1
          }
          fwLabels.write(row.getAs[Double]("label").toString + "\n")
        }
      } finally {
        if (fwFeatures != null) fwFeatures.close()
        if (fwLabels != null) fwLabels.close()
      }
      Iterator(Row(featureArrayFile, labelsFile))
    }.toDF("featureArrayFile", "labelsFile")
  }
}
