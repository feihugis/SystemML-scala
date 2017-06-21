package org.apache.sysml.example

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.sysml.api.mlcontext.{MLContext, MatrixMetadata}
import org.apache.spark.sql._
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}
import org.apache.sysml.api.mlcontext.ScriptFactory._

import scala.util.Random

/**
  * Created by Fei Hu on 6/21/17.
  */
object DataFrame {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SystemML-MNIST").setMaster("local[4]")
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder().getOrCreate()
    val ml = new MLContext(sc)

    val numRows = 10000
    val numCols = 100
    val data = sc.parallelize(0 to numRows-1).map { _ => Row.fromSeq(Seq.fill(numCols)(Random.nextDouble)) }
    val schema = StructType((0 to numCols-1).map { i => StructField("C" + i, DoubleType, true) } )
    val df = spark.createDataFrame(data, schema)

    val minMaxMean = """
                     minOut = min(Xin)
                     maxOut = max(Xin)
                     meanOut = mean(Xin)
                     """
    val mm = new MatrixMetadata(numRows, numCols)
    val minMaxMeanScript = dml(minMaxMean).in("Xin", df, mm).out("minOut", "maxOut", "meanOut")
    val (min, max, mean) = ml.execute(minMaxMeanScript).getTuple[Double, Double, Double]("minOut", "maxOut", "meanOut")
    print(min + " , " + max + " , " + mean)

  }

}
