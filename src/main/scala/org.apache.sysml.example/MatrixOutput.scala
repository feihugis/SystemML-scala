package org.apache.sysml.example

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.sysml.api.mlcontext._
import org.apache.sysml.api.mlcontext.ScriptFactory._

/**
  * Created by Fei Hu on 6/21/17.
  */
object MatrixOutput {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SystemML-Matrix-Example").setMaster("local[4]")
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder().getOrCreate()
    val ml = new MLContext(sc)

    val s =
      """
      m = matrix("11 22 33 44", rows=2, cols=2)
      n = sum(m)
      """
    val scr = dml(s).out("m", "n");
    val res = ml.execute(scr)
    val (x, y) = res.getTuple[Matrix, Double]("m", "n")
    x.toRDDStringIJV.collect.foreach(println)
    x.toRDDStringCSV.collect.foreach(println)
    x.toDF.collect.foreach(println)
    x.to2DDoubleArray

  }

}
