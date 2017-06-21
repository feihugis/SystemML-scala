package org.apache.sysml.example

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.sysml.api.mlcontext.{MLContext, MatrixMetadata}
import org.apache.sysml.api.mlcontext.ScriptFactory._

/**
  * Created by Fei Hu on 6/21/17.
  */
object Haberman {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SystemML-Haberman-Example").setMaster("local[4]")
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder().getOrCreate()
    val ml = new MLContext(sc)

    val habermanUrl = "http://archive.ics.uci.edu/ml/machine-learning-databases/haberman/haberman.data"
    val habermanList = scala.io.Source.fromURL(habermanUrl).mkString.split("\n")
    val habermanRDD = sc.parallelize(habermanList)
    val habermanMetadata = new MatrixMetadata(306, 4)
    val typesRDD = sc.parallelize(Array("1.0,1.0,1.0,2.0"))
    val typesMetadata = new MatrixMetadata(1, 4)
    val scriptUrl = "https://raw.githubusercontent.com/apache/systemml/master/scripts/algorithms/Univar-Stats.dml"
    val uni = dmlFromUrl(scriptUrl).in("A", habermanRDD, habermanMetadata).in("K", typesRDD, typesMetadata).in("$CONSOLE_OUTPUT", true)
    ml.execute(uni)

    println(uni.info())

  }

}
