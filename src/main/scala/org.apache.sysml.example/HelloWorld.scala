package org.apache.sysml.example

/**
  * Created by Fei Hu on 6/21/17.
  */

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.sysml.api.mlcontext._
import org.apache.sysml.api.mlcontext.ScriptFactory._

object HelloWorld {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SystemML-HelloWorld").setMaster("local[4]")
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder()
      .getOrCreate()
    val ml = new MLContext(spark)

    val helloScript = dml("print('hello world')")
    ml.execute(helloScript)
  }


}
