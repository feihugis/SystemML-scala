package org.apache.sysml.example

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.sysml.api.mlcontext.{MLContext, MatrixMetadata}
import org.apache.sysml.api.mlcontext.ScriptFactory._

/**
  * Created by Fei Hu on 6/21/17.
  */
object RDD {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SystemML-RDD-Example").setMaster("local[4]")
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder().getOrCreate()
    val ml = new MLContext(sc)

    val rdd1 = sc.parallelize(Array("1.0,2.0", "3.0,4.0"))
    val rdd2 = sc.parallelize(Array("5.0,6.0", "7.0,8.0"))
    val sums = """
               s1 = sum(m1);
               s2 = sum(m2);
               if (s1 > s2) {
                 message = "s1 is greater"
                 } else if (s2 > s1) {
                   message = "s2 is greater"
                   } else {
                   message = "s1 and s2 are equal"
                   }
               """
    scala.tools.nsc.io.File("sums.dml").writeAll(sums)
    val sumScript = dmlFromFile("sums.dml").in(Map("m1"-> rdd1, "m2"-> rdd2)).out("s1", "s2", "message")
    val sumResults = ml.execute(sumScript)
    val s1 = sumResults.getDouble("s1")
    val s2 = sumResults.getDouble("s2")
    val message = sumResults.getString("message")
    print(message)

    print("Matrix with MetaData")

    val rdd1Metadata = new MatrixMetadata(2, 2)
    val rdd2Metadata = new MatrixMetadata(2, 2)
    val sumScriptwithMetaMatrix = dmlFromFile("sums.dml").in(Seq(("m1", rdd1, rdd1Metadata), ("m2", rdd2, rdd2Metadata))).out("s1", "s2", "message")
    val (firstSum, secondSum, sumMessage) = ml.execute(sumScriptwithMetaMatrix).getTuple[Double, Double, String]("s1", "s2", "message")
    print(sumMessage)
  }

}
