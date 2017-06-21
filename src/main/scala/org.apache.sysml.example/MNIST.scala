package org.apache.sysml.example

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.sysml.api.mlcontext._
import org.apache.sysml.api.mlcontext.ScriptFactory._
import org.apache.sysml.scripts.nn.examples.Mnist_lenet




/**
  * Created by Fei Hu on 6/21/17.
  */
object MNIST {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SystemML-MNIST").setMaster("local[4]")
    val sc = new SparkContext(conf)
    val ml = new MLContext(sc)

    val clf = ml.nn.examples.Mnist_lenet
    //val clf = new Mnist_lenet
    val dummy = clf.generate_dummy_data
    val dummyVal = clf.generate_dummy_data
    val params = clf.train(dummy.X, dummy.Y, dummyVal.X, dummyVal.Y, dummy.C, dummy.Hin, dummy.Win, 1)
    val probs = clf.predict(dummy.X, dummy.C, dummy.Hin, dummy.Win, params.W1, params.b1, params.W2, params.b2, params.W3, params.b3, params.W4, params.b4)
    val perf = clf.eval(probs, dummy.Y)
  }

}
