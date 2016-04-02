package io.github.adrianulbona

/**
  * Created by adrian.bona on 24/03/16.
  */

import org.apache.log4j.{Level, LogManager}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object App {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Hello Spark")
    conf.setMaster("local[*]")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.set("spark.kryoserializer.buffer", "24")

    val sc = new SparkContext(conf)
    val rdd: RDD[Int] = sc.parallelize(List(1, 2, 3, 4, 5))
    rdd.map(x => x * x).collect.foreach(println)
    sc.stop()
  }
}