package io.github.adrianulbona.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object HelloRDD {

  def main(args: Array[String]) {
    val spark = SparkSession
      .builder()
      .appName("Hello QTronic")
      .master("local[*]")
      .getOrCreate()

    val numbersRDD: RDD[Int] = spark.sparkContext.parallelize(List(1, 2, 3, 4, 5))
    val squaresRDD: RDD[Int] = numbersRDD.map(x => x * x)
    val squares: Array[Int] = squaresRDD.collect()
    squares.foreach(println)

    spark.stop()
  }
}