package Assignment_28

import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.functions._

object Assignment_28 {
  def main(args:Array[String]): Unit = {
    //Let us create a spark session object
    //Create a case class globally to be used inside the main method
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("Spark Machine Learning")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

    //Set the log level as warning
    spark.sparkContext.setLogLevel("WARN")

    //Task 1
    val delayed_flights = spark.sparkContext.textFile("E:\\Prachi IMP\\Hadoop\\Day 28\\DelayedFlights.csv")
    val mapping = delayed_flights.map(x => ((x.split(",")(18), 1))).filter(x => x._1 != null).reduceByKey(_ + _).map(x => (x._2, x._1)).sortByKey(false).map(x => (x._2, x._1)).take(5)
    mapping.foreach(x=>println(x._1+", "+x._2))

    //Task 2
    val cancelled = delayed_flights.map(x => x.split(",")).filter(x => ((x(22).equals("1"))&&(x(23).equals("B")))).map(x => (x(17),1)).reduceByKey(_+_).map(x => (x._2,x._1)).sortByKey(false).map(x => (x._2,x._1)).take(1)
    cancelled.foreach(x=>println("Cancelled flights " + x._1+ ", " + x._2))

    //Task 3
    delayed_flights.map(x => x.split(",")).filter(x => ((x(24).equals("1")))).map(x => ((x(17)+","+x(18)),1)).reduceByKey(_+_).map(x => (x._2,x._1)).sortByKey(false).map(x => (x._2,x._1)).take(10).foreach(println)
  }
}
