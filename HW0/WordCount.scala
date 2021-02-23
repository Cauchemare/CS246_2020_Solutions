package edu.stanford.cs246
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
object  WordCount{
  def main(args:Array[String]){
    val conf = new SparkConf().setAppName("SOME APP NAME").setMaster("local")
    val sc = new SparkContext(conf)
    val lines = sc.textFile("F:\\CS246\\1W.Introduction; MapReduce and Spark\\Assigment\\pg100.txt")
    val counts = lines.flatMap(_.toLowerCase().split("[^\\w]+")).filter(_.length()>1).map(w =>( w.charAt(0),1)).reduceByKey(_+_)
    counts.collect().foreach(println)
    sc.stop()

  }
}