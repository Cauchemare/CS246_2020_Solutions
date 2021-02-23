package com.stanford
import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.immutable.Map
object PageRank {

  def main(args: Array[String]): Unit = {
    val  conf= new SparkConf().setAppName("PageRank").setMaster("local")
    val sc = new SparkContext(conf)
    val n=1000
    val n_iters=40
    val beta=0.8f
    var i:Int=0
    var r_old:Map[Int,Float]= Array.fill(n)(1f/n).zipWithIndex. map{
      x => (x._2+1,x._1) }.toMap

    val data =sc.textFile("F:\\CS246\\10W.Link Spam and Introduction to Social Networks-Homework\\Assigment\\q2\\data\\graph-full.txt")
      .distinct()
      .map(_.split("\\s") match {
      case Array(a,b) => (a.toInt,b.toInt)
    } )
      .groupByKey()
      .flatMap( x=> x._2.map(e=>(e,(x._1,1f/ x._2.toSeq.length))))
      .groupByKey() // Mij
    var r_new = Map[Int,Float]()
    for (i <- 1 to n_iters){
      var r_new= data.map(
        e => (e._1,(1-beta)/n + beta * e._2.map( x => r_old.apply(x._1) * x._2 ).sum )
      ) .collect().toMap
      r_old =r_new
    }

    val r_seq=  r_old.toSeq
    r_seq.sortBy(_._2).take(5).foreach(println)
    println()
    r_seq.sortBy(-_._2).take(5).foreach(println)
}

}
