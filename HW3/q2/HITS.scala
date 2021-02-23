package com.stanford

import org.apache.spark.{SparkConf, SparkContext}

object HITS {
  def scale(i: Array[(Int,Float)]): Map[Int,Float]={
    val max_value  :Float = i.map(_._2).max
    return i.map(x =>  (x._1,x._2/ max_value)).toMap
  }
  def main(args: Array[String]): Unit = {
    val  conf= new SparkConf().setAppName("HITS").setMaster("local")
    val sc = new SparkContext(conf)
    val mu=1f
    val lamb= 1f
    val n_iters=40
    val n =1000
    var h= Array.fill(n)(1f).zipWithIndex. map{
      x => (x._2+1,x._1) }.toMap
    var a = Map[Int,Float]()
    val data =sc.textFile("F:\\CS246\\10W.Link Spam and Introduction to Social Networks-Homework\\Assigment\\q2\\data\\graph-full.txt")
      .distinct()
      .map(_.split("\\s") match {
        case Array(a,b) => (a.toInt,b.toInt)
      } )   // each element  Lij
    val L= data.groupByKey()
    val L_t= data.map(  x => (x._2,x._1) ).groupByKey()
  for (i<- 1 to  n_iters){
    a = scale(  L_t.map( x => (x._1, mu *  ( x._2.map( x=> h.apply(x)).sum )     ) ).collect() )
    h = scale( L.map( x => (x._1, lamb * ( x._2.map( x=> a.apply(x)).sum )     )    ).collect() )
  }
println("a vector result display...")
    a.toSeq.sortBy(_._2).take(5).foreach(println)
    println()
    a.toSeq.sortBy(-_._2).take(5).foreach(println)

println("h vector result display...")
    h.toSeq.sortBy(_._2).take(5).foreach(println)
    println()
    h.toSeq.sortBy(-_._2).take(5). foreach(println)











  }

}
