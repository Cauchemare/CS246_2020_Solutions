package com.stanford.cs246
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.math.{abs, pow, sqrt}



object KMeans {
  def distaceEuclidien(p1: Array[Float], p2: Array[Float]): Float = {
    ( p1 zip p2  map(r => pow((r._1 - r._2), 2)) sum )toFloat
  }

  def distanceManhattan(p1: Array[Float], p2: Array[Float]): Float = {
    p1 zip p2 map (r => abs(r._1 - r._2)) sum
  }

  def findClosest(centroids: Array[Array[Float]], point: Array[Float], metrics: (Array[Float], Array[Float]) => Float = distaceEuclidien):
  (Array[Float], Int, Float) = {
    //point find closest centroid
    //Return:Tuple3 ( point:Array[String],closest_centroid_label:Int,closest_distance:Float)
    var i = 0
    centroids map (c => {
      i += 1
      (point, i, metrics(c, point))
    })  minBy(_._3)
}
  def subMain( texts: RDD[Array[Float]],sc:SparkContext,metrics: (Array[Float], Array[Float]) => Float,centroidsPath:String, max_iter:Int= 10)
  :Array[Double]={
    var centroids= sc.textFile(centroidsPath).map(_.split("\\s").map(_.toFloat )).collect()
    val N_features=centroids(0).length

    var errList = Array[Double]()
    for (i <- 1 to max_iter){
      val interResult =  texts.map(r => findClosest(centroids,r,metrics))
      centroids= interResult.map{ case (a,b,c) => (b,a)}.aggregateByKey((Array.fill(N_features)(0f),0) )(
        (u,c) =>( u._1 zip c map( r => r._1 + r._2),u._2+1 ),
        (a,b) => (a._1 zip b._1 map(r => r._1 + r._2),a._2 + b._2))
        .sortBy(_._1).map(r=> r._2._1.map(_/r._2._2)).collect()
      errList= errList.+:(interResult.map( _._3).sum() )
    }

    return errList

  }
  def percentChange[T >: Float](x:Array[T]): Array[T]={
    x.slice(1,x.length) zip x map( e=> (e._1-e._2)/e._2)
  }

  def main(args: Array[String]): Unit = {
    val  conf= new SparkConf().setAppName("KMeans").setMaster("local")
    val sc = new SparkContext(conf)
    val texts = sc.textFile("").map(_.split("\\s").map(_.toFloat))
    val c1_euclidien= subMain(texts,sc,distaceEuclidien,"c1.txt",20)
    val  c1_e_percChange=percentChange(c1_euclidien)
    val c1_e_change= (c1_euclidien(10)- c1_euclidien(0))/c1_euclidien(0)

    val c2_euclidien = subMain(texts,sc,distanceManhattan,"c2.txt",20)
    val c2_e_perChange= percentChange(c2_euclidien)


    val c1_manhandis =subMain(texts,sc,distanceManhattan,"c1.txt",20)
    val  c1_manhandis_pc =percentChange(c1_manhandis)
    val c2_manhandis =subMain(texts,sc,distanceManhattan,"c2.txt",20)
    val c2_m_pc = percentChange(c2_manhandis)
  }

}
