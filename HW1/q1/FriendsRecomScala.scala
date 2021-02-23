package edu.stanford.cs246

import java.util.Date

import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.immutable.Set

object FriendsRecomScala {
  def spanLine(line: String): List[((String, String), Int)] = {
    val notConnected = -999999
    val context = line.split("\\s")
    if (context.size > 1) {
      val user = context(0)
      val friends = context(1).split("\\,")

      val innerFriends = friends.map(friend => ((user, friend), notConnected)).toList
      val outerFriends = friends.combinations(2).flatMap(_ permutations).map(friend => ((friend(0), friend(1)), 1)).toList
      return innerFriends ++ outerFriends
    }

    else
      return List()
  }

  def main(args:Array[String]) {
    val conf = new SparkConf().setAppName("FriendsRecomScala").setMaster("local")
    val sc = new SparkContext(conf)
    val texts = sc.textFile(path = "F:\\CS246\\2W.Frequent Itemsets Mining\\Assigment\\q1\\data\\soc-LiveJournal1Adj.txt")

    val result =texts.flatMap(spanLine)
        .reduceByKey( _+_)
        .filter(_._2>0)
        .map(r => ( r._1._1 ,(r._1._2,r._2)  ))
        .groupByKey()
        .map( r => r._1+ "\t"+ r._2.toSeq.sortBy(e =>( - e._2,e._1.toInt) ).map(_._1).take(10).mkString(","))
        .saveAsTextFile("friendsRecomResult.txt" )
    sc.stop()

  }
}
