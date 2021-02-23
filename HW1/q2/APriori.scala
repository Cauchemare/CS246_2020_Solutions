import java.{io, util}

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.immutable.{HashSet, Set}
import scala.collection.Seq

object  Test{
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("FriendsRecomScala").setMaster("local")
    val sc = new SparkContext(conf)

    val  pairs=   sc.parallelize(Array(("a", 3), ("a", 1), ("b", 7), ("a", 5)))
    val sets = pairs.keys
    val s1= 1
    println(s1/3)

  }
}

object APriori {
  val s=100
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("FriendsRecomScala").setMaster("local")
    val sc = new SparkContext(conf)
    val texts = sc.textFile(path = "F:\\CS246\\2W.Frequent Itemsets Mining\\Assigment\\q2\\data\\browsing.txt")
      .map(_.split("\\s"))
      .cache()
    val resultOne=
      texts.flatMap(_.toList)
        .map(x =>(x,1))

        .reduceByKey(_+_)
        .filter( _._2 >= s ).collectAsMap()


    val resultOneKeys=resultOne.keys.toSet
    val textsFilteredOne = texts.map( _.toSet intersect  resultOneKeys )

    val  resultTwo = textsFilteredOne.flatMap(_.toList.combinations(2))
      .map(x =>(x.toSet,1))
      .reduceByKey(_+_)
      .filter( _._2 >= s ).collectAsMap()

    val resultTwoKeys = resultTwo.keys.flatMap(_.toList).toSet


    val textsFilteredTwo =  texts.map( _.toSet intersect  resultTwoKeys )
    val resultTree=textsFilteredTwo.flatMap(_.toList.combinations(3))
      .map( x => (x.toSet,1))
      .reduceByKey(_+_)
      .filter(_._2 >=s).collectAsMap()
    //计算confidence score
    val  confTwo = resultTwo.flatMap( x =>
                                x._1.toList.permutations
                                  .map{
                                    case List(a,b) =>( a,b, x._2.toFloat/ resultOne(a) )
                                  })
    confTwo.toSeq.sortBy( x =>  ( - x._3,x._1 ) ).take(5).foreach {
      case  (a,b,conf) => println(a + "=>" +b ,conf)
    }
    val  confTree= resultTree.flatMap( x =>
      x._1.toList match {

          case List(a,b,c) =>List (
            (a, b ,c, x._2.toFloat/ resultTwo(Set(a,b)) ),
            (a,c,b, x._2.toFloat/ resultTwo(Set(a,c)) ),
            (b,c,a, x._2.toFloat/ resultTwo(Set(b,c)) )
          )
        }
    )
    confTree.toSeq.sortBy( x =>(- x._4,x._1,x._2) ).take(5).foreach{
      case (a,b,c,conf ) => println( a+","+b +"=>" +c,conf)
    }
  }
}
