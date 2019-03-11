package comp9313.proj2

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object Problem1 {
  def main (args: Array[String]) {
    // Read arguments:
    val inputFile = args(0)
    val outputFolder = args(1)
    
    // Set configurations:
    val conf = new SparkConf().setAppName("relativeFrequency").setMaster("local")
    val sc = new SparkContext(conf)
    
    // Read input file into RDD:
    val inputRDD = sc.textFile(inputFile).map ( _.split ("[\\s*$&#/\"'\\,.:;?!\\[\\](){}<>~\\-_]+") )
    
    // Clean data:
    val cleanedRDD = inputRDD.map {
      _.filter {
        seg => seg.length() > 0
      }.map {
        seg => seg.toLowerCase()
      }.filter {
        seg => seg.charAt(0) >= 'a' && seg.charAt(0) <= 'z'
      }
    }
    
    // Building co-occurrent Pairs:
    val cooccurPairs = cleanedRDD.map ( _.zipWithIndex ).flatMap {
      line => line.flatMap {
        termWithIndex1 => line.map {
          termWithIndex2 => (termWithIndex1._1, termWithIndex1._2, termWithIndex2._1, termWithIndex2._2)
        }
      }.filter {
        coocurWithIndex => coocurWithIndex._2 < coocurWithIndex._4
      }.map {
        coocurWithIndex => ((coocurWithIndex._1, coocurWithIndex._3), 1)
      }
    }.reduceByKey( _ + _ )
    //cooccurPairs.foreach(println)
    
    val relativeFreq = cooccurPairs.groupBy( _._1._1 ).map {
      mp => (mp._2, mp._2.foldLeft(0) ( _+_._2 ) )
    }.flatMap {
      bf => bf._1.map {
        pair => (pair._1, pair._2.toDouble / bf._2)
      }
    }.sortBy(_._1._2, true).sortBy(_._2, false).sortBy(_._1._1, true).map {
      pair => pair._1._1.toString() + " " + pair._1._2.toString() + " " + pair._2.toString()
    }
    //relativeFreq.foreach(println)
    relativeFreq.saveAsTextFile(outputFolder)
  }
}