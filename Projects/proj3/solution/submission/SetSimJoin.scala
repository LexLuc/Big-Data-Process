package comp9313.proj3

import scala.math.BigDecimal
import scala.collection.mutable.HashMap
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object SetSimJoin {
  
  def main(args: Array[String]) {
    
    // Read arguments:
    val inputFileR = args(0)
    val inputFileS = args(1)
    val outputPath = args(2)
    val simThresh = args(3).toDouble
    
    // Set configurations:
    val conf = new SparkConf().setAppName("relativeFrequency")//.setMaster("local")
    val sc = new SparkContext(conf)
    
    // Read files:
    val inputRDDR = sc.textFile(inputFileR)
    .map(_.split(" "))
    .map( line => {
      line(0)="-"+line(0); line       // mark docID from R set as negative
    })
    val inputRDDS = sc.textFile(inputFileS).map(_.split(" "))
    
    // Union two files together:
    val inputRDDU = inputRDDR.union(inputRDDS)
    
    // Word count: 
    val tokenFreqHashM = inputRDDU.flatMap {_.drop(1)}     // remove docID
      .map(token => (token, 1))
      .reduceByKey(_+_)         // map
      .collectAsMap             // reduce
    val tokenFreqBroadC = sc.broadcast(tokenFreqHashM)
    
    // Sort tokens:
    val tokenSorted = inputRDDU.map (
      line => (line(0), line.tail.sortBy {                 // sort each line
        token => ( tokenFreqBroadC.value(token), token )   // in non-increasing order and then in alphabetical order
      }))
      .map (line => (line._1.toInt, line._2))              // (docID, context)
      .sortByKey(true)
    // tokenSorted.saveAsTextFile(outputPath)
    
    // Filter prefix and generate intermediate result:
    val intermediateRes = tokenSorted.flatMap(line => {
      val len = line._2.length
      val n = len - Math.ceil(len * simThresh.toDouble).toInt + 1         // calculate length of prefix
      val pair = line._2
        .take(n)
        .map( tok => ( tok, (line._1, line._2) ))         // ( infrequentToken, (docID, context) )
      pair
    })
    
    // Seperate intermediate result by different files:
    val tokenContentPairR = intermediateRes.filter(line => line._2._1 <= 0)
    val tokenContentPairS = intermediateRes.filter(line => line._2._1 > 0)
    
    // Calculate Cartesian product:
    val joinedPairs = tokenContentPairR.join(tokenContentPairS)
    //joinedPairs.foreach {line => print(line._1, line._2._1._1, line._2._1._2.mkString("_"), line._2._2._1, line._2._2._2.mkString("_")); println }
    
    val finalRes = joinedPairs.map (
        pair => {
        val setR = pair._2._1._2.toSet
        val setS = pair._2._2._2.toSet
        val interSize = setR.intersect(setS).size
        val unionSize = setR.union(setS).size
        val jaccardSim = BigDecimal(interSize.toDouble / unionSize)
                          .setScale(6, BigDecimal.RoundingMode.HALF_UP).toDouble
        (-pair._2._1._1, pair._2._2._1, jaccardSim)
      })
      .filter(line => line._3 >= simThresh)
      .distinct()
      .sortBy(line => (line._1, line._2), true)
    
    // Reformat and save the result:
    finalRes.map(line => s"(${line._1},${line._2})\t${line._3}")
    .saveAsTextFile(outputPath)
  }
}