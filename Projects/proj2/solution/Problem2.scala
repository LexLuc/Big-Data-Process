package comp9313.proj2

import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object Problem2 {
  def main(args: Array[String]) = {
    
    // Set configurations:
    val conf = new SparkConf().setAppName("k-cycle").setMaster("local")
    val sc = new SparkContext(conf)
    
    // Read Arguments:
    val filePath = args(0)
    val k = args(1).toInt
    
    // Read & Create graph:
    val graphRDD = sc.textFile(filePath)   
    val edges = graphRDD.map(line => line.split(" ")).map {
      line=> Edge(line(1).toLong, line(2).toLong, 0)
    }
    val graph = Graph.fromEdges[List[List[VertexId]], Int](edges, List())
	  //graph.triplets.collect().foreach(println)
    
    // Initialize graph:
    val initGraph = graph.mapVertices((id, _) => List[List[VertexId]]())
    
    // Pregel API call:
    val k_cycle = initGraph.pregel(List[List[VertexId]](), k)(
      // Vertex Program:
      (id, ns, newns) => {
        if (ns.isEmpty) List(List(id))
        else newns.map {
          path => List(id):::path
        }
      },
      
      // Send Message:
      triplet => {  
        Iterator((triplet.dstId, triplet.srcAttr))
      },
      
      // Merge Message
      (a, b) => a:::b 
    )
//    k_cycle.vertices.collect().foreach(println)
    
    // Filter cycles: same beginning and ending && only one duplicates:
    val cycles = k_cycle.vertices.flatMap {
      pair => Set(pair._2: _*)
    }.filter {
      path => path(0) == path(k) && Set(path: _*).size == path.size-1
    }
//    cycles.collect().foreach(println)
    // k starting point should have k identical cycles: 
    println(cycles.collect().size/k)
  }
}