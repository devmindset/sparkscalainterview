package com.deb.intv

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf

object ReduceVsReduceByKey {
  val conf = new SparkConf().setMaster("local[*]");
  val sc = SparkSession.builder().config(conf).appName("OracleSpark").getOrCreate().sparkContext;
  
  def main(args:Array[String]){
  
    val x = sc.parallelize(Array(("a", 1), ("b", 1), ("a", 1),
      ("a", 1), ("b", 1), ("b", 1),
      ("b", 1), ("b", 1)), 3)
    println("ReduceByKey - 1")
    val y = x.reduceByKey((accum, n) => (accum + n))
    y.collect.foreach(println)
    
    println("ReduceByKey - 2")
    val z = x.reduceByKey(_ + _)
    z.collect().foreach(println)
    
    println("ReduceByKey - 3")
    def sumFunc(accum:Int, n:Int) =  accum + n
    val w = x.reduceByKey(sumFunc)
    w.collect().foreach(println)
    
    //All output same - 
    //(a,3)
    //(b,5) 
    
    // reduce numbers 1 to 10 by adding them up
 val a = sc.parallelize(1 to 10, 2)
 val b = a.reduce((accum,n) => (accum + n)) 
//y: Int = 55

// shorter syntax
 val c = a.reduce(_ + _) 
//y: Int = 55

// same thing for multiplication
 val d = a.reduce(_ * _) 
  //y: Int = 3628800
    
    println("***********************")
        val p = sc.parallelize(Array(("a", 1), ("b", 1), ("a", 1),
      ("b", 1), ("a", 1), ("d", 1),
      ("y", 1), ("x", 1)), 3)
    println("ReduceByKey - 1")
    
     val q = p.reduceByKey(_ + _)
     q.collect().foreach(println)
     println("@@@@@@@@@@@@@@@@@@@@@@@@@@@")
     val r = q.map(item=>item.swap)
    // r.foreach(println)
     r.reduceByKey((x, y) => if (y > x) y 
                                    else  x).collect().foreach(println)
                                    
     
     val rdd = sc.parallelize(
    ("id1", 10, "v1") :: ("id2", 9, "v2") ::
    ("id2", 34, "v3") :: ("id1", 6, "v4") :: 
    ("id1", 12, "v5") :: ("id2", 2, "v6") :: Nil)

rdd
  .map{case (id, x, y) => (id, (x, y))}
  .groupByKey
  .mapValues(iter => iter.toList.sortBy(_._1))
  .sortByKey() // Optional if you want id1 before id2
  
  /*
   id1, 10, v1
id2, 9, v2
id2, 34, v3
id1, 6, v4
id1, 12, v5
id2, 2, v6
and I want output

id1; 6,v4 | 10,v1 | 12,v5
id2; 2,v6 | 9,v2 | 34,v3
   */
  
     
  }
}