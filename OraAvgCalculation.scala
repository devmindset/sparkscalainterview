package com.deb.intv

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext

/**
 * You have long csv file contains number . 
 * Find average without using aggregate function. 
 * 
 * I/P -
 * 1,2,3
   4,5,6,7,8
   11,12,14,15
   16
   
   O/P -
   Average - 8
 * 
 */

object OraAvgCalculation {
    def main(args:Array[String]){
  val conf = new SparkConf().setMaster("local[*]")
  val ss = SparkSession.builder().config(conf).appName("OraAvgCalculation").getOrCreate()
  val sc = ss.sparkContext
  val data = sc.textFile("data/avgCal.txt")
  
  import ss.implicits._
  
    var totalSum = sc.longAccumulator 
    var totalCount = sc.longAccumulator("Total Number per Partiotion")
    
   val randRDD = data.flatMap { x => x.split(",") }.map{x => (x.toInt,1) }
   val rPartitioner = new org.apache.spark.RangePartitioner(3, randRDD)
   val rtt = randRDD.partitionBy(rPartitioner)  
    
    rtt.foreachPartition { partition => {
         val array = partition.toArray;
         val sum = array.toList.map(f=>f._1).sum
         val size =  array.size
         totalSum.add(sum.toLong)
         totalCount.add(size.toLong)
       }
    }
    
    val avg2 = totalSum.value/totalCount.value 
    println("Using Accumulator = "+avg2)

    //---------Without using any aggregate function
    val inputrdd = sc.parallelize(Seq(("maths", 50), ("maths", 60), ("english", 65)))
    val mapped = inputrdd.mapValues(mark => (mark, 1))
   // mapped.foreach(println)
    //(maths,(50,1)) (maths,(60,1)) (english,(65,1))
    val reduced = mapped.reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
    reduced.foreach(println)
    
    val average = reduced.map { x =>
                           val temp = x._2
                           val total = temp._1
                           val count = temp._2
                           (x._1, total / count)
                           }
    println(average.collect().mkString)
    
    
    // Using dataframe 
    val rdd1 = data.flatMap { x => x.split(",") }.map { x => (1,x.toInt) }

    val count = rdd1.count();
    val result = rdd1.reduceByKey(_+_).map(x=>x._2)
    val sum = result.first()
    println("result ==>"+result +"  count==>"+(sum/count))
    
    }
}