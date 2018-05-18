package com.deb.intv

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf

object ParallelExecution {
  val conf = new SparkConf().setMaster("local[*]")
    val sc = SparkSession.builder().appName("AverageRevenue").config(conf).getOrCreate().sparkContext
     sc.setLogLevel("ERROR")
     def main(args:Array[String]): Unit ={
     println("/*************Start of synchronous processing*************/")
     val rdd = sc.parallelize(List(32, 34, 2, 3, 4, 54, 3), 4)
     rdd.collect().map{ x =>println("Items in the lists:" + x)}
     val rddCount = sc.parallelize(List(434, 3, 2, 43, 45, 3, 2), 4)
     println("Number of items in the list" + rddCount.count())
     println("/****************End of synchronous processing***************/")
     println("/*************************************************************/")
     
     println("/*************Start of Asynchronous processing*************/")
     val rdd2 = sc.parallelize(List(32, 34, 2, 3, 4, 54, 3), 4)
     //rdd2.collectAsync().map{ x => println("Items in the list:"+x)}
    // rdd2.collectAsync().map { x => x }
    // val rddCount2 = sc.parallelize(List(434, 3, 2, 43, 45, 3, 2), 4)
    // rddCount2.countAsync().map{x =>println("Number of items in the list: "+x)}

     
     
  }
}