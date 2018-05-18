package com.deb.intv

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf

/**
 * 
 * Word count without using flatMap
 */
object WordCount {
   val conf = new SparkConf().setMaster("local")
   val sc = SparkSession
    .builder
    .appName("ProductInfo")
    .config(conf)
    .getOrCreate().sparkContext
   def main(args:Array[String]){
     val data = sc.textFile("data/wordcount")
     var lineData = data.map { x => (x.split(" ").length)}.sum()
     println(lineData.toInt)
   }
}