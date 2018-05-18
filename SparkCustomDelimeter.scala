package com.deb.intv

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SparkCustomDelimeter {
  val conf = new SparkConf().setMaster("local[*]")
  val ss = SparkSession.builder().config(conf).appName("SparkCustomDelimeter").getOrCreate();
  val sc = ss.sparkContext
  // Since | record delimited thats why u don't need to use \\| but in case of field level u have to use \\|
  sc.hadoopConfiguration.set("textinputformat.record.delimiter","|")
  
  def main(args:Array[String]){
    
    val dataRDD = sc.textFile("data/delimitedfile")
    println(dataRDD.count())
    val result  = dataRDD
    .map { x => x.split(",") }
    .map { x => (x(0),x(1),x(2)) }
    .map{x=>x._2}
    result.foreach(println)
  }
}