package com.deb.intv

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf

object TavantCoGroup {
  val conf = new SparkConf().setMaster("local[*]");
  val sc = SparkSession.builder().config(conf).appName("TavantCoGroup").getOrCreate().sparkContext;
  def main(args:Array[String]){
    val rdd1 = sc.parallelize(Seq(
                     ("key1", 1),
                     ("key2", 2),
                     ("key1", 3)))
                     
    val rdd2 = sc.parallelize(Seq(
                     ("key1", 5),
                     ("key2", 4)))
                     
    val grouped = rdd1.cogroup(rdd2)
    grouped.collect().foreach(println)
    /*
     * Output -
         (key1,(CompactBuffer(1, 3),CompactBuffer(5)))
         (key2,(CompactBuffer(2),CompactBuffer(4))) 
     */
    
    val joined = rdd1.join(rdd2)
    joined.collect().foreach(println)
    /*
     * Output -  
      (key1,(1,5))
      (key1,(3,5))
      (key2,(2,4))
     */
    
  }
}