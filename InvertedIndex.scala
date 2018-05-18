package com.deb.intv

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object InvertedIndex {
  
  val conf = new SparkConf().setMaster("local[*]")
  val ss = SparkSession.builder().appName("InvertedIndex").config(conf).getOrCreate()
  val sc = ss.sparkContext
  
  def main(args:Array[String] ){
    val data = sc.wholeTextFiles("inverted_data/*")
    
    data.keys.collect().foreach(println)
    println("***************")
   val wordfile= data.flatMap{case (path,text) =>
        text.split(" ").map { x => (x.trim ,path) }
    }
  // wordfile.collect().foreach(println) 
   
   val result = wordfile.map{case (word,path) => 
    ((word,path),1)
   }
   
   val result2 =  result.reduceByKey(_+_)
   
   //result2.collect().foreach(println)
   
   val result3 = result2.map{case((word,file),count) =>
     (word,(file->count))  
   } 
   result3.groupByKey().collect().foreach(println)
   
   
  }
}