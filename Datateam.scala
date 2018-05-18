package com.deb.intv

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext

object Datateam {
   val conf = new SparkConf().setMaster("local[*]")
   val sc = SparkSession.builder().appName("AverageRevenue").config(conf).getOrCreate().sparkContext
   sc.setLogLevel("ERROR")
   val month = "Jun"
   val year = 2017
   case class sales(month:String,year:Int,custId:Int,billamt:Double)
   

      
   def main(args:Array[String] ){
       val salesData = sc.textFile("data/datateamintv/*");
       val salesDF =  salesData.map{x => x.split("-")}
                      .map { x => sales( x(0).toString(),x(1).toInt,x(2).toInt, x(3).toDouble  ) }
                      .filter { x => if(x.month == month && x.year==year) true else false }
     // salesDF.collect().foreach{ println}
      
      val sqlContext = new SQLContext(sc)
      import sqlContext.implicits._
      val filteredDDF = salesDF.toDF()
      //filteredDDF.printSchema() 
      
      import org.apache.spark.sql.functions._
     // filteredDDF.groupBy("custId").agg(sum("billamt").alias("totalspend")).show()
//      val result = filteredDDF.groupBy("custId")
//                  .agg(sum("billamt").alias("totalspend"))
//                  .agg(max("totalspend").alias("highestspender"))
      //result.show()
     // result.printSchema()
      
     //result.select("highestspender").take(10).foreach(print) --[700]
     // println(result.head()) - [700.0]
      
      //Remove brackets around value. Convert Dataframe to RDD . Highest spend amount
      //result.rdd.map(m=>m.get(0)).foreach(print)
      
      //OrderBy is just an alias for the Sort function and should give the same result.
      //However in Hive or any other DB the function is quite different.
      val resultFl = filteredDDF.groupBy("custId")
                  .agg(sum("billamt").alias("totalspend"))
                  .orderBy(desc("totalspend"))
      resultFl.show();
      
      print(resultFl.select("custId").head().get(0))
      
   }
}