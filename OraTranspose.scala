package com.deb.intv

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf

//https://medium.com/@chandukavar/thinking-in-big-data-part-2-transpose-matrix-for-large-datasets-8415a3955420
//https://medium.com/@chandukavar/thinking-in-big-data-part-1-top-n-most-viewed-products-from-large-dataset-in-e-commerce-db5e869f1920
object OraTranspose {
   val conf = new SparkConf().setMaster("local[*]")
   val sparkContext = SparkSession.builder().config(conf).appName("SortNumFile").getOrCreate().sparkContext
   sparkContext.setLogLevel("ERROR")
  
   def main(args:Array[String]){
        val rdd = sparkContext.parallelize(Seq(Seq(1, 2, 3), Seq(4, 5, 6), Seq(7, 8, 9), Seq(10, 11, 11)))
        //val transposed = sparkContext.parallelize(rdd.collect.toSeq.transpose)     
        //transposed.foreach(println)
        
        var counter = sparkContext.longAccumulator
        
        //rdd.zipWithIndex().map { x => (x) }.collect().foreach(println)
        
        
        //Approach1 
        val sec2= rdd.flatMap(x => x.indices.zip(x)  )
        sec2.foreach(println)
/*Output - 
(0,10)
(1,11)
(2,11)
(0,1)
(1,2)
(2,3)
(0,7)
(1,8)
(2,9)
(0,4)
(1,5)
(2,6)         
 */
        
        //Approach2
        println("********************************************************")
        val sec3= rdd.flatMap(x => x.zipWithIndex  )
        sec3.foreach(println)
/*Output -
(1,0)
(2,1)
(3,2)
(4,0)
(5,1)
(6,2)
(10,0)
(11,1)
(11,2)
(7,0)
(8,1)
(9,2)
*/
        println("********************************************************")
        val mapping = sec3.map(x=>(x._2,x._1))
        //If u do not mention num of partition in sortbykey result will be diff every time u execute
        //sortByKey is used to ensure that based on column value is displayed
        // here instead of map we use mapValues then col num will also get appended in output
        //.mapValues(_.map(_._2).mkString(","))
        val result = mapping.groupByKey().sortByKey(true,1).map(_._2)
        val finalResult = result.map { x => x.mkString(",") }
        finalResult.foreach(println) 
        
        /*Output - 
         1,4,7,10
         2,5,8,11
         3,6,9,11
         */

       
       /*modRdd.map{ case(values,row) => (row, formatRow(values.mkString(" "))) }
       .map{ case(row, formattedValues) => 
            values.map( value => (row, value)) 
   }
   .flatMap { rowValues => rowValues.indices zip rowValues }
   .groupByKey*/
        
        /*val inputRDD = sc.parallelize(List(
    (0, "1 2 3"),
    (1, "4 5 6"), 
    (2, "7 8 9")
))
def formatRow(row: String) = row.split(" ").map(_.toInt)
inputRDD
   .map{ case(row, values) => (row, formatRow(values)) }
   .map{ case(row, formattedValues) => 
            values.map( value => (row, value)) 
   }
   .flatMap { rowValues => rowValues.indices zip rowValues }
   .groupByKey*/
        
        
   }
}