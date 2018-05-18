package com.deb.intv

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext

/**
 * I/P  -
 Sarkar,A
Banerjee,A
Sarkar,D
Biswas,E
Sarkar,B
Biswas,A
Sarkar,F
Biswas,G

O/p -
+--------+---------+
|lastName|firstName|
+--------+---------+
|Sarkar  |A        |
|Sarkar  |B        |
|Sarkar  |D        |
|Sarkar  |F        |
|Biswas  |A        |
|Biswas  |E        |
|Biswas  |G        |
|Banerjee|A        |
+--------+---------+

Sarkar,A
Sarkar,B
Sarkar,D
Sarkar,F
Biswas,A
Biswas,E
Biswas,G
Banerjee,A

 */


object HarmanProb {
  case class Employee(lastName:String,firstName:String){}
   def main(args:Array[String]){
      val conf = new SparkConf().setMaster("local[1]") 
      val ss = SparkSession.builder().config(conf).appName("SecondarySort").getOrCreate()
      val sc = ss.sparkContext
      val rdd = sc.textFile("data/harmandata.txt")
        
      val sqlContext = new SQLContext(sc)
      import sqlContext.implicits._
      
      val rdd2 = rdd.map { x => x.split(",") }
         .map { x => Employee(x(0),x(1)) }
      rdd2.foreach(println)  
      
      import org.apache.spark.sql.functions._
      //StructType(StructField(lastName,StringType,true), StructField(firstNam,StringType,true))
      val df = rdd2.toDF()
      println(df.schema);
    
      val result = df.orderBy(desc("lastName"), asc("firstName"))
      println("**************Approach1***************")
      result.select("lastName","firstName").collect().foreach (println)
      /*
       +--------+---------+
|lastName|firstName|
+--------+---------+
|Sarkar  |A        |
|Sarkar  |B        |
|Sarkar  |D        |
|Sarkar  |F        |
|Biswas  |A        |
|Biswas  |E        |
|Biswas  |G        |
|Banerjee|A        |
+--------+---------+
       */
      println("**************Approach2***************")
      result.select("lastName","firstName").rdd.map { x => x.mkString(",") }.foreach { println }
      /*
       Sarkar,A
Sarkar,B
Sarkar,D
Sarkar,F
Biswas,A
Biswas,E
Biswas,G
Banerjee,A 
       
       */
      
      //Using dataframe
      println("*********Using DataFrame*********")
      val dfResult = df.groupBy("lastName")
                       .agg(sort_array(collect_set("firstName"),false).alias("fNameCma"))
                       .orderBy("lastName")
      dfResult.show(false)
      /* O/P - Ascending order of lastName and descending order of firstName
        +--------+------------+
        |lastName|fNameCma    |
        +--------+------------+
        |Banerjee|[A]         |
        |Biswas  |[G, E, A]   |
        |Sarkar  |[F, D, B, A]|
        +--------+------------+
       
       */
      
//      print(dfResult.select("lastName","fNameCma"))
      
      println("*********Spark Sql*********")
      //Using Spark Sql
      df.createOrReplaceTempView("my_table")
      val result2 = sqlContext.sql("SELECT lastName, sort_array(collect_set(firstName)) AS collected"+ 
                                    " FROM my_table GROUP BY lastName order by lastName")
      result2.show(false)
      /*  O/P - Ascending order of lastName and ascending order of firstName
        +--------+------------+
        |lastName|collected   |
        +--------+------------+
        |Banerjee|[A]         |
        |Biswas  |[A, E, G]   |
        |Sarkar  |[A, B, D, F]|
        +--------+------------+
       */
      result2.select("lastName","collected").rdd.map { x => x.mkString(",") }.foreach { println }
      /*
        Banerjee,WrappedArray(A)
        Biswas,WrappedArray(A, E, G)
        Sarkar,WrappedArray(A, B, D, F)
     //  */
      
      
      println("***************Using Window function ***********");
      import org.apache.spark.sql.expressions.Window
     
      //Define partition
      val reveDesc = Window.partitionBy("lastName").orderBy(desc("lastName"),desc("firstName"))
      //Set ranking
      val rankByDepname = rank().over(reveDesc)
      
      val new_df2 = df.select('*, rankByDepname as 'rank)
      new_df2.show(false)

/*
+--------+---------+----+
|lastName|firstName|rank|
+--------+---------+----+
|Banerjee|A        |1   |
|Sarkar  |F        |1   |
|Sarkar  |D        |2   |
|Sarkar  |B        |3   |
|Sarkar  |A        |4   |
|Biswas  |G        |1   |
|Biswas  |E        |2   |
|Biswas  |A        |3   |
+--------+---------+----+
 */
      
      new_df2.groupBy("lastName").agg(sort_array(collect_set("firstName")).alias("fNameCma"))
      .show(false)
/*      
+--------+------------+
|lastName|fNameCma    |
+--------+------------+
|Banerjee|[A]         |
|Sarkar  |[A, B, D, F]|
|Biswas  |[A, E, G]   |
+--------+------------+     
*/      
   }
}