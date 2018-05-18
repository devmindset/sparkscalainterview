package com.deb.intv

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.expressions.Window

object Xebia {
   
     val conf = new SparkConf().setMaster("local[*]")
     val ss = SparkSession.builder().appName("Xebia").config(conf).getOrCreate()
     val sc = ss.sparkContext
     sc.setLogLevel("ERROR")
   
  def main(args:Array[String] ){

    val sqlContext = new SQLContext(sc)
    import  sqlContext.implicits._
  
    //Load data into DataFrame --- 1st question
    val df2 = sqlContext.read.format("csv").option("header", "true").load("data/xebia.csv")
    df2.show(false)
	
	/*
+--------+--------+--------------+-----+--------+--------+-------+------+---+-----------+--------+---+-----+
|date    |register|private_agency|state|district|sub_dist|pincode|gender|age|aadhaar_gen|rejected|mob|email|
+--------+--------+--------------+-----+--------+--------+-------+------+---+-----------+--------+---+-----+
|20150419|ABank   |Pvt1          |WB   |KOL     |KOL_E   |700051 |M     |42 |100        |5       |95 |50   |
|20150420|ABank   |Pvt1          |WB   |KOL     |KOL_W   |700052 |F     |20 |150        |10      |145|140  |
|20150419|ABank   |Pvt2          |WB   |KOL     |KOL_E   |700051 |F     |25 |120        |0       |120|120  |
|20150420|ABank   |Pvt2          |WB   |MUR     |MUR_E   |400051 |M     |60 |20         |10      |10 |2    |
|20150420|ABank   |Pvt2          |WB   |MUR     |MUR_W   |400050 |F     |40 |40         |0       |5  |0    |
|20150420|ABank   |Pvt2          |WB   |MUR     |MUR_N   |400052 |M     |25 |100        |10      |60 |10   |
|20150421|Bbank   |Pvt3          |WB   |MUR     |MUR_N   |400052 |M     |15 |100        |15      |65 |15   |
|20150419|Cbank   |Pvt2          |HR   |DEL     |DEL_E   |700051 |M     |42 |200        |5       |95 |50   |
|20150420|CBank   |Pvt4          |HR   |DEL     |DEL_W   |700052 |F     |20 |125        |10      |115|110  |
|20150419|ABank   |Pvt4          |HR   |DEL     |DEL_E   |700051 |F     |25 |120        |0       |120|120  |
|20150420|BBank   |Pvt4          |HR   |GUR     |GUR_E   |400051 |M     |60 |20         |10      |10 |2    |
|20150420|BBank   |Pvt4          |HR   |GUR     |GUR_W   |400050 |F     |40 |40         |0       |5  |0    |
|20150420|DBank   |Pvt4          |HR   |GUR     |GUR_N   |400052 |M     |18 |100        |10      |60 |10   |
|20150421|Bbank   |Pvt1          |HR   |GUR     |GUR_N   |400052 |M     |18 |100        |15      |65 |15   |
|20150419|Ebank   |Pvt2          |UP   |LUC     |LUC_E   |700051 |M     |35 |200        |5       |95 |50   |
|20150420|CBank   |Pvt4          |UP   |LUC     |LUC_E   |700052 |F     |18 |125        |10      |115|110  |
|20150419|ABank   |Pvt4          |UP   |LUC     |LUC_W   |700051 |F     |19 |120        |0       |120|120  |
|20150420|BBank   |Pvt4          |UP   |ALH     |ALH_E   |400051 |M     |65 |20         |20      |10 |2    |
|20150420|BBank   |Pvt4          |UP   |ALH     |ALH_E   |400050 |F     |45 |140        |0       |5  |40   |
|20150420|DBank   |Pvt5          |UP   |ALH     |ALH_S   |400052 |M     |5  |180        |10      |60 |10   |
+--------+--------+--------------+-----+--------+--------+-------+------+---+-----------+--------+---+-----+
	*/
	
    df2.printSchema()
    /*
     * O/P - 
     *  root
         |-- ?date: string (nullable = true)
         |-- register: string (nullable = true)
         |-- private_agency: string (nullable = true)
         |-- state: string (nullable = true)
         |-- district: string (nullable = true)
         |-- sub_dist: string (nullable = true)
         |-- pincode: string (nullable = true)
         |-- gender: string (nullable = true)
         |-- age: string (nullable = true)
         |-- aadhaar_gen: string (nullable = true)
         |-- rejected: string (nullable = true)
         |-- mob: string (nullable = true)
         |-- email: string (nullable = true)
     */
    
    
    
    //Find the number of Aadhaar registration rejected in WB and HR
     //Define partition
    import org.apache.spark.sql.functions.{sum,desc}
    df2.filter($"state" === "WB" || $"state" === "HR")
    .groupBy($"state")
    .agg(sum($"rejected"))
    .show()
	
/*
O/P - 

+-----+-------------+
|state|sum(rejected)|
+-----+-------------+
|   HR|         50.0|
|   WB|         50.0|
+-----+-------------+
*/	
    
    //Find top 3 states generating most number of aadhaar card
   val top3 = df2.groupBy($"state")
    .agg(sum("aadhaar_gen").alias("aadhaar_sum"))
    .orderBy(desc("aadhaar_sum"))
    .take(3)
    top3.foreach(println)

/* 
O/P - 
[UP,905.0]
[HR,705.0]
[WB,630.0]
*/

    
    //Find total Number of states
    println("Number of state = ")
    println(df2.select($"state").distinct().count())
/* 
O/P -
Number of state = 
3
*/    
    //Find total Number of district
    println("Number of district = ")
    println(df2.select($"state",$"district").distinct().count())

/*
Number of district = 
6
*/	
 
    //Find district in each state
    df2.select($"state",$"district").distinct().show(false)
 /*
 O/P -
 +-----+--------+
|state|district|
+-----+--------+
|WB   |MUR     |
|WB   |KOL     |
|UP   |LUC     |
|HR   |DEL     |
|UP   |ALH     |
|HR   |GUR     |
+-----+--------+
 */
 
    //---Find subdistrict in each district  
    println("Subdistrict in each district = ")
    df2.select($"state",$"district",$"sub_dist").distinct().show(false)
    
    /*
     * O/P - 
Subdistrict in each district = 
+-----+--------+--------+
|state|district|sub_dist|
+-----+--------+--------+
|HR   |GUR     |GUR_W   |
|UP   |LUC     |LUC_E   |
|HR   |GUR     |GUR_N   |
|UP   |ALH     |ALH_N   |
|HR   |DEL     |DEL_E   |
|WB   |KOL     |KOL_W   |
|HR   |GUR     |GUR_E   |
|HR   |DEL     |DEL_W   |
|WB   |MUR     |MUR_N   |
|UP   |LUC     |LUC_W   |
|UP   |ALH     |ALH_E   |
|WB   |MUR     |MUR_W   |
|UP   |ALH     |ALH_S   |
|WB   |KOL     |KOL_E   |
|WB   |MUR     |MUR_E   |
+-----+--------+--------+
   */

   //Find top 3 states where Aadhaar cards generated percentage for female is highest
    
	println("StateWise total aadhaar Generated for female")
    val resultF = df2.filter($"gender" === "F")
    .groupBy($"state")
    .agg(sum($"aadhaar_gen").alias("aadhaar_sum_F"))
    .orderBy(desc("aadhaar_sum_F"))
      
    import org.apache.spark.sql.expressions._
    import org.apache.spark.sql.functions._
  
    println("StateWise total aadhaar Generated ")
   val resultAll = df2
  .groupBy($"state")
  .agg(sum($"aadhaar_gen").alias("aadhaar_sum_All"))

    // Set alias name  
    val df_asAll = resultAll.as("dfasAll")
    val df_asFemale = resultF.as("dfasFemale")
    
    val joined_df = df_asFemale.join(
    df_asAll
    , col("dfasFemale.state") === col("dfasAll.state")
    , "inner")
    
    val final_result = joined_df
    .withColumn("percentage", ($"aadhaar_sum_F" /  $"aadhaar_sum_All")*100)
    .orderBy(desc("percentage"))
    .take(3)
    
    final_result.foreach (println)
    
    
    resultAll.printSchema()
    resultF.printSchema()
        val joined_df2 = resultAll.join(
    resultF
    , col("resultAll.state") === col("resultF.state")
    , "inner").show(false)
	
	/*
	O/P -
+-----+-------------+-----+---------------+-----------------+
|state|aadhaar_sum_F|state|aadhaar_sum_All|       percentage|
+-----+-------------+-----+---------------+-----------------+
|   UP|        505.0|   UP|          905.0|55.80110497237569|
|   WB|        310.0|   WB|          630.0| 49.2063492063492|
|   HR|        285.0|   HR|          705.0|40.42553191489361|
+-----+-------------+-----+---------------+-----------------+

[UP,505.0,UP,905.0,55.80110497237569]
[WB,310.0,WB,630.0,49.2063492063492]
[HR,285.0,HR,705.0,40.42553191489361]

	*/
	
  }
     
}