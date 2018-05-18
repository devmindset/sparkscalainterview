package com.deb.intv

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext

/**
 * Say you have 2 columns String and Integer type respectively and want to store it in 3rd column
 */
object AggOf2ColStoredIn3rdCol {
  val conf = new SparkConf().setMaster("local")
  val sc = SparkSession
    .builder
    .appName("ProductInfo")
    .config(conf)
    .getOrCreate().sparkContext

  def main(args: Array[String]) {
    
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    val data = sc.parallelize(
      Seq(
        ("Joy", 1 ),("Ram", 2 ),("Abhi", 3 ),("Deb", 4 ))).toDF("Name", "Loc_Id")

    //Approach 1 - Concat Specific Columns
    import org.apache.spark.sql.functions.concat_ws    
    val result = data.withColumn("ConcaCol", concat_ws("_",$"Name",$"Loc_Id"))    
    result.show(false);    
    
    /* OUTPUT - 
    +----+------+--------+
    |Name|Loc_Id|ConcaCol|
    +----+------+--------+
    |Joy |1     |Joy_1   |
    |Ram |2     |Ram_2   |
    |Abhi|3     |Abhi_3  |
    |Deb |4     |Deb_4   |
    +----+------+--------+
     */
    
    //Approach2 - Concat All Columns using struct
    println("Concat All Columns using struct")
    import org.apache.spark.sql.functions.struct
    data.withColumn("NewCol",struct(data.columns.head, data.columns.tail: _*)).show(false)
    /*
     OUTPUT -
     Concat All Columns using struct
    +----+------+--------+
    |Name|Loc_Id|NewCol  |
    +----+------+--------+
    |Joy |1     |[Joy,1] |
    |Ram |2     |[Ram,2] |
    |Abhi|3     |[Abhi,3]|
    |Deb |4     |[Deb,4] |
    +----+------+--------+
     */
    
    //Approach3 
    println("Using concat only")
    import org.apache.spark.sql.functions.{concat, lit}
    data.select(concat($"Name", lit(" "), $"Loc_Id")).alias("ConcatedCol").show(false)
    /* OUTPUT - 
      +-----------------------+
      |concat(Name,  , Loc_Id)|
      +-----------------------+
      |Joy 1                  |
      |Ram 2                  |
      |Abhi 3                 |
      |Deb 4                  |
      +-----------------------+
     */

    //Approach 4 - Handle null
    println("Concat along with handling null value")
    import org.apache.spark.sql.functions.col
    import org.apache.spark.sql.functions.when
    val newDf = data.withColumn("NEW_COLUMN"
         , concat(
           when(col("Name").isNotNull,col("Name")).otherwise(lit("null"))
         , when(col("Loc_Id").isNotNull,col("Loc_Id")).otherwise(lit("null"))
         )
         )
    newDf.show(false)
    /*
      OUTPUT - 
      +----+------+----------+
      |Name|Loc_Id|NEW_COLUMN|
      +----+------+----------+
      |Joy |1     |Joy1      |
      |Ram |2     |Ram2      |
      |Abhi|3     |Abhi3     |
      |Deb |4     |Deb4      |
      +----+------+----------+
     */
    
    val newDf2 = data.selectExpr("concat(nvl(Name, ''), nvl(Loc_Id, '')) as NEW_COLUMN")
    newDf2.show(false)
    /*
     OUTPUT - 
      +----------+
      |NEW_COLUMN|
      +----------+
      |Joy1      |
      |Ram2      |
      |Abhi3     |
      |Deb4      |
      +----------+
     */
    
    //Approach 5 - Using UDF
    println("Using UDF")
    import org.apache.spark.sql.functions.udf
    //Define a udf to concatenate two passed in string values
    val getConcatenated = udf( (first: String, second: String) => { first + " " + second } )

    //use withColumn method to add a new column called newColName
    data.withColumn("newColName", 
        getConcatenated($"Name", $"Loc_Id")).select("newColName", "Name", "Loc_Id")
        .show(false)
    /*
      OUTPUT - 
      +----------+----+------+
      |newColName|Name|Loc_Id|
      +----------+----+------+
      |Joy 1     |Joy |1     |
      |Ram 2     |Ram |2     |
      |Abhi 3    |Abhi|3     |
      |Deb 4     |Deb |4     |
      +----------+----+------+     
     */
    
        
  }
}