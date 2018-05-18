package com.deb.intv

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext

object MaxSalDeptWise {
  val conf = new SparkConf().setMaster("local[*]");
  val sc = SparkSession.builder().config(conf).appName("OracleSpark").getOrCreate().sparkContext;
  def main(args:Array[String]){
    val emp_data=sc.textFile("data/empdept")
    //Remove header
    val emp_header=emp_data.first()
    val emp_data_without_header=emp_data.filter { x => !x.equals(emp_header) }
    //emp_data_without_header.foreach(println)  
    
    //empno,ename,designation,manager,hire_date,sal,deptno
    val mappedResult = emp_data_without_header.map { x => (x.split(",")(6),x.split(",")(5).toDouble)}
    val maxSal = mappedResult.reduceByKey(math.max(_, _)).sortBy(_._2,false)
    maxSal.collect().foreach(println)
    
    
    val mappedResultDept = emp_data_without_header.map { x => (x.split(",")(6),(x.split(",")(1),x.split(",")(5).toDouble))}
    val maxByDept = mappedResultDept.reduceByKey((acc,element) => if(acc._2 > element._2 ) acc else element)
    
    println("maximum salaries in each dept" + maxByDept.collect().toList)
    
    
    /*
     *It will show only highest sal
     *  val mappedResultDept2 = emp_data_without_header.map { x => (x.split(",")(6),(x.split(",")(1),x.split(",")(5).toDouble))}
    val maxByDept2 = mappedResultDept2.reduce((acc,element) => if(acc._2._2 > element._2._2 ) acc else element)
    println(maxByDept2)*/
    
    
    //----------------------
    
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    /*emp_data_without_header.foreach(println) 
    val dataFrame = emp_data_without_header.toDF("empno","ename","designation","manager","hire_date","sal","deptno")
    val df = dataFrame.repartition($"deptno")
    df.explain()*/
    
    //2nd highest Salary for each dept
   
     import org.apache.spark.sql.functions._
     import org.apache.spark.sql.expressions.Window
     
     val df4 = sqlContext.read.format("csv").option("header", true).load("data/empdept")
     df4.printSchema()
     //Define partition
      val reveDesc = Window.partitionBy("deptno").orderBy(desc("sal"))
      //Set ranking
      val rankByDepname = rank().over(reveDesc)
      
      val new_df2 = df4.select('*, rankByDepname as 'rank).where($"rank" === 2)
      new_df2.show(false)
    
    
    
  }
}