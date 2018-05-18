package com.deb.intv

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object OracleSpark {
  val conf = new SparkConf().setMaster("local[*]");
  val sc = SparkSession.builder().config(conf).appName("OracleSpark").getOrCreate().sparkContext;
  def main(args:Array[String]){
    /* Input -     
    u1,fb|fb|tweet|link
    u2,fb|tweet|insta
    u3,tweet|hike|msgn|msgn|msgn
    */
    val dataRDD =   sc.textFile("data/oraclespark.txt");
    
    val transRDD =  dataRDD.map{x => x.split(",")}
                         .map{y => (y(0),y(1))}
                         .flatMap{z =>z._2.split("\\|")}

    val result = transRDD.map { x => (x,1) }.reduceByKey(_+_).collect();                          
              
    result.foreach(println);
    /* Output - 
        (fb,3)
        (link,1)
        (msgn,3)
        (insta,1)
        (tweet,3)
        (hike,1)
     */
    
    println("**********************************8")
    //If you do not do distinct.mkString("-") u will get error
    val transRDD3 =  dataRDD.map{x => x.split(",")}
                         .map{y => (y(0),y(1))}
                         .map{z =>(z._2.split("\\|").distinct.mkString("-"))}
                         .flatMap(x => x.split("-"))
                         .map { x => (x,1) }
                         .reduceByKey(_+_)
    
    transRDD3.foreach(println)
    /* OUTPUT -
    (msgn,1)
(insta,1)
(tweet,3)
(hike,1)
(fb,2)
(link,1)
    */
  }
}