package com.deb.intv

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.hadoop.yarn.webapp.hamlet.HamletSpec._

//http://stdatalabs.blogspot.com/2017/02/mapreduce-vs-spark-secondary-sort.html
/**
 * 
Problem Statement:
Dataset contains a person's lastname and firstname
The output must contain records sorted on person's lastname first and then the firstname
Each output file must contain records with the same lastname
 *
 */

object SecondarySort {
  def main(args:Array[String]){
      val conf = new SparkConf().setMaster("local[1]") 
      val ss = SparkSession.builder().config(conf).appName("SecondarySort").getOrCreate()
      val sc = ss.sparkContext
      
      val df = sc.textFile("data/SecondarySortData.csv") 
        //ss.read.csv("data/SecondarySortData.csv") - If u need to remove header u need to use this
      val rdd = df.map { x => x.split(",") }.map { x => (x(0),x(1)) }
      //Group and Sort based on lastname ascending
      val rdd2 = rdd.groupByKey().sortByKey(true, 2)
      //rdd2.map(x => x._1,x._2.)
      rdd2.foreach(println)
      println("********************************")
      
      //Approach1
      val reesult = rdd2.map{case(x,iter) => (x,iter.toList.sorted)}
      println("Ascending order of firstname")
      reesult.foreach(println)

      //most of the time calling toSeq is not good idea because driver needs to put this in memory and you might run out of memory in 
      //on larger data sets. I guess this is o.k. for intro to spark.
      //Approach2 - Descending order - iter.toList.sorted will do ascending order only
      val reesult2 = rdd2.map{case(x,iter) => (x,iter.toList.sortWith(_>_) )}
      println("Descending order of firstname")
      reesult2.foreach(println)
      println("")
      println("Before Individual records")
      val result3 = reesult2.map{case(key,iter) => {
        val lst = iter.toList
        lst.map{ x => (key,x) }.mkString("-")
        //  iter.toList.map { y => (x,y) }
      }}
      println("Individual records")
      //result3.foreach(println)
      result3.flatMap { x => x.split("-") }.foreach(println)
      
      println("**************************************")
      println("*************Next Approach************")
      println("**************************************")
      val numReducers =4
      val listRDD = rdd.groupByKey(numReducers).mapValues(iter => iter.toList.sortBy(r => r))
      
      val resultRDD = listRDD.flatMap {
      case (label, list) => {
        list.map((label, _))
      }
    }
      resultRDD.foreach(println)
      
      
      
  }
}