package com.deb.intv
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object MovingAverage {
    val conf = new SparkConf().setMaster("local[*]")
    val sc = SparkSession.builder().appName("AverageRevenue").config(conf).getOrCreate().sparkContext
     sc.setLogLevel("ERROR")
   def main(args:Array[String]): Unit ={
      val ts = sc.parallelize(0 to 10, 2)
      val window = 3
//A simple partitioner that puts each row in the partition we specify by the key:

class StraightPartitioner(p: Int) extends org.apache.spark.Partitioner {
  def numPartitions = p
  def getPartition(key: Any) = key.asInstanceOf[Int]
}

 /*
  
  scala> val parallel = sc.parallelize(1 to 9, 3)
parallel: org.apache.spark.rdd.RDD[Int] = ParallelCollectionRDD[457] at parallelize at <console>:12
 
scala> parallel.mapPartitionsWithIndex( (index: Int, it: Iterator[Int]) => it.toList.map(x => index + ", "+x).iterator).collect
res390: Array[String] = Array(0, 1, 0, 2, 0, 3, 1, 4, 1, 5, 1, 6, 2, 7, 2, 8, 2, 9)
       
  */
//Create the data with the first window - 1 rows copied to the previous partition:
println("Hello")
val partitioned = ts.mapPartitionsWithIndex((i:Int, p:Iterator[Int]) => {
  
  val overlap = p.take(window - 1).toArray
  println("overlap="+overlap.mkString(","))
  
  val spill = overlap.iterator.map((i - 1, _))
  println("spill="+spill.mkString(","))
  
  val keep = (overlap.iterator ++ p).map((i, _))
  println("keep="+keep.mkString(","))
  
  if (i == 0) keep else keep ++ spill
}).partitionBy(new StraightPartitioner(ts.partitions.length)).values
   
//    Just calculate the moving average on each partition:

val movingAverage = partitioned.mapPartitions(p => {
  val sorted = p.toSeq.sorted
  val olds = sorted.iterator
  val news = sorted.iterator
  println("olds = "+olds.toList.toString())
  println("news = "+news.toList.toString())
  var sum = news.take(window - 1).sum
  println("Sum = "+sum)
  (olds zip news).map({ case (o, n) => {
    sum += n
    val v = sum
    sum -= o
    v
  }})
})
    
 println("Average = "+ movingAverage.collect().mkString(","))    
    
    }
}