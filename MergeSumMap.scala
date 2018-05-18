package com.deb.intv

/**
 * @author root
 */
object MergeSumMap {
  
  def mergeMap(m1:Map[Int,Int], m2:Map[Int,Int] ) : Map[Int,Int] = {
    //Create a new empty map and concatenate one map store it in new map
    var map: Map[Int,Int]  = Map[Int,Int]() ++ m1
    for(p<-m2){
      map = map + (p._1 -> (p._2+ map.getOrElse(p._1,0)))
    }
    map
  }
  
  
  def main(args: Array[String]){ 
     val map1 = Map(1->9,2->20)
     val map2 = Map(1->11,4->15,2->11)
     
     //Merge 2 maps and sum up the common key values
     val map3 = map1 ++ map2.map{case (k,v) => k -> (v+map1.getOrElse(k,0))}
     println(map3)
     
     //Print map3
     map3 foreach{case (k,v) => println("Key="+k+" - Value="+v) }
     
     var map4 = map1 ++ (for ( (k,v) <- map2) yield ( k -> (v + map1.getOrElse(k, 0)) ) )
     
     //print map 
     map4 foreach( x => println(x._1 +" - "+ x._2) ) 
     
     var map5 = mergeMap(map1,map2)
     println(map5)
     
  }
  
}