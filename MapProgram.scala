package com.deb.intv

/**
 * @author root
 * O/P -
 * Map(Item3 -> 72.0, Item6 -> 54.0, Item8 -> 18.0, Item2 -> 63.0, Item5 -> 45.0, Item7 -> 36.0, Item1 -> 90.0, Item4 -> 81.0)
 * 
 */
object MapProgram {
  def main(args: Array[String]){
    var mapping = scala.collection.mutable.Map(
    "Item1"->100
    ,"Item2"->70
    ,"Item3"->80
    ,"Item4"->90
    ,"Item5"->50
    ,"Item6"->60
    ,"Item7"->40
    ,"Item8"->20
    )
    
    var copiedMap = scala.collection.mutable.Map[String,Double]()

    //After slashing price by 10%
    for((key,value)<- mapping){
     // println(key+"--->"+value)
      copiedMap(key) = value*0.9;
    }
    println(copiedMap)
    
  }
}