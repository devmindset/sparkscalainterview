package com.deb.intv

/**
 * par splits your list for processing over several threads. 
 * You can then regulate how the threading is done by modyfying the tasksupport member of the resultant ParSeq.
 */
object ParallelCollection {
  
  def main(args:Array[String]){

    var i =0
    for(j <-(1 to 100).par){
      //load i from memory
      //add 1 to registry
      //store i to the memory
       i +=1      
    }
    println(i)
    
    (1 to 5).par foreach println
    println("***************")     
    (1 to 5) foreach println


    
  }
}