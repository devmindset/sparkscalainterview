package com.deb.intv

/**
 * @author root
 * 
 * O/P -
 * Min --->6.1  Max-->88.9
 */
object MinMaxUsingFunction {
   def main(args: Array[String]){
     var myArray = Array(25.5,88.9,14.2,19.9,6.1)
     /*var max =myArray(0)
     var min =myArray(0)
     for(i<-1 until myArray.length){
         if(max < myArray(i)){
           max = myArray(i)
         }
         if(min > myArray(i)){
           min = myArray(i)
         }
     }
     println("Min --->"+min+"  Max-->"+max)*/
     println(minMax(myArray))
     
   }
   
   def minMax(array: Array[Double]) : String= {
     if(array.isEmpty){
       throw new java.lang.UnsupportedOperationException("Array is empty")
     }
     var max:Double = array.head
     var min:Double = array.head
     
     for(x <-array){
         if(min>x) {
           min =x
         }       
         if(max<x) {
           max =x
         }
     }
     return "Min --->"+min+"  Max-->"+max
   }
     
   
}