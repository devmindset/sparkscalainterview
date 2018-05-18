package com.deb.intv

/**
 * @author root
 * 
 * Output - 
 * ******* After removing duplicate ********
Hello
How
Scala
Java
 * 
 */
object Demo {
  //def main(args: Array[String]):Unit ={}
  //println("Welcome to Scala");
  def main(args: Array[String]){
    //1st way to print
    var args =  Array("Hello","How","Scala","Java","How")
    for(i<-0 until args.length){
      println(args(i))
    }
   //2nd way to print
    for(myString <- args){
      println(myString);
    }
    println("******* After removing duplicate ********")
    val myArray = args.distinct
    for(i<-0 until myArray.length){
      println(myArray(i))
    }
    
    
  }
}