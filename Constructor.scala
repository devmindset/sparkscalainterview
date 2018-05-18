package com.deb.intv

/**
 * @author root
 */

class Point(strings: String*){
  def move(){
    strings.foreach(print);
    println();
  }
  
}

object Constructor {
   def main(args: Array[String]){ 
         var pt = new Point("1");
         pt.move();
         var pt2 = new Point("1","2");
         pt2.move();
         var pt3 = new Point("1","2","3");
         pt3.move();
         var pt4 = new Point("1","2","3","4");
         pt4.move();
         
   }
}