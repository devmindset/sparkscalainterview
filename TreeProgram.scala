package com.deb.intv

/**
 * @author root
 */

import scala.annotation.tailrec

sealed trait Tree[+A]
case object Empty extends Tree[Nothing]
case class Leaf[A](value:A) extends Tree[A]
case class Branch[A](value: A, left: Tree[A], right: Tree[A]) extends Tree[A]

object TreeProgram {
  
  def depth[A](tree:Tree[A]):Int = tree match {
    case Empty => 0
    case Leaf(v) => 1
    case Branch(_,left,right) => 1+ math.max(depth(left), depth(right))
  }
  
  def max_height[A](tree:Tree[A]):Int = tree match{
     case Empty => 0
     case Branch(_,left,right) =>  math.max(depth(left), depth(right))
  }

/*  def sum[A](tree:Tree[A]):Int = {
    @tailrec
    def sumAcc(tree:List[Tree[A]], acc:Int): Int = tree match{
      case Nil => acc
      case Leaf(v) :: rs => sumAcc(rs,acc+v)
      case Branch(_,left,right) ::rs => sumAcc(left :: right :: rs, acc)
    }
    sumAcc(List(tree),0)
  }*/
  
  //def max_amplitude[A](tree:Tree[A]):Int = 
  
  def main(args: Array[String]){ 
     val example = Branch(2,Leaf(7),
                          Branch(5,Branch(3,
                                    Branch(4,Leaf(9),Empty)
                                    ,Empty),
                                     Branch(1,Leaf(8),Empty)))
         
     print("Depth=")
     println(depth(example))
     println()
     print("Max_height=")
     println(max_height(example))
  }
  
}