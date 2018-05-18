package com.deb.intv

/**
 * @author root
 */


sealed trait Tree2
case object Empty2 extends Tree2
case class Leaf2(value:Int) extends Tree2
case class Branch2(value: Int, left: Tree2, right: Tree2) extends Tree2

object TreeTailRec2 {
  
  def max_height2(tree:Tree2):Int = tree match{
     case Empty2 => 0
     case Leaf2(v) => 0
     case Branch2(_,left,right) =>  1 + math.max(max_height2(left), max_height2(right))
  }
  
 def sumofLeaf(tree:Tree2):Int = tree match{
   case Empty2 => 0
   case Leaf2(v) => v
   case Branch2(_,left,right) =>   sumofLeaf(left)+sumofLeaf(right)
 } 
 
 /*
def sum2(t:Tree2) = {
  def sum1(acc:Int, ts:List[Tree2]) : Int = ts match {
      case Nil => acc
      case t :: ts => t match{
          case Leaf2(x) =>sum1(acc+x,ts)
          case Branch2(left,x,right) => sum1(acc+x,left :: right :: ts)
      }
  } 
  sum1(0,List(t))
} */

  
    def main(args: Array[String]){ 
     val example = Branch2(2,Leaf2(7),
                          Branch2(5,Branch2(3,
                                    Branch2(4,Leaf2(9),Empty2)
                                    ,Empty2),
                                     Branch2(1,Leaf2(8),Empty2)))
         
     print("Max_height=")
     println(max_height2(example))
     println()
     print("Sum of all nodes=")
     println(sumofLeaf(example))
     
  }
  
  
}