package com.deb.intv
/**
 * @author root
 */


object TreeTailRec {
sealed abstract class Tree
case class Leaf(n:Int) extends Tree
case class Branch(left: Tree, n:Int, right: Tree) extends Tree 


def sum(t:Tree) = {
  def sum0(acc:Int, ts:List[Tree]) : Int = ts match {
      case Nil => acc
      case t :: ts => t match{
          case Leaf(x) =>sum0(acc+x,ts)
          case Branch(left,x,right) => sum0(acc+x,left :: right :: ts)
      }
  } 
  sum0(0,List(t))
}  

  def main(args: Array[String]){ 
     print("Sum=")
     println(sum(Branch(Leaf(1), 2,Branch(Leaf(3),4,Leaf(5)))))
  }
}