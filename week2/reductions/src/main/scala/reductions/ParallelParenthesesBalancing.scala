package reductions

import scala.annotation._
import org.scalameter._

object ParallelParenthesesBalancingRunner {

  @volatile var seqResult = false

  @volatile var parResult = false

  val standardConfig = config(
    Key.exec.minWarmupRuns -> 40,
    Key.exec.maxWarmupRuns -> 80,
    Key.exec.benchRuns -> 120,
    Key.verbose -> true
  ) withWarmer(new Warmer.Default)

  def main(args: Array[String]): Unit = {
    val length = 100000000
    val chars = new Array[Char](length)
    val threshold = 10000
    val seqtime = standardConfig measure {
      seqResult = ParallelParenthesesBalancing.balance(chars)
    }
    println(s"sequential result = $seqResult")
    println(s"sequential balancing time: $seqtime")

    val fjtime = standardConfig measure {
      parResult = ParallelParenthesesBalancing.parBalance(chars, threshold)
    }
    println(s"parallel result = $parResult")
    println(s"parallel balancing time: $fjtime")
    println(s"speedup: ${seqtime.value / fjtime.value}")
  }
}

object ParallelParenthesesBalancing extends ParallelParenthesesBalancingInterface {

  /** Returns `true` iff the parentheses in the input `chars` are balanced.
   */
  def balance(chars: Array[Char]): Boolean = {
    val result = chars.foldLeft(0)((acc, ch) =>
      (acc, ch) match {
        case (-1, _) => -1;
        case (a, ')') => a - 1;
        case (a, '(') => a + 1;
        case (a, _) => a;
      }
    )
    result == 0
  }

  /** Returns `true` iff the parentheses in the input `chars` are balanced.
   */
  def parBalance(chars: Array[Char], threshold: Int): Boolean = {

    def traverse(idx: Int, until: Int): List[Int] = {
      var left = 0
      var right = 0
      for (ch <- chars.slice(idx, until)) {
        ch match {
          case '(' => right += 1;
          case ')' if (right > 0) => right -= 1;
          case ')' if (right <= 0) => left -= 1;
          case _ => ()
        }
      }
      List(left, right)
    }

    def reduce(from: Int, until: Int): List[Int] = {
      def reduceInner(ifrom: Int, iuntil: Int): List[Int] = {
        if ((iuntil - ifrom) > threshold) {
          val center = (iuntil - ifrom) / 2 + ifrom
          val parResult = parallel(reduceInner(ifrom, center), reduceInner(center, iuntil))
          val centerSum = parResult._2(0) + parResult._1(1)
          if (centerSum >= 0) {
            List(parResult._1(0), parResult._2(1) + centerSum)
          } else {
            List(parResult._1(0) + centerSum, parResult._2(1))
          }
        } else traverse(ifrom, iuntil)
      }
      reduceInner(from, until)
    }

    val result = reduce(0, chars.length)
    if (result.head < 0) false else {
      result.filter(p => p != 0).isEmpty
    }
  }
}
