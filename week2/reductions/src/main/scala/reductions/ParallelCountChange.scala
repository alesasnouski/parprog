package reductions

import org.scalameter._

object ParallelCountChangeRunner {

  @volatile var seqResult = 0

  @volatile var parResult = 0

  val standardConfig = config(
    Key.exec.minWarmupRuns -> 20,
    Key.exec.maxWarmupRuns -> 40,
    Key.exec.benchRuns -> 80,
    Key.verbose -> true
  ) withWarmer(new Warmer.Default)

  def main(args: Array[String]): Unit = {
    val amount = 250
    val coins = List(1, 2, 5, 10, 20, 50)
    val seqtime = standardConfig measure {
      seqResult = ParallelCountChange.countChange(amount, coins)
    }
    println(s"sequential result = $seqResult")
    println(s"sequential count time: $seqtime")

    def measureParallelCountChange(threshold: => ParallelCountChange.Threshold): Unit = try {
      val fjtime = standardConfig measure {
        parResult = ParallelCountChange.parCountChange(amount, coins, threshold)
      }
      println(s"parallel result = $parResult")
      println(s"parallel count time: $fjtime")
      println(s"speedup: ${seqtime.value / fjtime.value}")
    } catch {
      case e: NotImplementedError =>
        println("Not implemented.")
    }

    println("\n# Using moneyThreshold\n")
    measureParallelCountChange(ParallelCountChange.moneyThreshold(amount))
    println("\n# Using totalCoinsThreshold\n")
    measureParallelCountChange(ParallelCountChange.totalCoinsThreshold(coins.length))
    println("\n# Using combinedThreshold\n")
    measureParallelCountChange(ParallelCountChange.combinedThreshold(amount, coins))
  }
}

object ParallelCountChange extends ParallelCountChangeInterface {

  /** Returns the number of ways change can be made from the specified list of
   *  coins for the specified amount of money.
   */

  def countChange(money: Int, coins: List[Int]): Int = {
    def innerCombinations(combs: Array[Int], coins: List[Int]): Int = {
      if (coins.nonEmpty) {
        val coin = coins.head
        combs.zipWithIndex.foreach ( enumerated => {
          val privElementIdx: Int = enumerated._2 - coin
          if (enumerated._2 >= coin && privElementIdx >= 0) {
            combs(enumerated._2) = enumerated._1 + combs(privElementIdx)
          }
        })
        innerCombinations(combs, coins.tail)
      } else {
        combs.last
      }
    }
    if (coins.length == 1 && money == coins.head) 1
    else if (money == 0) 1
    else if (money < 0) 0
    else if (coins.length == 1 && money < coins.head) 0
    else if (coins.length == 0) 0
    else {
      val combs = (0 :: List.fill(money)(0)).updated(0, 1).toArray
      innerCombinations(combs, coins)
    }
  }


  type Threshold = (Int, List[Int]) => Boolean

  /** In parallel, counts the number of ways change can be made from the
   *  specified list of coins for the specified amount of money.
   */
  def parCountChange(money: Int, coins: List[Int], threshold: Threshold): Int = {
    (money, coins) match {
      case (m, c) if (m > 0 && c.nonEmpty && m < c.head) => 0;
      case (m, c) if (m > 0 && c.nonEmpty) =>
        if (threshold(money, coins)) {
          countChange(money - coins.head, coins) + countChange(money, coins.tail)
        } else {
          val res: Tuple2[Int, Int] = parallel(
            parCountChange(money - coins.head, coins, threshold),
            parCountChange(money, coins.tail, threshold)
          )
          res._1 + res._2
        }
      case (m, c) if (m > 0 && c.isEmpty) => 0;
      case (m, c) if (m == 0) => 1;
    }
  }

  /** Threshold heuristic based on the starting money. */
  def moneyThreshold(startingMoney: Int): Threshold =
    (m, _) => m <= (2 * startingMoney) / 3

  /** Threshold heuristic based on the total number of initial coins.
   *
   * [Test Description] totalCoinsThreshold should return true when the number of coins is equal to or less than two$minusthirds of the
   * initial number of coins(reductions.ReductionsSuite)
   * [Observed Error] assertion failed: totalCoinsThreshold should return true, hint: initial number of coins: 3
   * */
  def totalCoinsThreshold(totalCoins: Int): Threshold =
    (_, coins) => coins.length <= (2 * totalCoins) / 3

  /** Threshold heuristic based on the starting money and the initial list of coins. */
  def combinedThreshold(startingMoney: Int, allCoins: List[Int]): Threshold = {
    //Then, implement the method combinedThreshold, which returns a threshold function that returns true when the amount of money multiplied with the number of remaining coins is less than or equal to the starting money multiplied with the initial number of coins divided by 2:
    (m, coins) => m * coins.length <= startingMoney * allCoins.length / 2
  }
}