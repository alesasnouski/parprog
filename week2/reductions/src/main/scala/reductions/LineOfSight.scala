package reductions

import org.scalameter._

object LineOfSightRunner {

  val standardConfig = config(
    Key.exec.minWarmupRuns -> 40,
    Key.exec.maxWarmupRuns -> 80,
    Key.exec.benchRuns -> 100,
    Key.verbose -> true
  ) withWarmer(new Warmer.Default)

  def main(args: Array[String]): Unit = {
    val length = 10000000
    val input = (0 until length).map(_ % 100 * 1.0f).toArray
    val output = new Array[Float](length + 1)
    val seqtime = standardConfig measure {
      LineOfSight.lineOfSight(input, output)
    }
    println(s"sequential time: $seqtime")

    val partime = standardConfig measure {
      LineOfSight.parLineOfSight(input, output, 10000)
    }
    println(s"parallel time: $partime")
    println(s"speedup: ${seqtime.value / partime.value}")
  }
}

sealed abstract class Tree {
  def maxPrevious: Float
}

case class Node(left: Tree, right: Tree) extends Tree {
  val maxPrevious = left.maxPrevious.max(right.maxPrevious)
}

case class Leaf(from: Int, until: Int, maxPrevious: Float) extends Tree

object LineOfSight extends LineOfSightInterface {

  def lineOfSight(input: Array[Float], output: Array[Float]): Unit = {
    for ((height, heightIdx) <- input.zipWithIndex) {
      if (heightIdx == 0) {
        output(0) = 0;
      } else {
        val tan = height / heightIdx
        val tanToWrite = if (tan > output(heightIdx - 1)) tan else output(heightIdx - 1)
        output(heightIdx) = tanToWrite
      }
    }
  }

  /** Traverses the specified part of the array and returns the maximum angle.
   */
  def upsweepSequential(input: Array[Float], from: Int, until: Int): Float = {
    var maxTan: Float = 0
    for ((height, heightIdx) <- input.slice(from, until).zipWithIndex) {
        val realIdx = heightIdx + from
        val tan = height / realIdx
        if (tan > maxTan) {
          maxTan = tan
        }
    }
    maxTan
  }

  /** Traverses the part of the array starting at `from` and until `end`, and
   *  returns the reduction tree for that part of the array.
   *
   *  The reduction tree is a `Leaf` if the length of the specified part of the
   *  array is smaller or equal to `threshold`, and a `Node` otherwise.
   *  If the specified part of the array is longer than `threshold`, then the
   *  work is divided and done recursively in parallel.
   *
   */
  def upsweep(input: Array[Float], from: Int, end: Int, threshold: Int): Tree = {
    if (threshold < (end - from)) {
      val center = (( end - from ) / 2) + from
      val (t1, t2) = parallel(
        upsweep(input, from, center, threshold),
        upsweep(input, center, end, threshold)
      )
      Node(t1, t2)
    } else Leaf(from, end, upsweepSequential(input, from, end))
  }

  /** Traverses the part of the `input` array starting at `from` and until
   *  `until`, and computes the maximum angle for each entry of the output array,
   *  given the `startingAngle`.
   */
  def downsweepSequential(input: Array[Float], output: Array[Float],
                          startingAngle: Float, from: Int, until: Int): Unit = {
    var maxAngle = startingAngle
    for ((elem, idx) <- input.slice(from, until).zipWithIndex) {
      val realIdx = idx + from
      val thisAngle = elem / realIdx
      if ( thisAngle > maxAngle ) {
        maxAngle = thisAngle
      }
      output(realIdx) = maxAngle
    }
  }

  /** Pushes the maximum angle in the prefix of the array to each leaf of the
   *  reduction `tree` in parallel, and then calls `downsweepSequential` to write
   *  the `output` angles.
   *
   *  TODO : still unfinished:
   *
   * [Test Description] parLineOfSight should invoke the parallel construct 30 times (15 times during upsweep and 15 times during downsweep) for an array of size 17, with threshold 1(reductions.ReductionsSuite)
   * [Observed Error] assertion failed: The number of parallel calls should be between 29 and 31 but was 32
   *
   * [Test Description] downsweep should correctly compute the output for a tree with 4 leaves when the starting angle is zero(reductions.ReductionsSuite)
   * [Observed Error] expected:<List(0.0, 7.0, 7.0, 11.0, 12.0)> but was:<List(0.0, 7.0, 7.0, 12.0, 12.0)>
   *
   * [Test Description] downsweep should correctly compute the output for a non$minuszero starting angle(reductions.ReductionsSuite)
   * [Observed Error] expected:<List(0.0, 8.0, 8.0, 11.0, 12.0)> but was:<List(0.0, 8.0, 8.0, 12.0, 12.0)>
   *
   * [Test Description] parLineOfSight should correctly compute the output for threshold 2(reductions.ReductionsSuite)
   * [Observed Error] expected:<List(0.0, 7.0, 7.0, 11.0, 12.0)> but was:<List(0.0, 7.0, 12.0, 12.0, 12.0)>
   *
   *
   */
  def downsweep(input: Array[Float], output: Array[Float], startingAngle: Float, tree: Tree): Unit = {
    tree match {
      case Leaf(from, until, maxPrevious) =>
        downsweepSequential(input, output, startingAngle, from, until)
      case node: Node =>
        val (pRes1, pRes2) = parallel(
          downsweep(input, output, startingAngle, node.left),
          downsweep(input, output, scala.math.max(startingAngle, node.maxPrevious), node.right)
        )
    }
  }

  /** Compute the line-of-sight in parallel. */
  def parLineOfSight(input: Array[Float], output: Array[Float], threshold: Int): Unit = {
    val t = upsweep(input, 0, input.length, threshold)
    downsweep(input, output, 0, t)
  }
}
