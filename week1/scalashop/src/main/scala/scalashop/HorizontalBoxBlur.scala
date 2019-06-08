package scalashop

import org.scalameter._
import common._
import scalashop.VerticalBoxBlur.blur

object HorizontalBoxBlurRunner {

  val standardConfig = config(
    Key.exec.minWarmupRuns -> 1,
    Key.exec.maxWarmupRuns -> 1,
    Key.exec.benchRuns -> 1,
    Key.verbose -> true
  ) withWarmer(new Warmer.Default)

  def main(args: Array[String]): Unit = {
    val radius = 3
    val width = 1920
    val height = 1080
    val src = new Img(width, height)
    val dst = new Img(width, height)
    val seqtime = standardConfig measure {
      HorizontalBoxBlur.blur(src, dst, 0, height, radius)
    }
    println(s"sequential blur time: $seqtime ms")

    val numTasks = 32
    val partime = standardConfig measure {
      HorizontalBoxBlur.parBlur(src, dst, numTasks, radius)
    }
    println(s"fork/join blur time: $partime ms")
    println(s"speedup: ${seqtime / partime}")
  }
}


/** A simple, trivially parallelizable computation. */
object HorizontalBoxBlur {

  /** Blurs the rows of the source image `src` into the destination image `dst`,
   *  starting with `from` and ending with `end` (non-inclusive).
   *
   *  Within each row, `blur` traverses the pixels by going from left to right.
   */
  def blur(src: Img, dst: Img, from: Int, end: Int, radius: Int): Unit = {
    for {
      col <- from until end
      row <- 0 until src.width
      nRGBA = boxBlurKernel(src, row, col, radius)
      _ = dst.update(row, col, nRGBA)
    } yield ()
  }

  /** Blurs the rows of the source image in parallel using `numTasks` tasks.
   *
   *  Parallelization is done by stripping the source image `src` into
   *  `numTasks` separate strips, where each strip is composed of some number of
   *  rows.
   */
  def parBlur(src: Img, dst: Img, numTasks: Int, radius: Int): Unit = {
    val vby = src.height / numTasks + 1
    val splitPoints = (Range(0, src.height) by vby toList) ++ List(src.height)
    val splitRanges = splitPoints.zip (splitPoints.tail)  // List((0,41), (41,82), (82,123), (123,164), (164,200))
    val tasks = for {
      taskRange <- splitRanges
      t = task(blur(src, dst, taskRange._1, taskRange._2, radius))
    } yield t
    tasks.foreach(_.join())
  }

}
