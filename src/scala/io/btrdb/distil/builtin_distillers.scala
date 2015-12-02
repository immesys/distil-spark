package io.btrdb.distil
import io.btrdb.distil.dsl._
import io.btrdb._
import scala.collection.mutable

class MovingAverageDistiller extends Distiller {
  val version : Int
    = 1
  val maintainer : String
    = "Michael Andersen"
  val outputNames : Seq[String]
    = List("avg")
  val inputNames : Seq[String]
    = List("input")
  val kernelSizeNanos : Option[Long]
    = Some(1.minute + 500.millisecond)
  val timeBaseAlignment : Option[BTrDBAlignMethod]
    = Some(BTRDB_ALIGN_120HZ_SNAP_DENSE)
  val dropNaNs : Boolean
    = false

  val ptsInWindow = 60*120

  override def kernel(range : (Long, Long),
               rangeStartIdx : Int,
               input : IndexedSeq[(Long, IndexedSeq[Double])],
               inputMap : Map[String, Int],
               output : Map[String, mutable.ArrayBuffer[(Long, Double)]],
               db : BTrDB) = {

    //Our inputs are dense, so we can just use indexes. we will use 120 indexes
    val out = output("avg")
    //println(s"kernel started with input sz: ${input.size} and rangestartidx=${rangeStartIdx}")
    if (rangeStartIdx > 0) {

    }
    //Bootstrap
    var sum = input.view
      .slice(rangeStartIdx-(ptsInWindow+1), rangeStartIdx-1)
      .map(x=>x._2(0))
      .filterNot(_.isNaN)
      .foldLeft(0.0)(_+_)

    var count = input.view
      .slice(rangeStartIdx-(ptsInWindow+1), rangeStartIdx-1)
      .map(x=>x._2(0))
      .filterNot(_.isNaN)
      .size

    for (i <- rangeStartIdx until input.size) {
      if (!input(i-(ptsInWindow+1))._2(0).isNaN) {
        count -= 1
        sum -= input(i-(ptsInWindow+1))._2(0)
      }

      if (!input(i)._2(0).isNaN) {
        count += 1
        sum += input(i)._2(0)
      }

      if (count > 0)
        out += ((input(i)._1 , sum/count))


    }
    deleteAllRanges(range)(db)
  }
}

class DoublerDistiller extends Distiller {
  import io.btrdb.distil.dsl._

  val version : Int
    = 1
  val maintainer : String
    = "Michael Andersen"
  val outputNames : Seq[String]
    = List("output")
  val inputNames : Seq[String]
    = List("input")
  val kernelSizeNanos : Option[Long]
    = None
  val timeBaseAlignment : Option[BTrDBAlignMethod]
    = None
  val dropNaNs : Boolean
    = false

  override def kernel(range : (Long, Long),
               rangeStartIdx : Int,
               input : IndexedSeq[(Long, IndexedSeq[Double])],
               inputMap : Map[String, Int],
               output : Map[String, mutable.ArrayBuffer[(Long, Double)]],
               db : BTrDB) = {
    val out = output("output")
    for (i <- rangeStartIdx until input.size) {
      out += ((input(i)._1 , input(i)._2(0)*2))
    }
    deleteAllRanges(range)(db)
  }
}

class FrequencyDistiller extends Distiller {
  val version : Int
    = 1
  val maintainer : String
    = "Michael Andersen"
  val outputNames : Seq[String]
    = List("freq_1s", "freq_c37")
  val inputNames : Seq[String]
    = List("angle")
  val kernelSizeNanos : Option[Long]
    = Some(1.second + 500.millisecond)
  val timeBaseAlignment : Option[BTrDBAlignMethod]
    = Some(BTRDB_ALIGN_120HZ_SNAP_DENSE)
  val dropNaNs : Boolean
    = false

  def angwrap(d:Double) : Double = {
    if (d > 180) d-360
    else if (d < -180) d +360
    else d
  }
  override def kernel(range : (Long, Long),
               rangeStartIdx : Int,
               input : IndexedSeq[(Long, IndexedSeq[Double])],
               inputMap : Map[String, Int],
               output : Map[String, mutable.ArrayBuffer[(Long, Double)]],
               db : BTrDB) = {

    //Our inputs are dense, so we can just use indexes. we will use 120 indexes
    println("rangestartidx is: ", rangeStartIdx)
    println("range is: ", range)
    println("input0 is: ", input(0))
    val out1s = output("freq_1s")
    val outc37 = output("freq_c37")
    for (i <- rangeStartIdx until input.size) {
      val time = input(i)._1

      val v1s = angwrap(input(i)._2(0) - input(i-120)._2(0))/360.0 + 60
      if (!v1s.isNaN)
        out1s += ((time, v1s))

      val p1 = input(i)._2(0)
      val p2 = input(i-1)._2(0)
      val p3 = input(i-2)._2(0)
      val p4 = input(i-3)._2(0)
      val v1 = angwrap(p1-p2)
      val v2 = angwrap(p2-p3)
      val v3 = angwrap(p3-p4)
      val c37 = 60.0 + (((6.0*(v1)+3.0*(v2)+1.0*(v3))/10)*((120.0/360.0)))
      if (!c37.isNaN)
        outc37 += ((time, c37))
    }
    deleteAllRanges(range)(db)
  }
}

class SlidingQuartileDistiller extends Distiller {
  val version : Int
    = 1
  val maintainer : String
    = "Michael Andersen"
  val outputNames : Seq[String]
    = List("1st_quartile","median","3rd_quartile")
  val inputNames : Seq[String]
    = List("angle")
  val kernelSizeNanos : Option[Long]
    = Some(15.minute + 500.millisecond)
  val timeBaseAlignment : Option[BTrDBAlignMethod]
    = None
  val dropNaNs : Boolean
    = false

  def getQuartiles(input : IndexedSeq[(Long, IndexedSeq[Double])], start : Int, end : Int)
    : (Double, Double, Double, Boolean) =
  {
      val dat = input.slice(start, end).map(x=>x._2(0)).toArray
      scala.util.Sorting.quickSort(dat)
      if (dat.size > 4) {
        val q1i = dat.size / 4
        val q2i = dat.size / 2
        val q3i = q2i + q1i
        (dat(q1i), dat(q2i), dat(q3i), true)
      } else {
        (0,0,0, false)
      }

  }

  override def kernel(range : (Long, Long),
               rangeStartIdx : Int,
               input : IndexedSeq[(Long, IndexedSeq[Double])],
               inputMap : Map[String, Int],
               output : Map[String, mutable.ArrayBuffer[(Long, Double)]],
               db : BTrDB) = {
    var ei = rangeStartIdx
    var si = 0
    val out1q = output("1st_quartile")
    val out2q = output("median")
    val out3q = output("3rd_quartile")
    while (ei < input.size) {
      while (input(si)._1 < input(ei)._1 - 15.minute) si += 1
      val t = input(ei)._1
      val (q1, q2, q3, ok) = getQuartiles(input, si, ei)
      if (ok) {
        out1q += ((t,q1))
        out2q += ((t,q2))
        out3q += ((t,q3))
      }
      ei += 1
    }
    deleteAllRanges(range)(db)
  }
}
