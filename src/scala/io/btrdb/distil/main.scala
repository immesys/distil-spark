package io.btrdb

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._
import scala.collection.mutable
import io.btrdb._
import scala.collection.immutable

package object distil {
  def alignIterTo120HzUsingSnapClosest(vz : Iterator[(Long, Double)])
    : Iterator[(Long, Double)] =
  {
    vz.map( v =>
    {
      val tsec = v._1 / 1000000000L
      val resid = v._1 - tsec*1000000000L
      val cyc = resid / 8333333
      ((tsec*1000000000L) +
      (if ((resid % 8333333) <= 4166666)
      {
        cyc*8333333
      }
      else
      {
        if (cyc == 119) //Actually the start of the next second
          1000000000L
        else
          (cyc+1)*8333333
      }), v._2)
    })
  }

  def alignIterTo120HzUsingFloor(vz : Iterator[(Long, Double)])
    : Iterator[(Long, Double)] =
  {
    vz.map( v =>
    {
      val tsec = v._1 / 1000000000L
      val resid = v._1 - tsec*1000000000L
      val cyc = resid / 8333333
      ((tsec*1000000000L) + cyc*8333333, v._2)
    })
  }

  implicit class DistilSingleRDD(val rdd : RDD[(Long, Double)]) extends AnyVal
  {
    def alignTo120HzUsingSnapClosest()
      : RDD[(Long, Double)] =
    {
      rdd.mapPartitions((vz : Iterator[(Long, Double)]) =>
      {
        alignIterTo120HzUsingSnapClosest(vz)
      })
    }
    def alignTo120HzUsingFloor()
      : RDD[(Long, Double)] =
    {
      rdd.mapPartitions((vz : Iterator[(Long, Double)]) =>
      {
        alignIterTo120HzUsingFloor(vz)
      })
    }
    /*
    def persistToBtrdb(stream : UUID)
    {
      //Note that we assume that partitions don't overlap
      //I believe this is correct, but it might not be
      rdd.foreachPartition((vz : Iterator[(Long, Double)])) =>
      {
        var b = new BTrDB("localhost", 4410)
        val dat = vz.toIndexedSeq
        val bracketStart = dat.view.map(_._1).min
        val bracketEnd = dat.view.map(_._1).max
        b.deleteValues(stream, bracketStart, bracketEnd)
        val stat = b.insertValues(stream, dat)
        if (stat != "OK")
          throw BTrDBException("Error: " + stat)
      })
    }
    */
  }
  implicit class DistilMultiRDD(val rdd : RDD[(Long, immutable.Seq[Double])])
  {
    def dropNaNs()
      : RDD[(Long, immutable.Seq[Double])] =
    {
      rdd.mapPartitions((vz : Iterator[(Long, immutable.Seq[Double])]) =>
      {
        vz.filter( x => !(x._2.exists( y => y.isNaN) ))
      })
    }

/*
    def persistToBtrdb(streams : Seq[UUID])
    {
      if (streams.size != rdd._2.size)
        throw new IllegalArgumentException("Wrong number of UUIDs")

      //Note that we assume that partitions don't overlap
      //I believe this is correct, but it might not be
      rdd.foreachPartition((vz : Iterator[(Long, immutable.Seq[Double])])) =>
      {
        var b = new BTrDB("localhost", 4410)
        val dat = vz.toIndexedSeq
        val bracketStart = dat.view.map(_._1).min
        val bracketEnd = dat.view.map(_._1).max
        streams.zipWithIndex.forEach(u =>
        {
          val stat = b.deleteValues(u._1, bracketStart, bracketEnd)
          if (stat != "OK")
            throw BTrDBException("Error: " + stat)
          val sdat = dat.map(rec => (rec._1, rec._2(u._2))).filter(!_._2.isNan)
          stat = b.insertValues(u._1, sdat)
          if (stat != "OK")
            throw BTrDBException("Error: " + stat)
        })
      })
    }
    */
  }

  implicit class DistilSparkContext(val sc : SparkContext) extends AnyVal
  {
    def BTRDB_LATEST_VER = 0

    def BTRDB_LATEST_VERS (n : Int) = (0 until n).map(_ => 0)

    def BTRDB_ALIGN_120HZ_SNAP = alignIterTo120HzUsingSnapClosest _

    def BTRDB_ALIGN_120HZ_FLOOR = alignIterTo120HzUsingFloor _

    def btrdbStream(stream : String, startTime : Long, endTime : Long, version : Long)
      : RDD[(Long, Double)] =
    {
      var chunks : mutable.Buffer[(Long, Long)] = mutable.Buffer()
      var t = startTime
      val chunkSize = 5*60*1000000000L //5 mins
      while (t < endTime) {
        var st = t
        t += chunkSize
        if (t > endTime) t = endTime
        val pair = (st,t)
        chunks += pair
      }
      sc.parallelize(chunks).mapPartitions( (vz : Iterator[(Long, Long)]) =>
      {
        var b = new BTrDB("localhost", 4410)
        vz.flatMap( v =>
        {
          var (stat, ver, it) = b.getRaw(stream, v._1, v._2, version)
          if (stat != "OK")
            throw BTrDBException("Error: "+stat)
          it.map(x => (x.t, x.v)).toIndexedSeq
        })
      })
    }

    def multipleBtrdbStreams(streams : immutable.Seq[String], startTime : Long, endTime : Long, versions : immutable.Seq[Long],
      resample : Option[Iterator[(Long, Double)] => Iterator[(Long, Double)]] = None)
      : RDD[(Long, immutable.Seq[Double])] =
    {

      var chunks : mutable.Buffer[(Long, Long)] = mutable.Buffer()
      var t = startTime
      val chunkSize = 5*60*1000000000L //5 mins
      while (t < endTime) {
        var st = t
        t += chunkSize
        if (t > endTime) t = endTime
        val pair = (st,t)
        chunks += pair
      }
      sc.parallelize(chunks).mapPartitions( (vz : Iterator[(Long, Long)] ) =>
      {
        var b = new BTrDB("localhost", 4410)
        vz.flatMap( v =>
        {
          var raw_ires = streams.zip(versions).map(s =>
          {
            var (stat, ver, it) = b.getRaw(s._1, v._1, v._2, s._2)
            it.map(x => (x.t, x.v)).toIndexedSeq
          })
          var ires = resample match
          {
            case Some(f) => raw_ires.map(_.iterator).map(f).map(_.toIndexedSeq)
            case None => raw_ires
          }
          var idxz = streams.map(_ => 0).toBuffer
          val rvlen = ires.map(_.size).max
          (0 until rvlen).map(_ =>
          {
            val ts = idxz.zipWithIndex.map( x => ires(x._1)(x._2)._1).min
            (ts, (0 until ires.size).map(i =>
            {
              if (idxz(i) >= ires(i).size || ires(i)(idxz(i))._1 != ts)
                Double.NaN
              else {
                idxz(i) = idxz(i) + 1
                ires(i)(idxz(i) - 1)._2
              }
            }))
          })
        })
      })
    }
  }
}
