package io.btrdb

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._
import scala.collection.mutable
import io.btrdb._
import scala.collection.immutable
import com.mongodb.casbah.Imports._

import scala.language.implicitConversions
import io.jvm.uuid._
import java.time._

package object distil {

  type BTrDBAlignMethod = (Option[(Long, Long)], Iterator[(Long, Double)]) => Iterator[(Long, Double)]

  case class BTrDBTimestamp (t : Long)

  object dsl {

    import DslImplementation._

    implicit def StringToTS (s : String) :BTrDBTimestamp = {
      try {
        val t = ZonedDateTime.parse(s)
        val i = t.toInstant()
        BTrDBTimestamp(i.getEpochSecond()*1000000000 + i.getNano().toLong)
      } catch {
        case e : java.time.format.DateTimeParseException => {
          val t = LocalDateTime.parse(s)
          val i = t.toInstant(ZoneOffset.UTC)
          BTrDBTimestamp(i.getEpochSecond()*1000000000 + i.getNano().toLong)
        }
      }
    }
    implicit def LongToTS (l : Long) : BTrDBTimestamp = BTrDBTimestamp(l)

    implicit class TimeSpans (val l : Long) {
      def minute = l*60*1000000000L
      def second = l*1000000000L
      def millisecond = l*1000000L
      def microsecond = l*1000L
      def hour = l*60*60*1000000000L
      def day = l*24*60*60*1000000000L
    }

    def $eq(field : String, rhs : String) : LiteralWhereExpression = {
      LiteralWhereExpression(field, rhs)
    }

    def $has(field : String) : HasWhereExpression = {
      HasWhereExpression(field)
    }

    def $regex(field : String, rhs : String) : RegexWhereExpression = {
      RegexWhereExpression(field, rhs)
    }

    implicit class AutoStringRegex(val s : String) {
      def =~ (rhs : String) : RegexWhereExpression = {
        RegexWhereExpression(s, rhs)
      }
      def === (rhs : String) : LiteralWhereExpression = {
        LiteralWhereExpression(s, rhs)
      }
    }

    val DROPNAN = DROPNAN_t()
    val ONE = selectorSingleDataType()
    val ALL = selectorMultipleDataType()
    val METADATA = metadataDataType()
    val UUID = "uuid"
    val INNER = "INNER"
    val OUTER = "OUTER"
    val TRUE = TrueWhereExpression()

    def SELECT (sel : selectorSingleDataType) (implicit sc : org.apache.spark.SparkContext) : Selector[Double, SingleResult] = {
        new Selector[Double, SingleResult](sc.BTRDB_MIN_TIME, sc.BTRDB_MAX_TIME, sel, Seq.empty[QueryModifier], None, None, false)
    }

    def SELECT (sel : selectorMultipleDataType) (implicit sc : org.apache.spark.SparkContext) : Selector[Double, MultipleResult] = {
        new Selector[Double, MultipleResult](sc.BTRDB_MIN_TIME, sc.BTRDB_MAX_TIME, sel, Seq.empty[QueryModifier], None, None, false)
    }

    def SELECT (sel : metadataDataType) (implicit sc : org.apache.spark.SparkContext) : Selector[Any, MetadataResult] = {
        new Selector[Any, MetadataResult](sc.BTRDB_MIN_TIME, sc.BTRDB_MAX_TIME, sel, Seq.empty[QueryModifier], None, None, false)
    }

    def RESOLVE (path : String) : Option[String] = {
      val res = SELECT(METADATA) WHERE "Path" =~ path
      if (res.isEmpty) {
        None
      } else {
        Some(res(0)("uuid"))
      }
    }

    def SET (tuples : (String, String)*): Setter = {
      new Setter(tuples)
    }

    def CREATE (path : String): Creator = {
      new Creator(path, Seq.empty[(String,String)])
    }

    def DROP (path : String) {
      metadataCollection.remove(MongoDBObject("Path" -> path))
    }

    def MATERIALIZE (path : String, identifier : String = "interactive") : Long = {
      io.btrdb.distil.Distiller.genericMaterialize(path, identifier)
    }

    def LISTLOCKED() : Seq[StreamObject] = {
      SELECT(METADATA) WHERE !("Locks/Materialize" === "UNLOCKED")
    }

    def UNLOCK(path : String) = {
      val ires = (SELECT(METADATA) WHERE "Path" === path)
      val instanceID = ires(0)("distil/instance")
      SELECT(METADATA) WHERE "distil/instance" === instanceID foreach (d => {
        SET ("Locks/Materialize" -> "UNLOCKED") WHERE "uuid" === d("uuid")
      })
    }

    def MKDISTILLATE (klass : String) : MkDistiller = {
      new MkDistiller(klass,
        Array[(String, String)](),
        Array[(String, String)](),
        Array[(String, String)](),
        Array[String]())
    }

    def BTRDB_ALIGN_120HZ_SNAP = alignIterTo120HzUsingSnapClosest _

    def BTRDB_ALIGN_120HZ_SNAP_DENSE = alignIterTo120HzUsingSnapClosestDense _

    def BTRDB_ALIGN_120HZ_FLOOR = alignIterTo120HzUsingFloor _

    def BTRDB_ALIGN_120HZ_FLOOR_DENSE = alignIterTo120HzUsingFloorDense _
  }

  lazy val loc_btrdb = new BTrDB(btrdbHost, 4410)
  def alignPtTo120HzUsingSnapClosest(v :(Long, Double)) : (Long, Double) = {
    val tsec = v._1 / 1000000000L
    val resid = v._1 - tsec*1000000000L
    val cyc = (resid + 4166666) / 8333333
    ((tsec*1000000000L) +
    ( if (cyc == 120) //Actually the start of the next second
        1000000000L
      else
        cyc*8333333L
    ), v._2)
  }
  def alignIterTo120HzUsingSnapClosest(range : Option[(Long, Long)], vz : Iterator[(Long, Double)])
    : Iterator[(Long, Double)] =
  {
    vz.map( v =>
    {
      alignPtTo120HzUsingSnapClosest(v)
    })
  }

  class DenseIterator(start: Long, end: Long, vz : Iterator[(Long, Double)], interval : Long, roundwhen : Long)
    extends Iterator[(Long, Double)] {
    //println(s"DenseIterator constructed: s=$start e=$end i=$interval rw=$roundwhen")
    var head : Option[(Long, Double)] = Some(vz.next)
    var now : Long = start
    def hasNext() : Boolean = {
      head match {
        case Some(x) => x._1 < end
        case None => now < end
      }
    }
    def next() : (Long, Double) = {
      //This is not quite right...
      if (now > end || (!head.isEmpty && head.get._1 >= end)) {
        throw new RuntimeException(s"huh: now=$now head=$head end=$end")
        /*
        if (head.isEmpty)
          throw new RuntimeException(s"huh: now=$now head=$head end=$end")
        else {
          val rv = head.get
          head = (if (vz.hasNext) Some(vz.next()) else None)
          return rv
        }
        */
      }

      var incnow = false
      val rv = head match {
        case Some(v) =>
          if (v._1 <= now) {
            while (!head.isEmpty && head.get._1 <= now) {
              head = (if (vz.hasNext) Some(vz.next()) else None)
              //println(s"for $this advanced iter to $head")
            }
            v
          } else {
            incnow = true
            (now, Double.NaN)
          }
        case None =>
          incnow = true
          (now, Double.NaN)
      }
      now += interval
      val delta = now % 1000000000L
      if (delta < roundwhen) {
        now -= delta
      } else if (delta > 1000000000L- roundwhen) {
        now += 1000000000L - delta
      }
      //println(s"now is $now")
      rv
    }
  }

  def alignIterTo120HzUsingSnapClosestDense(range : Option[(Long, Long)], vz : Iterator[(Long, Double)])
    : Iterator[(Long, Double)] =
  {
    val rng = range.getOrElse(throw BTrDBException("Dense align with no bounds?"))
    val aligned = alignIterTo120HzUsingSnapClosest(None, vz)
    new DenseIterator(alignPtTo120HzUsingSnapClosest(rng._1, 0)._1, rng._2, aligned, 8333333, 1000000)
  }

  def alignIterTo120HzUsingFloor(range : Option[(Long, Long)], vz : Iterator[(Long, Double)])
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

  def alignIterTo120HzUsingFloorDense(range : Option[(Long, Long)], vz : Iterator[(Long, Double)])
    : Iterator[(Long, Double)] =
  {
    val rng = range.getOrElse(throw BTrDBException("Dense align with no bounds?"))
    val aligned = alignIterTo120HzUsingFloor(None, vz)
    new DenseIterator(rng._1, rng._2, aligned, 8333333, 1000000)
  }

  //This function can be called from a remote context, it doesn't use sc
  def multipleBtrdbStreamLocal(conn : BTrDB, streams : immutable.Seq[String], range : (Long, Long), versions : immutable.Seq[Long],
    align : Option[BTrDBAlignMethod] = None)
    : IndexedSeq[(Long, immutable.IndexedSeq[Double])] =
  {
    var raw_ires = streams.zip(versions).map(s =>
    {
      var (stat, ver, it) = conn.getRaw(s._1, range._1, range._2, s._2)
      if (stat != "OK")
          throw BTrDBException("Error: "+stat)
      it.map(x => (x.t, x.v)).toIndexedSeq
    })
    var ires = align match
    {
      case Some(f) => raw_ires.map(_.iterator).map(v => f(Some(range), v)).map(_.toIndexedSeq)
      case None => raw_ires
    }
    //ires is now Seq[ Iter (time, value) ]
    var idxz = streams.map(_ => 0).toBuffer
    val rvlen = ires.map(_.size).max
    val rv = (0 until rvlen).map(_ =>
    {
      //Zip into (idx, streamnumber) filter out where the idx is greater than the stream length
      //map that onto just the timestamp at the index, and get the minimum
      val ts = idxz.zipWithIndex.filter( x => x._1 < ires(x._2).size ).map( x => ires(x._2)(x._1)._1).min
      (ts, (0 until ires.size).map(i =>
      {
        if (idxz(i) >= ires(i).size || ires(i)(idxz(i))._1 != ts) {
          //idxz(i) = idxz(i) + 1
          Double.NaN
        } else {
          idxz(i) = idxz(i) + 1
          ires(i)(idxz(i) - 1)._2
        }
      }))
    })
    rv
  }

  implicit class DistilSingleRDD(val rdd : RDD[(Long, Double)]) extends AnyVal
  {
    def alignTo120HzUsingSnapClosest()
      : RDD[(Long, Double)] =
    {
      rdd.mapPartitions((vz : Iterator[(Long, Double)]) =>
      {
        alignIterTo120HzUsingSnapClosest(None, vz)
      })
    }
    def alignTo120HzUsingFloor()
      : RDD[(Long, Double)] =
    {
      rdd.mapPartitions((vz : Iterator[(Long, Double)]) =>
      {
        alignIterTo120HzUsingFloor(None, vz)
      })
    }
    def persistToBtrdb(stream : String)
    {
      //Note that we assume that partitions don't overlap
      //I believe this is correct, but it might not be
      val targetHost = btrdbHost
      rdd.foreachPartition((vz : Iterator[(Long, Double)]) =>
      {
        if (vz.hasNext) {
          var b = getBTrDB(targetHost)
          //val bracketStart = vz.view.map(_._1).min
          //val bracketEnd = dat.view.map(_._1).max
          //b.deleteValues(stream, bracketStart, bracketEnd)
          val stat = b.insertValues(stream, vz.map(t => RawTuple(t._1, t._2)))
          b.close()
          if (stat != "OK")
            throw BTrDBException("Error: " + stat)
        }
      })
    }
  }
  implicit class DistilMultiRDD(val rdd : RDD[(Long, immutable.IndexedSeq[Double])])
  {
    def dropNaNs()
      : RDD[(Long, immutable.IndexedSeq[Double])] =
    {
      rdd.mapPartitions((vz : Iterator[(Long, immutable.IndexedSeq[Double])]) =>
      {
        vz.filter( x => !(x._2.exists( y => y.isNaN) ))
      })
    }

    def persistToBtrdb(streams : Seq[String])
    {
      //Note that we assume that partitions don't overlap
      //I believe this is correct, but it might not be
      val targetHost = btrdbHost
      rdd.foreachPartition((vz : Iterator[(Long, immutable.IndexedSeq[Double])]) =>
      {
        val dat = vz.toIndexedSeq
        if (dat.size > 0) {
          var b = getBTrDB(targetHost)
          val bracketStart = dat.view.map(_._1).min
          val bracketEnd = dat.view.map(_._1).max
          val rv = streams.zipWithIndex.foreach(u =>
          {
            var stat = b.deleteValues(u._1, bracketStart, bracketEnd)
            if (stat != "OK")
              throw BTrDBException("Error: " + stat)
            val sdat = dat.view.map(rec => (rec._1, rec._2(u._2))).filter(!_._2.isNaN).map( t => RawTuple(t._1, t._2)).iterator
            stat = b.insertValues(u._1, sdat)
            if (stat != "OK")
              throw BTrDBException("Error: " + stat)
          })
          b.close()
          rv
        }
      })
    }
  }

  implicit class DistilSparkContext(val sc : SparkContext) extends AnyVal
  {
    def BTRDB_LATEST_VER = 0L

    def BTRDB_MIN_TIME = -1152921504606846976L
    def BTRDB_MAX_TIME = 3458764513820540928L
    def BTRDB_LATEST_VERS (n : Int) = (0 until n).map(_ => 0L)

    def BTRDB_ALIGN_120HZ_SNAP = alignIterTo120HzUsingSnapClosest _

    def BTRDB_ALIGN_120HZ_SNAP_DENSE = alignIterTo120HzUsingSnapClosestDense _

    def BTRDB_ALIGN_120HZ_FLOOR = alignIterTo120HzUsingFloor _

    def BTRDB_ALIGN_120HZ_FLOOR_DENSE = alignIterTo120HzUsingFloorDense _

    def initDistil(btrdb : String, mongo : String)
    {
      distilInitialized = true
      implicitSparkContext = sc
      btrdbHost = btrdb
      mongoHost = mongo
    }
    private def checkInit() {
      if (!distilInitialized) throw new RuntimeException("DISTIL not initialized")
    }
    def btrdbStream(stream : String, startTime : Long, endTime : Long, version : Long)
      : RDD[(Long, Double)] =
    {
      checkInit()
      //TODO add a bracket here to adjust startTime and endTime
      var chunks : mutable.Buffer[(Long, Long)] = mutable.Buffer()
      var t = startTime
      val chunkSize = 15*60*1000000000L
      while (t < endTime) {
        var st = t
        t += chunkSize
        if (t > endTime) t = endTime
        val pair = (st,t)
        chunks += pair
      }
      val targetHost = btrdbHost
      sc.parallelize(chunks).mapPartitions( (vz : Iterator[(Long, Long)]) =>
      {
        var b = getBTrDB(targetHost)
        val rv = vz.flatMap( v =>
        {
          var (stat, ver, it) = b.getRaw(stream, v._1, v._2, version)
          if (stat != "OK")
            throw BTrDBException("Error: "+stat)
          it.map(x => (x.t, x.v)).toIndexedSeq
        })
        new KillConnIterator(rv, b)
      })
    }

    def openDefaultBTrDB() = getBTrDB(btrdbHost)

    def deleteBTrdbRange(stream : String, startTime : Long, endTime : Long) : Unit = {
      checkInit()
      var stat = loc_btrdb.deleteValues(stream, startTime, endTime)
      if (stat != "OK")
        throw BTrDBException("Error: " + stat)
    }

    //TODO replace this with less dumb method
    //TODO for large queries, use statistical prequery to plan chunks
    //TODO for default start/end use bracket query to narrow to real range
    private def genChunks(stream : String, startTime : Long, endTime : Long, chunkMultiple : Long)
        : mutable.Buffer[(Long, Long)] = {
      var chunks : mutable.Buffer[(Long, Long)] = mutable.Buffer()
      var t = startTime
      var chunkSize = 10*60*1000000000L //10 mins
      chunkSize /= chunkMultiple
      chunkSize *= chunkMultiple
      while (t < endTime) {
        var st = t
        t += chunkSize
        if (t > endTime) t = endTime
        val pair = (st,t)
        chunks += pair
      }
      chunks
    }

    private def genWindowChunks(startTime : Long, endTime : Long, chunkMultiple : Long)
        : mutable.Buffer[(Long, Long)] = {
      var chunks : mutable.Buffer[(Long, Long)] = mutable.Buffer()
      var t = startTime
      var chunkSize = (endTime - startTime) / sc.defaultParallelism
      chunkSize /= chunkMultiple
      chunkSize *= chunkMultiple
      while (t < endTime) {
        var st = t
        t += chunkSize
        if (t > endTime) t = endTime
        val pair = (st,t)
        chunks += pair
      }
      chunks
    }

    def btrdbWindowStream(stream : String, startTime : Long, endTime : Long, depth : Int, window : Long, version : Long)
        : RDD[(Long, StatTuple)] = {
        checkInit()
        val targetHost = btrdbHost
        var chunks = genWindowChunks(startTime, endTime, window)
        sc.parallelize(chunks).mapPartitions( (vz : Iterator[(Long, Long)]) => {
            var b = getBTrDB(targetHost)
            val rv = vz.flatMap( v =>
            {
                var (stat, ver, it) = b.getWindow(stream, window, v._1, v._2, depth, version)
                if (stat != "OK")
                    throw BTrDBException("Error: "+stat)
                it.map(x => (x.t, x)).toIndexedSeq
            })
            new KillConnIterator(rv, b)
        })
    }
    def btrdbStatisticalStream(stream : String, startTime : Long, endTime : Long, windowExponent : Int, version : Long)
        : RDD[(Long, StatTuple)] = {
        checkInit()
        val targetHost = btrdbHost
        val realStartTime = startTime & ~((1<<windowExponent)-1)
        val realEndTime = endTime & ~((1<<windowExponent)-1)
        var chunks = genWindowChunks(realStartTime, realEndTime, 1<<windowExponent)
        sc.parallelize(chunks).mapPartitions( (vz : Iterator[(Long, Long)]) => {
            var b = getBTrDB(targetHost)
            val rv = vz.flatMap( v =>
            {
                var (stat, ver, it) = b.getStatistical(stream, windowExponent, v._1, v._2, version)
                if (stat != "OK")
                    throw BTrDBException("Error: "+stat)
                it.map(x => (x.t, x)).toIndexedSeq
            })
            new KillConnIterator(rv, b)
        })
    }

    def multipleBtrdbStatisticalStreams(streams : immutable.Seq[String], startTime : Long, endTime : Long, windowExponent : Int, versions : immutable.Seq[Long])
      : RDD[(Long, immutable.Seq[StatTuple])] =
    {
      checkInit()
      val targetHost = btrdbHost
      val realStartTime = startTime & ~((1<<windowExponent)-1)
      val realEndTime = endTime & ~((1<<windowExponent)-1)
      var chunks = genWindowChunks(realStartTime, realEndTime, 1<<windowExponent)
      sc.parallelize(chunks).mapPartitions( (vz : Iterator[(Long, Long)]) => {
          var b = getBTrDB(targetHost)
          val rv = vz.flatMap( v =>
          {
              var ires = streams.zip(versions).map(s =>
              {
                var (stat, ver, it) = b.getStatistical(s._1, windowExponent, v._1, v._2, s._2)
                if (stat != "OK")
                    throw BTrDBException("Error: "+stat)
                it.toIndexedSeq
              })
              var idxz = streams.map(_ => 0).toBuffer
              val rvlen = ires.map(_.size).max
              (0 until rvlen).map(_ =>
              {
                //Zip into (idx, streamnumber) filter out where the idx is greater than the stream length
                //map that onto just the timestamp at the index, and get the minimum
                val ts = idxz.zipWithIndex.filter( x => x._1 < ires(x._2).size ).map( x => ires(x._2)(x._1).t).min
                (ts, (0 until ires.size).map(i =>
                {
                  if (idxz(i) >= ires(i).size || ires(i)(idxz(i)).t != ts) {
                    //idxz(i) = idxz(i) + 1
                    StatTuple(ts, 0, 0, 0, 0) //count=0 implies invalid
                  } else {
                    idxz(i) = idxz(i) + 1
                    ires(i)(idxz(i) - 1)
                  }
                }))
              })
          })
          new KillConnIterator(rv, b)
      })
    }
    def multipleBtrdbWindowStreams(streams : immutable.Seq[String], startTime : Long, endTime : Long, depth : Int, window : Long, versions : immutable.Seq[Long])
      : RDD[(Long, immutable.Seq[StatTuple])] =
    {
      checkInit()
      val targetHost = btrdbHost
      var chunks = genWindowChunks(startTime, endTime, window)
      sc.parallelize(chunks).mapPartitions( (vz : Iterator[(Long, Long)]) => {
          var b = getBTrDB(targetHost)
          val rv = vz.flatMap( v =>
          {
              var ires = streams.zip(versions).map(s =>
              {
                var (stat, ver, it) = b.getWindow(s._1, window, v._1, v._2, depth, s._2)
                if (stat != "OK")
                    throw BTrDBException("Error: "+stat)
                it.toIndexedSeq
              })
              var idxz = streams.map(_ => 0).toBuffer
              val rvlen = ires.map(_.size).max
              (0 until rvlen).map(_ =>
              {
                //Zip into (idx, streamnumber) filter out where the idx is greater than the stream length
                //map that onto just the timestamp at the index, and get the minimum
                val ts = idxz.zipWithIndex.filter( x => x._1 < ires(x._2).size ).map( x => ires(x._2)(x._1).t).min
                (ts, (0 until ires.size).map(i =>
                {
                  if (idxz(i) >= ires(i).size || ires(i)(idxz(i)).t != ts) {
                    //idxz(i) = idxz(i) + 1
                    StatTuple(ts, 0, 0, 0, 0) //count=0 implies invalid
                  } else {
                    idxz(i) = idxz(i) + 1
                    ires(i)(idxz(i) - 1)
                  }
                }))
              })
          })
          new KillConnIterator(rv, b)
      })
    }

    def multipleBtrdbStreams(streams : immutable.Seq[String], startTime : Long, endTime : Long, versions : immutable.Seq[Long],
      align : Option[BTrDBAlignMethod] = None)
      : RDD[(Long, immutable.IndexedSeq[Double])] =
    {
      checkInit()
      val targetHost = btrdbHost
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
        var b = getBTrDB(targetHost)
        val rv = vz.flatMap( v =>
        {
          multipleBtrdbStreamLocal(b, streams, v, versions, align)
        })
        new KillConnIterator(rv, b)
      })
    }
  } //end class
  private def getBTrDB(targetHost : String) : BTrDB = {
    new BTrDB(targetHost, 4410)
  }

  private class KillConnIterator[T](iter : Iterator[T], conn : BTrDB) extends Iterator[T] {
    def hasNext() : Boolean = {
      iter.hasNext
    }
    def next() : T = {
      val v = iter.next()
      if (!iter.hasNext) {
        conn.close()
      }
      v
    }
    if (!hasNext()) {
      conn.close()
    }
  }

  private var distilInitialized = false
  implicit var implicitSparkContext : org.apache.spark.SparkContext = null
  //In package object ensure Mongo connection
  private lazy val mongoClient: MongoClient = MongoClient(mongoHost)
  private lazy val mongoDB = mongoClient("qdf")
  lazy val metadataCollection = mongoDB("metadata2")
  var btrdbHost = "localhost"
  private var mongoHost = "localhost"
}
