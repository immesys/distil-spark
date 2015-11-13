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

object DslImplementation {
  import distil._



  type BTrDBAlignMethod = (Iterator[(Long, Double)]) => Iterator[(Long, Double)]

  case class selectorSingleDataType()

  case class selectorMultipleDataType()

  case class metadataDataType()

  case class selectorAlignedDataType()

  val ALIGNED = selectorAlignedDataType()

  import distil.dsl._

  case class MERGE_t() extends QueryModifier

  case class DROPNAN_t() extends QueryModifier

  class WhereExpression() {
    def && (w : WhereExpression) : AndWhereExpression = {
      AndWhereExpression(this, w)
    }
    def || (w : WhereExpression) : OrWhereExpression = {
      OrWhereExpression(this, w)
    }
    override def toString : String = {
      "DEFAULT CASE"
    }
    def generate() : MongoDBObject = {
      throw new UnsupportedOperationException("Should not call parent generate")
    }
  }
  case class HasWhereExpression(field : String) extends WhereExpression {
    override def toString : String = {
      "( HAS , "+field + ")"
    }
    override def generate() : MongoDBObject = {
      return MongoDBObject("$exists" -> MongoDBObject(field -> true))
    }
  }
  case class OrWhereExpression(lhs : WhereExpression, rhs : WhereExpression) extends WhereExpression {
    override def toString : String = {
      "("+lhs.toString+", OR, "+rhs.toString+")"
    }
    override def generate() : MongoDBObject = {
      return MongoDBObject("$or" -> Array(lhs.generate(),rhs.generate()))
    }
  }
  case class TrueWhereExpression() extends WhereExpression {
    override def toString : String = {
      "TRUE"
    }
    override def generate() : MongoDBObject = {
      return MongoDBObject()
    }
  }
  case class RegexWhereExpression(field : String, pattern : String) extends WhereExpression {
    override def toString : String = {
      "("+field+", RLIKE, "+pattern+")"
    }
    override def generate() : MongoDBObject = {
      return MongoDBObject(field -> MongoDBObject("$regex" -> pattern))
    }
  }
  case class AndWhereExpression(lhs : WhereExpression, rhs : WhereExpression) extends WhereExpression {
    override def toString : String = {
      "("+lhs.toString+", AND, "+rhs.toString+")"
    }
    override def generate() : MongoDBObject = {
      return MongoDBObject("$and" -> Array(lhs,rhs))
    }
  }
  case class LiteralWhereExpression(field : String, rhs : String) extends WhereExpression {
    override def toString : String = {
      "("+field+", EQ, "+rhs+")"
    }
    override def generate() : MongoDBObject = {
      return MongoDBObject(field -> rhs)
    }
  }

  import scala.collection.mutable.HashMap

  class StreamObject (elems : Tuple2[String, String]*) extends HashMap[String, String] {
    this ++= elems
  }

  class QueryModifier

  class Setter (tuples : Seq[(String, String)]) {
    def WHERE (w : WhereExpression) (implicit sc : org.apache.spark.SparkContext) {
      execute(w, sc)
    }

    def execute (w : WhereExpression, sc : org.apache.spark.SparkContext) {
      var query = w.generate()
      metadataCollection.findAndModify(query, MongoDBObject("$set"->MongoDBObject(tuples:_*)))
    }
  }
  case class StreamExistsException(path : String) extends Exception("Stream exists: "+path)

  class Creator (path : String, meta : Seq[(String, String)]) {
    def WITH (m : (String, String)*) : Creator = {
      new Creator(path, meta ++ m)
    }
    private[this] def convObj(m : mutable.Map[String, Any]) : MongoDBObject  = {
      val builder = MongoDBObject.newBuilder
      m.foreach(kv => {
        kv._2 match {
          case x : String => builder += (kv._1 -> x)
          case x => builder += (kv._1 -> convObj(x.asInstanceOf[mutable.Map[String, Any]]))
        }
      })
      builder.result
    }
    private[this] def buildObj(m : mutable.Map[String, String]) : MongoDBObject  = {
      var m2 : mutable.Map[String, Any] = mutable.Map()
      //Please shoot me. Never tell anyone I wrote this
      for (k <- m.keys) {
        var curmap = m2
        var pfx = k.split("/")
        var tail = pfx(pfx.size-1)
        pfx = pfx.slice(0, pfx.size-1)
        for (kk <- pfx) {
          var nxtmap = curmap.getOrElse(kk, mutable.Map.empty[String, Any])
          curmap(kk) = nxtmap
          curmap = nxtmap.asInstanceOf[mutable.Map[String, Any]]
        }
        curmap(tail) = m(k)
      }
      convObj(m2)
    }
    def FROM (rdd : RDD[(Long, Double)]): String = {
      val dat : mutable.Map[String, String] = mutable.Map()
      for (t <- meta) dat(t._1) = t._2
      val ks = dat.keySet
      if (!ks.contains("Properties/Timezone")) {
        dat("Properties/Timezone") = "America/Los_Angeles"
      }
      if (!ks.contains("Properties/UnitofMeasure")) {
        dat("Properties/UnitofMeasure") = "Unspecified"
      }
      dat("Properties/UnitofTime") = "ns"
      dat("Properties/ReadingType") = "double"
      dat("Path") = path
      if (!ks.contains("Metadata/SourceName")) {
        dat("Metadata/SourceName") = "BTS"
      }
      if (!ks.contains("uuid")) {
        dat("uuid") = io.jvm.uuid.UUID.random.toString
      } else {
        //Lazy as hell validation
        dat("uuid") = io.jvm.uuid.UUID(dat("uuid")).toString
      }
      if (metadataCollection.find(MongoDBObject("Path" -> path)).count() != 0) {
        throw StreamExistsException(path)
      }
      var obj = buildObj(dat)
      metadataCollection.insert(obj)
      rdd.persistToBtrdb(dat("uuid"))
      dat("uuid")
    }
  }
/*
  class Deleter (stream : String) {
    def BEFORE (t : BTrDBTimestamp) : Selector = {
      sc
        new Selector(startT, t, fields, mods, align, window)
    }
    def AFTER (t : BTrDBTimestamp) : Selector = {
        new Selector(t, endT, fields, mods, align, window)
    }
    def BETWEEN (s : BTrDBTimestamp, e : BTrDBTimestamp) : Selector = {
        new Selector(s, e, fields, mods, align, window)
    }
  }
  */
  class Selector[T] (startT : BTrDBTimestamp, endT : BTrDBTimestamp,
                  sel : Any, mods : Seq[QueryModifier],
                  align : Option[BTrDBAlignMethod], window : Option[String]) {
    def WHERE (w : WhereExpression) (implicit sc : org.apache.spark.SparkContext) : T = {
        execute(w, sc)
    }
    def WITH (w : QueryModifier*) : Selector[T] = {
        new Selector[T](startT, endT, sel, mods ++ w, align, window)
    }
    def BEFORE (t : BTrDBTimestamp) : Selector[T] = {
        new Selector[T](startT, t, sel, mods, align, window)
    }

    def JOIN (s : String) : Selector[(Seq[StreamObject], RDD[(Long, Seq[Double])])] = {
        s match {
          case INNER => new Selector[(Seq[StreamObject], RDD[(Long, Seq[Double])])](startT, endT, ALIGNED,
            mods ++ Array(DROPNAN), align, window)
          case OUTER => new Selector[(Seq[StreamObject], RDD[(Long, Seq[Double])])](startT, endT, ALIGNED,
            mods, align, window)
        }
    }
    def WINDOW (s : String) : Selector[T] = {
        new Selector[T](startT, endT, sel, mods, align, Some(s))
    }

    def ALIGN (method : BTrDBAlignMethod) : Selector[T] = {
      new Selector[T](startT, endT, sel, mods, Some(method), window)
    }

    def AFTER (t : BTrDBTimestamp) : Selector[T] = {
        new Selector[T](t, endT, sel, mods, align, window)
    }

    def BETWEEN (s : BTrDBTimestamp, e : BTrDBTimestamp) : Selector[T] = {
        new Selector[T](s, e, sel, mods, align, window)
    }

    private[this] def execute(w : WhereExpression, sc : org.apache.spark.SparkContext) : T = {

      //First step: grab all the metadata that matches
      var query = w.generate()
      var res = metadataCollection.find(query)

      def add (d : MongoDBObject, pfx : String, target : mutable.Map[String,String]) {
        for (k <- d.keys) {
          d(k) match {
            case s : String => {
              target += (if (pfx == "") k -> s else (pfx+"/"+k) -> s)
            }
            case dd : com.mongodb.BasicDBObject => {
              add(dd, pfx + "/" + k, target)
            }
            case default => {
            }
          }
        }
      }

      var streamMetadatas = (for (d <- res) yield {
        var somap : mutable.Map[String, String] = mutable.Map()
        add (d, "", somap)
        new StreamObject(somap.toSeq:_*)
      } ).toIndexedSeq

      var bDropNaN = mods contains DROPNAN

      sel match {
        case ONE => {
          var so = streamMetadatas(0)
          var dat = sc.btrdbStream(so("uuid"), startT.t, endT.t, sc.BTRDB_LATEST_VER)
          (so, dat).asInstanceOf[T]
        }
        case ALL => {
          (streamMetadatas map (so => {
            (so, sc.btrdbStream(so("uuid"), startT.t, endT.t, sc.BTRDB_LATEST_VER))
          })).asInstanceOf[T]
        }
        case METADATA => {
          streamMetadatas.asInstanceOf[T]
        }
        case ALIGNED => {
          var rv = sc.multipleBtrdbStreams((streamMetadatas map (x => x("uuid"))).toIndexedSeq, startT.t, endT.t, (streamMetadatas map (_=> sc.BTRDB_LATEST_VER)).toIndexedSeq, align)
          if (mods contains DROPNAN)
            rv = rv.dropNaNs()
          (streamMetadatas, rv).asInstanceOf[T]
        }
      }
    }
  }
}

package object distil {

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
    implicit def LongToTS (l : Long) = BTrDBTimestamp(l)

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
    }

    val DROPNAN = DROPNAN_t()
    val ONE = selectorSingleDataType()
    val ALL = selectorMultipleDataType()
    val METADATA = metadataDataType()
    val UUID = "uuid"
    val INNER = "INNER"
    val OUTER = "OUTER"
    val TRUE = TrueWhereExpression()

    def SELECT (sel : selectorSingleDataType) (implicit sc : org.apache.spark.SparkContext) : Selector[(StreamObject, RDD[(Long, Double)])] = {
        new Selector[(StreamObject, RDD[(Long, Double)])](sc.BTRDB_MIN_TIME, sc.BTRDB_MAX_TIME, sel, Seq.empty[QueryModifier], None, None)
    }

    def SELECT (sel : selectorMultipleDataType) (implicit sc : org.apache.spark.SparkContext) : Selector[Seq[(StreamObject, RDD[(Long, Double)])]] = {
        new Selector[Seq[(StreamObject, RDD[(Long, Double)])]](sc.BTRDB_MIN_TIME, sc.BTRDB_MAX_TIME, sel, Seq.empty[QueryModifier], None, None)
    }

    def SELECT (sel : metadataDataType) (implicit sc : org.apache.spark.SparkContext) : Selector[Seq[StreamObject]] = {
        new Selector[Seq[StreamObject]](sc.BTRDB_MIN_TIME, sc.BTRDB_MAX_TIME, sel, Seq.empty[QueryModifier], None, None)
    }
  /*
    def SELECT (sel : selectorAlignedDataType) : Selector = {
      new Selector[(Seq[StreamObject], RDD[(Long, Seq[Double])])](sc.BTRDB_MIN_TIME, sc.BTRDB_MAX_TIME, sel, Seq.empty[QueryModifier], None, None)
    }
  */
    def SET (tuples : (String, String)*): Setter = {
      new Setter(tuples)
    }

    def CREATE (path : String): Creator = {
      new Creator(path, Seq.empty[(String,String)])
    }

    def DROP (path : String) {
      metadataCollection.remove(MongoDBObject("Path" -> path))
    }
  }


  lazy val loc_btrdb = new BTrDB(btrdbHost, 4410)
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
    def persistToBtrdb(stream : String)
    {
      //Note that we assume that partitions don't overlap
      //I believe this is correct, but it might not be
      val targetHost = btrdbHost
      rdd.foreachPartition((vz : Iterator[(Long, Double)]) =>
      {
        if (vz.hasNext) {
          var b = new BTrDB(targetHost, 4410)
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

    def persistToBtrdb(streams : Seq[String])
    {
      //Note that we assume that partitions don't overlap
      //I believe this is correct, but it might not be
      val targetHost = btrdbHost
      rdd.foreachPartition((vz : Iterator[(Long, immutable.Seq[Double])]) =>
      {
        val dat = vz.toIndexedSeq
        if (dat.size > 0) {
          var b = new BTrDB(targetHost, 4410)
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

    def BTRDB_ALIGN_120HZ_FLOOR = alignIterTo120HzUsingFloor _

    def initDistil(btrdb : String, mongo : String)
    {
      implicitSparkContext = sc
      btrdbHost = btrdb
      mongoHost = mongo
    }
    def btrdbStream(stream : String, startTime : Long, endTime : Long, version : Long)
      : RDD[(Long, Double)] =
    {
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
        var b = new BTrDB(targetHost, 4410)
        val rv = vz.flatMap( v =>
        {
          var (stat, ver, it) = b.getRaw(stream, v._1, v._2, version)
          if (stat != "OK")
            throw BTrDBException("Error: "+stat)
          it.map(x => (x.t, x.v)).toIndexedSeq
        })
        //As above, can;t do this
        //b.close()
        rv
      })
    }

    def deleteBTrdbRange(stream : String, startTime : Long, endTime : Long) : Unit = {
      var stat = loc_btrdb.deleteValues(stream, startTime, endTime)
      if (stat != "OK")
        throw BTrDBException("Error: " + stat)
    }

    def multipleBtrdbStreams(streams : immutable.Seq[String], startTime : Long, endTime : Long, versions : immutable.Seq[Long],
      align : Option[Iterator[(Long, Double)] => Iterator[(Long, Double)]] = None)
      : RDD[(Long, immutable.Seq[Double])] =
    {

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
        var b = new BTrDB(targetHost, 4410)
        val rv = vz.flatMap( v =>
        {
          var raw_ires = streams.zip(versions).map(s =>
          {
            var (stat, ver, it) = b.getRaw(s._1, v._1, v._2, s._2)
            it.map(x => (x.t, x.v)).toIndexedSeq
          })
          var ires = align match
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
                idxz(i) = idxz(i) + 1
                Double.NaN
              else {
                idxz(i) = idxz(i) + 1
                ires(i)(idxz(i) - 1)._2
              }
            }))
          })
        })
        //can't do this
        //b.close()
        rv
      })
    }
  }

  implicit var implicitSparkContext : org.apache.spark.SparkContext = null
  //In package object ensure Mongo connection
  private lazy val mongoClient: MongoClient = MongoClient(mongoHost)
  private lazy val mongoDB = mongoClient("qdf")
  lazy val metadataCollection = mongoDB("metadata2")
  private var btrdbHost = "localhost"
  private var mongoHost = "localhost"
}
