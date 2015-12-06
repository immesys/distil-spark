package io.btrdb.distil

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
      return MongoDBObject(field.replaceAll("/",".") -> MongoDBObject("$exists" -> true))
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
      return MongoDBObject(field.replaceAll("/",".") -> MongoDBObject("$regex" -> pattern))
    }
  }
  case class AndWhereExpression(lhs : WhereExpression, rhs : WhereExpression) extends WhereExpression {
    override def toString : String = {
      "("+lhs.toString+", AND, "+rhs.toString+")"
    }
    override def generate() : MongoDBObject = {
      return MongoDBObject("$and" -> Array(lhs.generate(),rhs.generate()))
    }
  }
  case class LiteralWhereExpression(field : String, rhs : String) extends WhereExpression {
    override def toString : String = {
      "("+field+", EQ, "+rhs+")"
    }
    override def generate() : MongoDBObject = {
      return MongoDBObject(field.replaceAll("/",".") -> rhs)
    }
  }

  case class LiteralNotWhereExpression(field : String, rhs : String) extends WhereExpression {
    override def toString : String = {
      "("+field+", NEQ, "+rhs+")"
    }
    override def generate() : MongoDBObject = {
      return MongoDBObject(field.replaceAll("/",".") -> MongoDBObject("$ne" -> rhs))
    }
  }

  import scala.collection.mutable.HashMap

  class StreamObject (elems : Tuple2[String, String]*) extends HashMap[String, String] {
    this ++= elems
    def path : String = this("Path")
    def uuid : String = this("uuid")
  }

  class QueryModifier

  class Setter (tuples : Seq[(String, String)], single : Boolean) {
    def WHERE (w : WhereExpression) (implicit sc : org.apache.spark.SparkContext) : Boolean = {
      execute(w, sc)
    }

    def execute (w : WhereExpression, sc : org.apache.spark.SparkContext) : Boolean = {
      var query = w.generate()
      val ntups = tuples.map(t=>(t._1.replaceAll("/","."),t._2))
      if (single) {
        val rv = metadataCollection.findAndModify(query, MongoDBObject("$set"->MongoDBObject(ntups:_*)))
        rv match {
          case Some(x) => true
          case None => false
        }
      } else {
        var rv = metadataCollection.update(query, MongoDBObject("$set"->MongoDBObject(ntups:_*)), multi=true)
        rv.getN > 0
      }
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
    def FROM (): String = {
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
      dat("uuid")
    }
    def FROM (rdd : RDD[(Long, Double)]): String = {
      val uu = FROM()
      rdd.persistToBtrdb(uu)
      uu
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



  class MkDistiller (klass : String,
    inputs : Seq[(String, String)],
    outputs : Seq[(String, String)],
    params : Seq[(String, String)],
    deps : Seq[String]) {
    def INPUT (m : (String, String)*)
      : MkDistiller = {
      new MkDistiller(klass, inputs ++ m, outputs, params, deps)
    }
    def OUTPUT (m : (String, String)*)
      : MkDistiller = {
      new MkDistiller(klass, inputs, outputs ++ m, params, deps)
    }
    def PARAM (m : (String, String)*)
      : MkDistiller = {
      new MkDistiller(klass, inputs, outputs, params ++ m, deps)
    }
    def AFTER (s : String)
      : MkDistiller = {
      new MkDistiller(klass, inputs, outputs, params :+ (("after",s)), deps)
    }
    def BEFORE (s : String)
      : MkDistiller = {
      new MkDistiller(klass, inputs, outputs, params :+ (("before",s)), deps)
    }
    def WITHJAR (uri : String)
      : MkDistiller = {
      new MkDistiller(klass, inputs, outputs, params, deps :+ (uri))
    }
    def AS (name : String) {
      Distiller.makeDistillate(klass,Map(inputs:_*),Map(outputs:_*),Map(params:_*),name,deps)
    }

  }
  import scala.language.higherKinds
  type SingleResult[T] = (StreamObject, RDD[(Long, T)])
  type MultipleResult[T] = Seq[(StreamObject, RDD[(Long, T)])]
  type MergedResult[T] =  (Seq[StreamObject], RDD[(Long, Seq[T])])
  type MetadataResult[T] = Seq[StreamObject]
  class Selector[T, C[X]] (startT : BTrDBTimestamp, endT : BTrDBTimestamp,
                  sel : Any, mods : Seq[QueryModifier],
                  align : Option[BTrDBAlignMethod], window : Option[(Boolean, Long)], joined : Boolean) {
    def WHERE (w : WhereExpression) (implicit sc : org.apache.spark.SparkContext) : C[T] = {
        execute(w, sc)
    }
    def WITH (w : QueryModifier*) : Selector[T, C] = {
        new Selector[T, C](startT, endT, sel, mods ++ w, align, window, joined)
    }
    def BEFORE (t : BTrDBTimestamp) : Selector[T, C] = {
        new Selector[T, C](startT, t, sel, mods, align, window, joined)
    }
    def JOIN (s : String) : Selector[T, MergedResult] = {
        s match {
          case INNER => new Selector[T, MergedResult] (startT, endT, ALIGNED,
            mods ++ Array(DROPNAN), align, window, true)
          case OUTER => new Selector[T, MergedResult] (startT, endT, ALIGNED,
            mods, align, window, true)
        }
    }

    def WINDOW (l : Long) : Selector[StatTuple, C] = {
        new Selector[StatTuple, C](startT, endT, sel, mods, align, Some((false, l)), joined)
    }

    def BINWINDOW (l : Long) : Selector[StatTuple, C] = {
        val pw = (Math.floor(Math.log(l)/Math.log(2))).toInt
        new Selector[StatTuple, C](startT, endT, sel, mods, align, Some((true, pw)), joined)
    }

    def ALIGN (method : BTrDBAlignMethod) : Selector[T, C] = {
      new Selector[T, C](startT, endT, sel, mods, Some(method), window, joined)
    }

    def AFTER (t : BTrDBTimestamp) : Selector[T, C] = {
        new Selector[T, C](t, endT, sel, mods, align, window, joined)
    }

    def BETWEEN (s : BTrDBTimestamp, e : BTrDBTimestamp) : Selector[T, C] = {
        new Selector[T, C](s, e, sel, mods, align, window, joined)
    }

    private[this] def execute(w : WhereExpression, sc : org.apache.spark.SparkContext) : C[T] = {

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
              val npfx = (if (pfx == "") "" else pfx+"/")
              add(dd, npfx + k, target)
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
      if (sel == METADATA) {
        streamMetadatas.asInstanceOf[C[T]]
      } else {
      val rv = window match {
        case Some((doBin, param)) => {
            sel match {
                case ONE => {
                    val so = streamMetadatas(0)
                    val dat = (if (doBin) {
                      sc.btrdbStatisticalStream(so("uuid"), startT.t, endT.t, param.toInt, sc.BTRDB_LATEST_VER)
                    } else {
                      sc.btrdbWindowStream(so("uuid"), startT.t, endT.t, 0, param, sc.BTRDB_LATEST_VER)
                    })
                    (so, dat)
                }
                case ALL => {
                    streamMetadatas map (st => {
                      val dat = (if (doBin) {
                        sc.btrdbStatisticalStream(st("uuid"), startT.t, endT.t, param.toInt, sc.BTRDB_LATEST_VER)
                      } else {
                        sc.btrdbWindowStream(st("uuid"), startT.t, endT.t, 0, param, sc.BTRDB_LATEST_VER)
                      })
                      (st, dat)
                    })
                }
                case ALIGNED => {
                    val dat = (if (doBin) {
                      sc.multipleBtrdbStatisticalStreams(streamMetadatas.map(x=>x("uuid")).toIndexedSeq, startT.t, endT.t, param.toInt, (streamMetadatas map (_=> sc.BTRDB_LATEST_VER)).toIndexedSeq)
                    } else {
                      sc.multipleBtrdbWindowStreams(streamMetadatas.map(x=>x("uuid")).toIndexedSeq, startT.t, endT.t, 0, param, (streamMetadatas map (_=> sc.BTRDB_LATEST_VER)).toIndexedSeq)
                    })
                    (streamMetadatas, dat)
                }
            }
          }
          case None => {
              sel match {
                  case ONE => {
                      var so = streamMetadatas(0)
                      var dat = sc.btrdbStream(so("uuid"), startT.t, endT.t, sc.BTRDB_LATEST_VER)
                      (so, dat)
                  }
                  case ALL => {
                      (streamMetadatas map (so => {
                      (so, sc.btrdbStream(so("uuid"), startT.t, endT.t, sc.BTRDB_LATEST_VER))
                      }))
                  }
                  case ALIGNED => {
                      var rv = sc.multipleBtrdbStreams((streamMetadatas map (x => x("uuid"))).toIndexedSeq, startT.t, endT.t, (streamMetadatas map (_=> sc.BTRDB_LATEST_VER)).toIndexedSeq, align)
                      if (mods contains DROPNAN)
                      rv = rv.dropNaNs()
                      (streamMetadatas, rv)
                  }
              }
          }
        }
        rv.asInstanceOf[C[T]]
      }
    }
  }
}
