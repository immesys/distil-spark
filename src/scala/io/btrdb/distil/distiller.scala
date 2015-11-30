package io.btrdb.distil

import io.jvm.uuid._
import scala.collection.immutable
import scala.collection.mutable

class RootDistillerException(msg : String) extends Exception(msg)

case class DistillerException(msg : String) extends RootDistillerException(msg)
case class StreamNotFoundException(msg : String) extends RootDistillerException(msg)

/*
 A distiller is instatiated as a result of a MATERIALIZE or MKDISTILLATE command.
 It looks like:

 MATERIALIZE (Path)
 or
 MKDISTILLATE (JAR, CLASS) INPUTS (wiring map)
      OUTPUTS (wiring map) PARAM (param map)
      AFTER "date" BEFORE "date"
      LAZY | EAGER

 INPUTS is a map from <symbolic name> to <path>
 OUTPUTS is a map from <symbolic name> to <path>
 PARAM is a map of string -> string
 also a helper function: RESOLVE(Path) returns uuid

 A distillate stream has the following metadata tags:
 distil/instance : uuid (same for all outputs of a given instance)
 distil/classpath : (list of JAR URIs seperatated by commas)
 distil/version : <version>
 distil/paramver : <version>
 distil/nextparamver : <version> //set when params are changed
 distil/automaterialize : "true" or "false" //used by background scheduler
 distil/inputs/<symbolic name> : <UUID>:version
 distil/output : symbolic name of this output
 distil/param/<symbolic name> : value
 */
abstract class Distiller {
  //Abstract algorithm-specific members
  val version : Int
  val maintainer : String
  val outputNames : Seq[String]
  val inputNames : Seq[String]
  val kernelSizeNanos : Option[Long]
  val timeBaseAlignment : Option[BTrDBAlignMethod]
  val dropNaNs : Boolean

  //These come from loadinfo
  private var inputVersions : immutable.Map[String, Long] = Map()
  private var inputCurrentVersions : immutable.Map[String, Long] = Map()
  private var lastParamVer : Int = -1
  private var nextParamVer : Int = -1
  private var lastVersion : Int = -1
  private var outputUUIDs : immutable.Map[String, String] = Map()
  private var requireFullRecomputation : Boolean = false
  var params : immutable.Map[String, String] = Map()

  def clampRange(range : (Long, Long))
    : Option[(Long, Long)] = {
    var start = range._1
    var end = range._2
    if (params.contains("after")) {
      val after = dsl.StringToTS(params("after")).t
      if (start < after) start = after
    }
    if (params.contains("before")) {
      val before = dsl.StringToTS(params("before")).t
      if (end > before) end = before
    }
    if (start > end)
      None
    else
      Some((start, end))
  }

  def deleteRange(range : (Long, Long)) = {

  }
  def expandPrereqsParallel(changedRanges : Seq[(Long, Long)]) : Seq[(Long, Long)] = {
    var ranges : mutable.ArrayBuffer[(Long, Long, Boolean)] = mutable.ArrayBuffer(changedRanges.map(x=>(x._1, x._2, false)):_*)
    var combinedRanges : mutable.ArrayBuffer[(Long, Long)] = mutable.ArrayBuffer()

    var notDone = true
    while (notDone) {
      var progress = false
      var combined = false
      var minidx = 0
      for (ri <- ranges.zipWithIndex) {
        if (!ri._1._3) {
          progress = true
          //If another range starts before minidx
          if (ri._1._1 < ranges(minidx)._1) {
            minidx = ri._2 //we zipped with index
          }
        }
      }
      //Now see if any other ranges' starts lie before the end of min
      for (ri <- ranges.zipWithIndex) {
        if (ri._1._3) {}  //used up
        else if (ri._2 == minidx) {}
        else if (ri._1._1 <= ranges(minidx)._2) {
          //This range's start lies before the end of min
          //set minidx's end to the max of the new range and min's end
          ranges(minidx) = (ranges(minidx)._1, math.max(ranges(minidx)._2, ri._1._2), false)
          //Remove the new range (it is subsumed)
          ranges(ri._2) = (0,0, true)
          combined = true
        }
      }
      if (!progress) {
        notDone = false
      } else if (!combined) {
        val t = (ranges(minidx)._1 , ranges(minidx)._2)
        combinedRanges += t
        ranges(minidx) = (0,0, true)
      }
    }
    combinedRanges
  }

  private def loadinfo(path : String, sc : org.apache.spark.SparkContext) {
    import io.btrdb.distil.dsl._
    val matches = SELECT(METADATA) WHERE "Path" =~ path
    if (matches.size == 0) {
      throw StreamNotFoundException(s"Could not find $path")
    }
    if (matches.size > 1) {
      throw DistillerException(s"Multiple streams have path $path")
    }
    val instanceID = io.jvm.uuid.UUID(matches(0)
      .get("distil/instance")
      .getOrElse(
        throw DistillerException(
          s"$path does not seem to be a distillate")))
      .toString

    //Find all streams for this instance. We are trying to find the earliest
    //set of metadata from all the outputs
    val matches2 = SELECT(METADATA) WHERE "distil/instance" =~ instanceID
    var useIDX = 0
    inputVersions = immutable.Map( inputNames.map( name => name -> matches2(useIDX)("distil/inputs/"+name).toLong ):_* )

    matches2.zipWithIndex.foreach(s => {
      inputNames.foreach(name => {
        if (s._1("distil/inputs/"+name).toInt < inputVersions(name)) {
          useIDX = s._2
          inputVersions = immutable.Map( inputNames.map( name => name -> matches2(useIDX)("distil/inputs/"+name).toLong ):_* )
        }
      })
    })
    lastParamVer = matches2(useIDX)("distil/paramver").toInt
    lastVersion = matches2(useIDX)("distil/version").toInt
    nextParamVer = matches2(useIDX)("distil/nextparamver").toInt

    if (matches2.size != outputNames.size) {
      println("The algorithm output arity does not match the found streams\nWill do full recompute to ensure consistency")
      requireFullRecomputation = true
    }
    if (lastParamVer != nextParamVer) {
      println("The algorithm parameter version has changed\nWill do full recompute to ensure consistency")
      requireFullRecomputation = true
    }
    if (lastVersion != version) {
      println("The algorithm implementation version has changed\nWill do full recompute to ensure consistency")
      requireFullRecomputation = true
    }

    //Now lets grab all the outputUUIDs
    //It is possible that not all of them exist, either because some were
    //dropped or because the algorithm changed.
    outputUUIDs = immutable.Map(matches2.map( s=> s("distil/output") -> s("uuid")):_*)

    //Lets grab the parameters:
    params = immutable.Map(matches2(useIDX).toSeq.filter(kv => kv._1.startsWith("distil/param/")).map(kv => {
      kv._1.slice(12,kv._1.size) -> kv._2
    }):_*)

    //Lets get the current versions of the inputs
    val btrdb = sc.openDefaultBTrDB()
    val keyseq = inputVersions.keys.toIndexedSeq //not sure this is stable, so keep it
    val (errcode, vers) = btrdb.getVersions(keyseq)
    if (errcode != "OK") throw DistillerException("BTrDB error: "+errcode)
    inputCurrentVersions = immutable.Map( keyseq.zip(vers.toIndexedSeq):_* )
    btrdb.close()

  }
  // /**
  //  * Called before the kernel is executed
  //  */
  // def preop (rdd : RDD[(Long,Seq[Double])])
  //   : RDD[(Long,Seq[Double])]] = {
  //   rdd
  // }
  //
  // /**
  //  * Called after the kernel is executed
  //  */
  // def postop (rdd : RDD[(Long,Seq[Double])])
  //   : RDD[(Long,Seq[Double])]] = {
  //   rdd
  // }

  def deleteDefaultRanges(range : (Long, Long)) {

  }
  def deleteRange(output : String, range : (Long, Long)) {

  }

  def kernel(range : (Long, Long),
             rangeStartIdx : Int,
             input : Seq[(Long, Seq[Double])],
             inputMap : Map[String, Int],
             output : Map[String, mutable.ArrayBuffer[(Long, Double)]])

  def materialize(path : String) (implicit sc : org.apache.spark.SparkContext) {
    loadinfo(path, sc)

    //Stage 1: find all the changed ranges in the inputs
    val btrdb = sc.openDefaultBTrDB()
    val keyseq = inputVersions.keys.toIndexedSeq //not sure this is stable, so keep it
    val someChange = keyseq.exists(k => inputVersions(k) != inputCurrentVersions(k))
    if (!someChange) {
      return
    }
    val ranges = keyseq.filter(k => inputVersions(k) != inputCurrentVersions(k))
                       .flatMap(k => {
      val (errcode, _, cranges) = btrdb.getChangedRanges(k, inputVersions(k), inputCurrentVersions(k), 34)
      cranges.map(x=>(x.start, x.end))
    }).toIndexedSeq
    btrdb.close()

    //Stage 2: merge the changed ranges to a single set of ranges
    val combinedRanges = expandPrereqsParallel(ranges)
    val partitionHint = Math.max(combinedRanges.map(r => r._2 - r._1).foldLeft(0L)(_ + _) / sc.defaultParallelism,
                                 30L*60L*1000000000L) //Seriously don't split finer than 30 minutes...

    //Stage 3: split the larger ranges so that we can get better partitioning
    val newRanges = combinedRanges.flatMap(r => {
      var segments : Long = (r._2 - r._1) / partitionHint + 1
      var rv = for (i<- 0L until segments) yield {
        (r._1 + i*partitionHint,
         math.min(r._1 + (i+1)*partitionHint, r._2))
      }
      println(s"turned $r into $rv")
      rv
    })
    //Stage 4: adjust for the prefetch data that the kernel requires
    val fetchRanges = kernelSizeNanos match {
      case None => newRanges
      case Some(ksz) => newRanges.map(r => (r._1 - ksz, r._2))
    }

    //Should be a tuple of (idx, (fetchstart, fetchend), (rangestart, rangend))
    val invocationParams = (Stream from 1).zip(newRanges.zip(fetchRanges))

    println(s"Processing a total of ${fetchRanges.size} ranges")

    //Stage 4: make the RDD
    //Buffer for serialization
    val targetBTrDB = io.btrdb.distil.btrdbHost
    /*
    sc.parallelize(invocationParams).map (p => {
      val (idx, fetch, range) = p
        var b = new BTrDB(targetBTrDB)
        var raw_ires = streams.zip(versions).map(s =>
          {
            var (stat, ver, it) = b.getRaw(s._1, v._1, v._2, s._2)
            if (stat != "OK")
                throw BTrDBException("Error: "+stat)
            it.map(x => (x.t, x.v)).toIndexedSeq
          })
      })
      */
  }
}

class MovingAverageDistiller extends Distiller {
  import io.btrdb.distil.dsl._

  val version : Int
    = 1
  val maintainer : String
    = "Michael Andersen"
  val outputNames : Seq[String]
    = List("avg")
  val inputNames : Seq[String]
    = List("input")
  val kernelSizeNanos : Option[Long]
    = Some(1.second)
  val timeBaseAlignment : Option[BTrDBAlignMethod]
    = Some(BTRDB_ALIGN_120HZ_SNAP_DENSE)
  val dropNaNs : Boolean
    = false

  override def kernel(range : (Long, Long),
               rangeStartIdx : Int,
               input : Seq[(Long, Seq[Double])],
               inputMap : Map[String, Int],
               output : Map[String, mutable.ArrayBuffer[(Long, Double)]]) = {
    //Our inputs are dense, so we can just use indexes. we will use 120 indexes
    val out = output("avg")
    for (i <- rangeStartIdx until input.size) {
      //For now its identity
      out += ((input(i)._1 , input(i)._2(0)))
    }
  }
}
