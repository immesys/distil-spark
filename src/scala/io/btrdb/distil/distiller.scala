package io.btrdb.distil

import io.btrdb._
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
 MKDISTILLATE (CLASS) INPUTS (wiring map)
      OUTPUTS (wiring map) PARAM (param map)
      AFTER "date" BEFORE "date"
      ADDJAR "jar uri"
      AS "name"

 INPUTS is a map from <symbolic name> to <path>
 OUTPUTS is a map from <symbolic name> to <path>
 PARAM is a map of string -> string
 also a helper function: RESOLVE(Path) returns uuid

 A distillate stream has the following metadata tags:
 distil/instance : uuid (same for all outputs of a given instance)
 distil/instanceName : symbolic name given at creation time
 distil/classpath : (list of JAR URIs seperatated by commas)
 distil/class : Actual class
 distil/version : <version>
 distil/paramver : <version>
 distil/nextparamver : <version> //set when params are changed
 distil/automaterialize : "true" or "false" //used by background scheduler
 distil/inputs/<symbolic name> : <UUID>:version
 distil/output : symbolic name of this output
 distil/param/<symbolic name> : value
 */
object Distiller {
  def makeDistillate(klass : String, inputs : Map[String, String],
        outputs: Map[String, String], params: Map[String, String], name: String, jars: Seq[String])
    = {
      import io.btrdb.distil.dsl._
      //Check that none of the outputs exist
      outputs.foreach(kv => {
        val res = SELECT (METADATA) WHERE "Path" =~ kv._2
        if (!res.isEmpty) {
          throw DistillerException(s"Output stream: ${kv._2} exists")
        }
      })
      val instanceID = io.jvm.uuid.UUID.random.toString
      val loadedVersion = 1 //TODO get from jar

      //Create all of the outputs
      outputs.foreach(kv => {
        (CREATE (kv._2) WITH (
          "distil/instance" -> instanceID,
          "distil/class" -> klass,
          "distil/instanceName" -> name,
          "distil/classpath" -> jars.mkString(","),
          "distil/version" -> loadedVersion.toString,
          "distil/paramver" -> "1",
          "distil/nextparamver" -> "1",
          "distil/automaterialize" -> "false",
          "distil/output" -> kv._1
        )
        WITH (params.map(kv => ("distil/param/"+kv._1,kv._2)).toSeq:_*)
        WITH (inputs.map(kv => ("distil/inputs/"+kv._1,RESOLVE(kv._2).get+":1")).toSeq:_*)
        FROM ())
        println(s"created ${kv._2}")
      })
  }
  def constructDistiller(klass : String, classpath : Seq[String]) : Distiller = {
    /*var classLoader = new URLClassLoader(Array[URL](
  new File("module.jar").toURI.toURL))
var clazz = classLoader.loadClass(Module.ModuleClassName)*/
    var classloader = this.getClass.getClassLoader
    var k = classloader.loadClass(klass)
    var rv : Distiller = k.newInstance().asInstanceOf[Distiller]
    rv
  }
  def genericMaterialize(path : String) : Long = {
    import io.btrdb.distil.dsl._
    val res = SELECT(METADATA) WHERE "Path" =~ path
    if (res.isEmpty) throw StreamNotFoundException(s"Could not find $path")
    println("res0 was: ", res(0))
    val dd = constructDistiller(res(0)("distil/class"), res(0)("distil/classpath").split(','))
    dd.materialize(path)
  }
}
@SerialVersionUID(100L)
abstract class Distiller extends Serializable {
  println("Abstract distiller instantiated")
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
  private var inputUUIDs : immutable.SortedMap[String, String] = immutable.SortedMap()
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

    println("chk: ", instanceID == matches(0).get("distil/instance"))
    //Find all streams for this instance. We are trying to find the earliest
    //set of metadata from all the outputs
    val matches2 = SELECT(METADATA) WHERE "distil/instance" =~ instanceID
    println(s"matches2 is $matches2, instanceID is $instanceID")
    var useIDX = 0
    inputVersions = immutable.Map( inputNames.map( name => name -> matches2(useIDX)("distil/inputs/"+name)
      .split(':')(1).toLong ):_* )

    matches2.zipWithIndex.foreach(s => {
      inputNames.foreach(name => {
        if (s._1("distil/inputs/"+name).split(":")(1).toLong < inputVersions(name)) {
          useIDX = s._2
          inputVersions = immutable.Map( inputNames.map( name => name -> matches2(useIDX)("distil/inputs/"+name)
            .split(':')(1).toLong ):_* )
        }
      })
    })
    lastParamVer = matches2(useIDX)("distil/paramver").toInt
    lastVersion = matches2(useIDX)("distil/version").toInt
    nextParamVer = matches2(useIDX)("distil/nextparamver").toInt
    inputUUIDs = immutable.SortedMap( inputNames.map( name => name -> matches2(useIDX)("distil/inputs/"+name)
      .split(':')(0)):_* )

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
    val (errcode, vers) = btrdb.getVersions(keyseq.map(inputUUIDs(_)))
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

  def deleteAllRanges(range : (Long, Long)) (implicit db : BTrDB) {
    outputUUIDs.map { case (_, uu) => {
      val stat = db.deleteValues(uu, range._1, range._2)
      if (stat != "OK") {
        throw DistillerException("could not delete range")
      }
    }}
  }

  def deleteRange(output : String, range : (Long, Long)) (implicit db : BTrDB) {
    val stat = db.deleteValues(outputUUIDs(output), range._1, range._2)
    if (stat != "OK") {
      throw DistillerException("could not delete range")
    }
  }

  def kernel(range : (Long, Long),
             rangeStartIdx : Int,
             input : IndexedSeq[(Long, IndexedSeq[Double])],
             inputMap : Map[String, Int],
             output : Map[String, mutable.ArrayBuffer[(Long, Double)]],
             db : BTrDB)

  def materialize(path : String) (implicit sc : org.apache.spark.SparkContext) : Long = {
    loadinfo(path, sc)

    //Stage 1: find all the changed ranges in the inputs
    val btrdb = sc.openDefaultBTrDB()
    val keyseq = inputVersions.keys.toIndexedSeq //not sure this is stable, so keep it
    val someChange = keyseq.exists(k => inputVersions(k) != inputCurrentVersions(k))
    if (!someChange) {
      0L
    }
    val ranges = keyseq.filter(k => inputVersions(k) != inputCurrentVersions(k))
                       .flatMap(k => {
      val (errcode, _, cranges) = btrdb.getChangedRanges(inputUUIDs(k), inputVersions(k), inputCurrentVersions(k), 34)
      cranges.map(x=>(x.start, x.end))
    }).toIndexedSeq
    btrdb.close()

    //Stage 2: merge the changed ranges to a single set of ranges
    val combinedRanges = ranges//expandPrereqsParallel(ranges)
    println(s"combinedRanges is: $combinedRanges")
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
    val invocationParams = ((Stream from 1), newRanges, fetchRanges).zipped.toList

    println(s"Processing a total of ${fetchRanges.size} ranges")

    //Stage 4: load the partitions
    //Buffer for serialization
    val targetBTrDB = io.btrdb.distil.btrdbHost
    println("XXX -B")
    sc.parallelize(invocationParams).map (p => {
      println("XXX -A", p)
      Console.err.println("XXX -+-A", p)
      val idx = p._1
      val fetch = p._2
      val range = p._3
      val b = new BTrDB(targetBTrDB, 4410)
      println("Created BTrDB conn")
      val inputUUIDSeq = inputUUIDs.map(kv => kv._2).toIndexedSeq
      val inputMap = immutable.Map(inputUUIDs.keys.zipWithIndex.toSeq:_*)
      val inputVerSeq = inputUUIDs.map(kv => inputCurrentVersions(kv._1)).toIndexedSeq
      println("XXX A")
      val data = io.btrdb.distil.multipleBtrdbStreamLocal(b, inputUUIDSeq,
        fetch, inputVerSeq, timeBaseAlignment)
      println("XXX B")
      val output = immutable.Map(outputNames.map(name => (name, new mutable.ArrayBuffer[(Long, Double)](data.length))):_*)
      println("XXX C")
      val dstartIdx = data.indexWhere(_._1 >= range._1)
      println("XXX D")
      val thn = System.currentTimeMillis
      kernel(range, dstartIdx, data, inputMap, output, b)
      val delta = System.currentTimeMillis - thn
      println("XXX E")
      /*
      println(s"Kernel processing completed in $delta ms")
      output.foreach(kv => {
        val uu = outputUUIDs(kv._1)
        b.insertValues(uu, kv._2.view.iterator.map(tup => RawTuple(tup._1, tup._2)), true)
      })
      b.close()
      output.map(kv => kv._2.size).foldLeft(0L)(_+_)*/
      5L
    }).reduce(_+_)
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
               input : IndexedSeq[(Long, IndexedSeq[Double])],
               inputMap : Map[String, Int],
               output : Map[String, mutable.ArrayBuffer[(Long, Double)]],
               db : BTrDB) = {
    //Our inputs are dense, so we can just use indexes. we will use 120 indexes
    val out = output("avg")
    for (i <- rangeStartIdx until input.size) {
      //For now its identity
      out += ((input(i)._1 , input(i)._2(0)))
    }
    deleteAllRanges(range)(db)
  }
}

class DoublerDistiller extends Distiller {
  println("Concrete distiller instantiated")
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
    = Some(1.second)
  val timeBaseAlignment : Option[BTrDBAlignMethod]
    = Some(BTRDB_ALIGN_120HZ_SNAP_DENSE)
  val dropNaNs : Boolean
    = false

  override def kernel(range : (Long, Long),
               rangeStartIdx : Int,
               input : IndexedSeq[(Long, IndexedSeq[Double])],
               inputMap : Map[String, Int],
               output : Map[String, mutable.ArrayBuffer[(Long, Double)]],
               db : BTrDB) = {
    //Our inputs are dense, so we can just use indexes. we will use 120 indexes
    val out = output("output")
    for (i <- rangeStartIdx until input.size) {
      //For now its identity
      out += ((input(i)._1 , input(i)._2(0)*2))
    }
    deleteAllRanges(range)(db)
  }
}
