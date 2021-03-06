package io.btrdb.distil

import io.btrdb._
import io.jvm.uuid._
import scala.collection.immutable
import scala.collection.mutable


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
          throw DistilException(s"Output stream: ${kv._2} exists")
        }
      })
      val instanceID = io.jvm.uuid.UUID.random.toString
      val loadedVersion = 1 //TODO get from jar

      //Verify the input/output mapping
      val dd = constructDistiller(klass, jars)

      //Verify all actual are in formal
      inputs.map (i => i._1).foreach (symname => {
        if (!dd.inputNames.contains(symname)) {
          throw DistilException(s"Distillate does not take input '$symname'")
        }
      })
      outputs.map (i => i._1) foreach (symname => {
        if (!dd.outputNames.contains(symname)) {
          throw DistilException(s"Distillate does not have output '$symname'")
        }
      })
      //Verify all formal are in actual
      dd.inputNames.foreach( symname => {
        if (!inputs.contains(symname)) {
          throw DistilException(s"Distillate requires input '$symname'")
        }
      })
      dd.outputNames.foreach( symname => {
        if (!outputs.contains(symname)) {
          throw DistilException(s"Distillate requires output '$symname'")
        }
      })
      dd.reqParams.foreach( symname => {
        if(!params.contains(symname)) {
          throw DistilException(s"Distillate requires parameter '$symname'")
        }
      })
      //Verify all input streams exist
      inputs.foreach (kv => {
        val res = SELECT(METADATA) WHERE "Path" =~ kv._2
        if (res.isEmpty) {
          throw DistilException(s"Input '${kv._1}' is bound to nonexistant path '${kv._2}'")
        }
      })

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
          "distil/output" -> kv._1,
          "Locks/Materialize" -> "UNLOCKED"
        )
        WITH (params.map(kv => ("distil/param/"+kv._1,kv._2)).toSeq:_*)
        WITH (inputs.map(kv => ("distil/inputs/"+kv._1,PATH2UUID(kv._2).get+":1")).toSeq:_*)
        FROM ())
      })
  }
  def constructDistiller(klass : String, classpath : Seq[String]) : Distiller = {
    var classLoader = (if (classpath.size != 0) {
      val cpurls = classpath.map(new java.net.URI(_).toURL).toArray
      cpurls foreach (u => {
        if (u.openConnection().asInstanceOf[java.net.HttpURLConnection].getResponseCode != 200) {
          throw DistilException(s"Cannot load JAR '$u'")
        }
      })
      classpath foreach (cp => {
        implicitSparkContext.addJar(cp)
      })
      new scala.tools.nsc.util.ScalaClassLoader.URLClassLoader(cpurls, this.getClass.getClassLoader)
    } else {
      this.getClass.getClassLoader
    })
    var k = classLoader.loadClass(klass)
    var rv : Distiller = k.newInstance().asInstanceOf[Distiller]
    rv
  }
  def genericMaterialize(path : String, identifier : String) : Long = {
    import io.btrdb.distil.dsl._
    val res = SELECT(METADATA) WHERE "Path" =~ path
    if (res.isEmpty) throw StreamNotFoundException(s"Could not find $path")
    val cp = (if(res(0)("distil/classpath") == "") Array[String]() else res(0)("distil/classpath").split(','))
    val dd = constructDistiller(res(0)("distil/class"), cp)
    dd.materialize(path, identifier)
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
          if (ranges(minidx)._3 || ri._1._1 < ranges(minidx)._1) {
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
}
@SerialVersionUID(100L)
abstract class Distiller extends Serializable {
  import io.btrdb.distil.dsl._
  //Abstract algorithm-specific members
  val version : Int
  val maintainer : String
  val outputNames : Seq[String]
  val inputNames : Seq[String]
  val reqParams : Seq[String]
  def kernelSizeNanos : Option[Long]
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
    if (start >= end)
      None
    else
      Some((start, end))
  }



  private def loadinfo(path : String, sc : org.apache.spark.SparkContext) {

    val matches = SELECT(METADATA) WHERE "Path" =~ path
    if (matches.size == 0) {
      throw StreamNotFoundException(s"Could not find $path")
    }
    if (matches.size > 1) {
      throw DistilException(s"Multiple streams have path $path")
    }
    val instanceID = io.jvm.uuid.UUID(matches(0)
      .get("distil/instance")
      .getOrElse(
        throw DistilException(
          s"$path does not seem to be a distillate")))
      .toString

    //Find all streams for this instance. We are trying to find the earliest
    //set of metadata from all the outputs
    val matches2 = SELECT(METADATA) WHERE "distil/instance" =~ instanceID
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
      kv._1.slice(13,kv._1.size) -> kv._2
    }):_*)

    //Lets get the current versions of the inputs
    val btrdb = sc.openDefaultBTrDB()
    val keyseq = inputVersions.keys.toIndexedSeq //not sure this is stable, so keep it
    val (errcode, vers) = btrdb.getVersions(keyseq.map(inputUUIDs(_)))
    if (errcode != "OK") throw DistilException("BTrDB error: "+errcode)
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
        throw DistilException("could not delete range")
      }
    }}
  }

  def deleteRange(output : String, range : (Long, Long)) (implicit db : BTrDB) {
    val stat = db.deleteValues(outputUUIDs(output), range._1, range._2)
    if (stat != "OK") {
      throw DistilException("could not delete range")
    }
  }

  def kernel(range : (Long, Long),
             rangeStartIdx : Int,
             input : IndexedSeq[(Long, IndexedSeq[Double])],
             inputMap : Map[String, Int],
             output : Map[String, mutable.ArrayBuffer[(Long, Double)]],
             db : BTrDB)

  def materialize(path : String, identifier : String) : Long = {
    loadinfo(path, implicitSparkContext)

    //Stage 0: lock all the outputs
    val lockid = s"$identifier::${implicitSparkContext.applicationId}"
    var ourlocks : List[String] = List()
    outputUUIDs.foreach(kv => {
      val ok = SET ("Locks/Materialize" -> lockid) WHERE "uuid" === kv._2 && "Locks/Materialize" === "UNLOCKED"
      if (!ok) {
        ourlocks.foreach (uu => {
          SET ("Locks/Materialize" -> "UNLOCKED") WHERE "uuid" === uu
        })
        throw LockedException(s"Output '${kv._1}' (${kv._2}) could not be locked")
      }
      ourlocks = ourlocks :+ (kv._2)
    })

    //If we require full computation, issue a delete now
    val btrdb = implicitSparkContext.openDefaultBTrDB()
    if (requireFullRecomputation) {
      outputUUIDs.foreach {case (symname, uuid) => {
        btrdb.deleteValues(uuid, implicitSparkContext.BTRDB_MIN_TIME, implicitSparkContext.BTRDB_MAX_TIME)
      }}
    }

    //Possibly override inputVersions if we require full recomputation
    val shadowInputVersions : Map[String, Long] = (if (requireFullRecomputation) {
      inputVersions map (kv => (kv._1 -> 1L))
    } else {
      inputVersions
    })

    //Possibly override inputCurrentVersions if the delta is too big
    //Disabled for now because it means that "MATERIALIZE" no longer
    //guarantees the stream is up to date
    /*
    val maxVersionDelta = 4000000000/16000 // 4 billion points roughly
    val shadowInputCurrentVersions : Map[String, Long] =
      inputCurrentVersions map (kv => {
        if (kv._2 - inputVersions(kv._1) > maxVersionDelta)
          kv._1 -> (inputVersions(kv._1) + maxVersionDelta)
        else
          kv._1 -> kv._2
      })
    */
    val shadowInputCurrentVersions = inputCurrentVersions

    //Stage 1: find all the changed ranges in the inputs
    val keyseq = shadowInputVersions.keys.toIndexedSeq //not sure this is stable, so keep it
    val someChange = keyseq.exists(k => shadowInputVersions(k) != shadowInputCurrentVersions(k))
    if (!someChange) {
      println("No changes")
      ourlocks.foreach (uu => {
        SET ("Locks/Materialize" -> "UNLOCKED") WHERE "uuid" === uu
      })
      return 0L
    }
    val ranges = keyseq.filter(k => shadowInputVersions(k) != shadowInputCurrentVersions(k))
                       .flatMap(k => {
      val (errcode, _, cranges) = btrdb.getChangedRanges(inputUUIDs(k), shadowInputVersions(k), shadowInputCurrentVersions(k), 34)
      cranges.map(x=>(x.start, x.end))
    }).toIndexedSeq
    btrdb.close()

    //Stage 2: merge the changed ranges to a single set of ranges
    var combinedRanges = Distiller.expandPrereqsParallel(ranges)

    //Clamp ranges to before/after
    combinedRanges = combinedRanges.map(t => clampRange(t)).filterNot(_.isEmpty).map(_.get)

    if (combinedRanges.size == 0) {
      println("No changes")
      ourlocks.foreach (uu => {
        SET ("Locks/Materialize" -> "UNLOCKED") WHERE "uuid" === uu
      })
      return 0L
    }

    val partitionHint = Math.min(Math.max(combinedRanges.map(r => r._2 - r._1).foldLeft(0L)(_ + _) / (implicitSparkContext.defaultParallelism*3),
                                 30L*60L*1000000000L), //Seriously don't split finer than 30 minutes...
                                 2*60*60*1000000000L) //But also not bigger than two hours

    //Stage 3: split the larger ranges so that we can get better partitioning
    val newRanges = combinedRanges.flatMap(r => {
      var segments : Long = (r._2 - r._1) / partitionHint + 1
      var rv = for (i<- 0L until segments) yield {
        (r._1 + i*partitionHint,
         math.min(r._1 + (i+1)*partitionHint, r._2))
      }
      rv
    }).filterNot(t=>t._1 == t._2)
    //println(s"ranges after split: ${newRanges.size} ${newRanges}")
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
    var cnt : Long = -1
    try {
      cnt = implicitSparkContext.parallelize(invocationParams).map (p => {
        val idx = p._1
        val range = p._2
        val fetch = p._3
        println(s"invoking p=$p")
        val b = new BTrDB(targetBTrDB, 4410)
        val inputUUIDSeq = inputUUIDs.map(kv => kv._2).toIndexedSeq
        val inputMap = immutable.Map(inputUUIDs.keys.zipWithIndex.toSeq:_*)
        val inputVerSeq = inputUUIDs.map(kv => shadowInputCurrentVersions(kv._1)).toIndexedSeq
        val data = io.btrdb.distil.multipleBtrdbStreamLocal(b, inputUUIDSeq,
          fetch, inputVerSeq, timeBaseAlignment)
        val output = immutable.Map(outputNames.map(name => (name, new mutable.ArrayBuffer[(Long, Double)](data.length))):_*)
        val dstartIdx = data.indexWhere(_._1 >= range._1)
        val thn = System.currentTimeMillis
        kernel(range, dstartIdx, data, inputMap, output, b)
        val delta = System.currentTimeMillis - thn

        println(s"Kernel processing completed in $delta ms")
        output.foreach(kv => {
          val uu = outputUUIDs(kv._1)
          b.insertValues(uu, kv._2.view.iterator.map(tup => RawTuple(tup._1, tup._2)), true)
        })
        b.close()
        output.map(kv => kv._2.size).foldLeft(0L)(_+_)
      }).reduce(_+_)

      //Set updated metadata
      shadowInputCurrentVersions.foreach(kv => {
        val uu = inputUUIDs(kv._1)
        outputUUIDs.foreach(okv => {
          SET( ("distil/inputs/"+kv._1) -> (uu+":"+kv._2)) WHERE "uuid" === okv._2
        })
      })
      outputUUIDs.foreach(okv => {
        SET("distil/version" -> version.toString) WHERE "uuid" === okv._2
        SET("distil/paramver" -> nextParamVer.toString) WHERE "uuid" === okv._2
      })

    } finally {
      ourlocks.foreach (uu => {
        SET ("Locks/Materialize" -> "UNLOCKED") WHERE "uuid" === uu
      })
    }
    cnt
  }
}
