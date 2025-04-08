package project_3

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx._
import org.apache.spark.storage.StorageLevel
import org.apache.log4j.{Level, Logger}

object main {
  // Set logging to reduce console noise
  val rootLogger = Logger.getRootLogger()
  rootLogger.setLevel(Level.ERROR)
  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
  Logger.getLogger("org.spark-project").setLevel(Level.WARN)

  /**
   * Luby's Algorithm for Maximal Independent Set (MIS).
   * We store the vertex labels as follows:
   *   0  => vertex is still undecided
   *   1  => vertex is in the MIS
   *   -1 => vertex is not in the MIS
   *
   * The algorithm runs multiple rounds. In each round:
   *   (a) Generate a random value for each 0-labeled (undecided) vertex.
   *   (b) If a vertex has the highest random value among its 0-labeled neighbors,
   *       mark it as 1 (in MIS).
   *   (c) Those newly chosen vertices cause all their 0-labeled neighbors to become -1.
   *   (d) Repeat until no 0-labeled (undecided) vertices remain or no vertex changes
   *       in a round.
   */
  def LubyMIS(g_in: Graph[Int, Int]): Graph[Int, Int] = {

    // Initialize all vertices with label=0 (undecided)
    var g = g_in.mapVertices((vid, attr) => 0)

    var activeVertices = g.vertices.filter { case (_, label) => label == 0 }.count()
    var oldActiveVertices = -1L

    // Keep iterating while there are undecided vertices and progress is still being made
    while (activeVertices > 0 && activeVertices != oldActiveVertices) {

      // Step (a): Generate a random value for each vertex (but only relevant if label == 0).
      // We'll do an RDD of (vertexId, randomDouble)
      val randomValues: VertexRDD[Double] = g.vertices.mapValues { (vid, label) =>
        if (label == 0) math.random else -1.0 // If already decided, use -1.0 (to exclude them from competition)
      }

      // Join these random values into the graph as a new vertex property ( (oldLabel, randomValue) )
      val g2 = g.outerJoinVertices(randomValues) {
        case (_, currentLabel, randOpt) =>
          (currentLabel, randOpt.getOrElse(-1.0)) // store tuple: (label, random)
      }

      // Step (b): Identify vertices that "win" among their neighbors
      //  - We gather the maximum random among neighbors that are still 0-labeled.
      //  - Then compare a vertex's own random with that max neighbor value.
      val maxNeighborRandom = g2.aggregateMessages[Double](
        sendMsg = triplet => {
          val (srcLabel, srcRand) = triplet.srcAttr
          val (dstLabel, dstRand) = triplet.dstAttr

          // Only compare among undecided (label==0) vertices
          if (srcLabel == 0 && dstLabel == 0) {
            // Send my random to neighbor
            triplet.sendToSrc(dstRand)
            triplet.sendToDst(srcRand)
          }
        },
        mergeMsg = (a, b) => math.max(a, b) // we want the maximum random from neighbors
      )

      // Now decide: if a vertex's own random >= max neighbor random (and label=0), it "wins"
      val newVertices = g2.vertices.leftJoin(maxNeighborRandom) {
        case (vid, (oldLabel, myRand), maxRandOpt) =>
          if (oldLabel != 0) {
            // Already decided, keep label
            oldLabel
          } else {
            // If I'm still 0-labeled, compare myRand to maxRand
            val maxRand = maxRandOpt.getOrElse(-1.0)
            if (myRand >= maxRand) 1 else 0  // 1 => in MIS, 0 => still 0-labeled
          }
      }

      // Build a temporary graph with updated "winners"
      var tempG = Graph(newVertices, g2.edges)

      // Step (c): Any neighbor of a newly chosen MIS vertex is forced to label -1 if it was 0
      // We'll aggregate again: if I have a neighbor labeled 1, I become -1 (unless I'm already decided).
      val markNotInMIS = tempG.aggregateMessages[Boolean](
        sendMsg = triplet => {
          // If src is newly labeled 1, we inform dst if it's 0
          if (triplet.srcAttr == 1 && triplet.dstAttr == 0) {
            triplet.sendToDst(true)
          }
          // If dst is newly labeled 1, we inform src if it's 0
          if (triplet.dstAttr == 1 && triplet.srcAttr == 0) {
            triplet.sendToSrc(true)
          }
        },
        mergeMsg = (a, b) => a || b
      )

      // Update labels: those informed become -1
      val finalVertices = tempG.vertices.leftJoin(markNotInMIS) {
        case (_, currentLabel, informedOpt) =>
          if (currentLabel == 0 && informedOpt.getOrElse(false)) -1
          else currentLabel
      }

      // Update the graph for the next iteration
      g = Graph(finalVertices, tempG.edges)

      // Count how many remain with label=0
      oldActiveVertices = activeVertices
      activeVertices = g.vertices.filter { case (_, label) => label == 0 }.count()
    }

    // At the end, g contains:
    // 1 => in MIS
    // -1 => not in MIS
    // 0 => if any remain, they trivially have no conflicting edges, but usually it means
    //      they also should be in MIS or cannot be (depending on random tie). For correctness,
    //      we can quickly label them as -1 or 1. But typically this won't happen if the code
    //      converges properly.

    // In practice, some final cleanup can ensure no leftover 0's remain.
    // Any 0-labeled vertex can safely be put in the MIS (it has no neighbor that forced it out).
    // But to be consistent, let's do a quick pass turning leftover 0 into 1:
    val cleanedVertices = g.vertices.mapValues { (_, label) =>
      if (label == 0) 1 else label
    }
    Graph(cleanedVertices, g.edges)
  }

  /**
   * Verify that a given labeling (1 => in MIS, -1 => not in MIS) is indeed a
   * Maximal Independent Set:
   *
   * 1) Independence: No edge should connect two vertices both labeled 1.
   * 2) Maximality: Any vertex labeled -1 must have at least one neighbor labeled 1,
   *    otherwise you could add it to the set without conflict.
   */
  def verifyMIS(g_in: Graph[Int, Int]): Boolean = {
    // 1) Check independence
    //    If there's any edge where both endpoints have label 1, it fails.
    val invalidEdgeCount = g_in.triplets.filter { trip =>
      trip.srcAttr == 1 && trip.dstAttr == 1
    }.count()
    if (invalidEdgeCount > 0) {
      return false
    }

    // 2) Check maximality
    //    For every vertex with label -1, we require that it has at least one neighbor labeled 1.
    //    If we find a -1-labeled vertex with no neighbor labeled 1, it fails.
    val hasNeighborInMIS = g_in.aggregateMessages[Boolean](
      triplet => {
        // If src is in MIS and dst == -1, inform dst:
        if (triplet.srcAttr == 1 && triplet.dstAttr == -1) {
          triplet.sendToDst(true)
        }
        // If dst is in MIS and src == -1, inform src:
        if (triplet.dstAttr == 1 && triplet.srcAttr == -1) {
          triplet.sendToSrc(true)
        }
      },
      // mergeMsg
      (a, b) => a || b
    )

    // Now check if any -1 vertex hasNeighborInMIS == false
    val problematicVertices = g_in.vertices.leftJoin(hasNeighborInMIS) {
      case (_, label, maybeNeighborHas1) => (label, maybeNeighborHas1.getOrElse(false))
    }.filter {
      // Condition: label == -1 but no neighbor in MIS
      case (_, (lbl, hasNeighbor)) => lbl == -1 && hasNeighbor == false
    }.count()

    if (problematicVertices > 0) {
      // Fails maximality
      return false
    }

    // If we pass both tests => valid MIS
    true
  }

  /**
   * Main function handling "compute" and "verify" modes.
   * Usage:
   *   spark-submit --class project_3.main --master local[*] target/scala-2.12/project_3_2.12-1.0.jar compute [graph_path] [output_path]
   *   spark-submit --class project_3.main --master local[*] target/scala-2.12/project_3_2.12-1.0.jar verify [graph_path] [MIS_path]
   */
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("project_3")
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder.config(conf).getOrCreate()

    if(args.length == 0) {
      println("Usage: project_3 option = {compute, verify}")
      sys.exit(1)
    }

    if(args(0) == "compute") {
      if(args.length != 3) {
        println("Usage: project_3 compute graph_path output_path")
        sys.exit(1)
      }
      val startTimeMillis = System.currentTimeMillis()

      // Read edges: each line has something like "srcId,dstId"
      val edges: RDD[Edge[Int]] = sc.textFile(args(1))
        .map(line => {
          val x = line.split(",")
          Edge(x(0).toLong, x(1).toLong , 1)
        })

      // Create a graph with default vertex value = 0
      val g: Graph[Int, Int] = Graph.fromEdges[Int, Int](
        edges,
        defaultValue = 0,
        edgeStorageLevel = StorageLevel.MEMORY_AND_DISK,
        vertexStorageLevel = StorageLevel.MEMORY_AND_DISK
      )

      // Compute MIS using Luby
      val g2 = LubyMIS(g)

      val endTimeMillis = System.currentTimeMillis()
      val durationSeconds = (endTimeMillis - startTimeMillis) / 1000
      println("==================================")
      println(s"Luby's algorithm completed in $durationSeconds seconds.")
      println("==================================")

      // Export the result as (vertexId, label)
      // label = 1 for in MIS, -1 for not in MIS
      val g2df = spark.createDataFrame(g2.vertices)
        .toDF("vertexId", "label")

      // Save as CSV (coalesce to 1 file for convenience)
      g2df.coalesce(1).write.format("csv").mode("overwrite").save(args(2))
    }

    else if(args(0) == "verify") {
      if(args.length != 3) {
        println("Usage: project_3 verify graph_path MIS_path")
        sys.exit(1)
      }

      // Read the edges
      val edges: RDD[Edge[Int]] = sc.textFile(args(1))
        .map(line => {
          val x = line.split(",")
          Edge(x(0).toLong, x(1).toLong , 1)
        })

      // Read the vertex labels
      // Each line has something like "vertexId,label"
      val vertices: RDD[(VertexId, Int)] = sc.textFile(args(2))
        .map(line => {
          val x = line.split(",")
          (x(0).toLong, x(1).toInt)
        })

      // Build the graph with these labels
      val g: Graph[Int, Int] = Graph[Int, Int](
        vertices,
        edges,
        edgeStorageLevel = StorageLevel.MEMORY_AND_DISK,
        vertexStorageLevel = StorageLevel.MEMORY_AND_DISK
      )

      // Verify the labeling
      val ans = verifyMIS(g)
      if(ans) println("Yes")
      else    println("No")
    }

    else {
      println("Usage: project_3 option = {compute, verify}")
      sys.exit(1)
    }
  }
}
