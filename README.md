# Large Scale Data Processing: Project 3

1. **(4 points)** Implement the `verifyMIS` function. The function accepts a Graph[Int, Int] object as its input. Each vertex of the graph is labeled with 1 or -1, indicating whether or not a vertex is in the MIS. `verifyMIS` should return `true` if the labeled vertices form an MIS and `false` otherwise. To execute the function, run the following:
```
// Linux
spark-submit --class project_3.main --master local[*] target/scala-2.12/project_3_2.12-1.0.jar verify [path_to_graph] [path_to_MIS]

// Unix
spark-submit --class "project_3.main" --master "local[*]" target/scala-2.12/project_3_2.12-1.0.jar verify [path_to_graph] [path_to_MIS]
```

Implementation of `verifyMIS`
```scala
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

```
Apply `verifyMIS` locally with the parameter combinations listed in the table below and **fill in all blanks**.
|        Graph file       |           MIS file           | Is an MIS? |
| ----------------------- | ---------------------------- | ---------- |
| small_edges.csv         | small_edges_MIS.csv          | Yes        |
| small_edges.csv         | small_edges_non_MIS.csv      | No         |
| line_100_edges.csv      | line_100_MIS_test_1.csv      | Yes          |
| line_100_edges.csv      | line_100_MIS_test_2.csv      | No          |
| twitter_10000_edges.csv | twitter_10000_MIS_test_1.csv | No          |
| twitter_10000_edges.csv | twitter_10000_MIS_test_2.csv | Yes          |




2. **(3 points)** Implement the `LubyMIS` function. The function accepts a Graph[Int, Int] object as its input. You can ignore the two integers associated with the vertex RDD and the edge RDD as they are dummy fields. `LubyMIS` should return a Graph[Int, Int] object such that the integer in a vertex's data field denotes whether or not the vertex is in the MIS, with 1 signifying membership and -1 signifying non-membership. The output will be written as a CSV file to the output path you provide. To execute the function, run the following:
```
// Linux
spark-submit --class project_3.main --master local[*] target/scala-2.12/project_3_2.12-1.0.jar compute [path_to_input_graph] [path_for_output_graph]

// Unix
spark-submit --class "project_3.main" --master "local[*]" target/scala-2.12/project_3_2.12-1.0.jar compute [path_to_input_graph] [path_for_output_graph]
```
Apply `LubyMIS` locally on the graph files listed below and report the number of iterations and running time that the MIS algorithm consumes for **each file**. You may need to include additional print statements in `LubyMIS` in order to acquire this information. Finally, verify your outputs with `verifyMIS`.
|        Graph file       |  Runtime  | Number of Iteration|
| ----------------------- |  -------- |--------------------|
| small_edges.csv         |  2.40s    |1|
| line_100_edges.csv      |  2.95s    |2|
| twitter_100_edges.csv   |  3.11s    |2|
| twitter_1000_edges.csv  |  3.81s     |3|
| twitter_10000_edges.csv |  5.19s     |3|

Trial 2
|        Graph file       |  Runtime  | Number of Iteration|
| ----------------------- |  -------- |--------------------|
| small_edges.csv         |  1.53s    |1|
| line_100_edges.csv      |  1.76s    |2|
| twitter_100_edges.csv   |  1.78s    |2|
| twitter_1000_edges.csv  |  2.03s     |3|
| twitter_10000_edges.csv |  2.89s     |3|

3. **(3 points)**  
a. Run `LubyMIS` on `twitter_original_edges.csv` in GCP with 3x4 cores (vCPUs). Report the number of iterations, running time, and remaining active vertices (i.e. vertices whose status has yet to be determined) at the end of **each iteration**. You may need to include additional print statements in `LubyMIS` in order to acquire this information. Finally, verify your outputs with `verifyMIS`.

###Luby's Algorithm Output Summary

**Cluster Configuration for 3x4 cores**

```scala
gcloud dataproc clusters create n2-3x4\
    --region=us-central1 \
    --master-machine-type=n2-standard-2 \
    --worker-machine-type=n2-standard-4 \
    --num-workers=3 \
    --image-version=2.2-debian12 \
    --max-idle=2h \
    --enable-component-gateway \
    --no-address
```


1. **Iterations and Active Vertices**

| **Iteration** | **Active Vertices** |
|---------------|---------------------|
| 1             | 6,551,624          |
| 2             | 34,433             |
| 3             | 514                |
| 4             | 6                  |
| 5             | 0                  |

By **Iteration 5**, the number of undecided (active) vertices reached **0**, indicating the algorithm converged.

2. **Timing**

- **Algorithm Time**: 262 seconds  
  (This is the duration spent inside the Luby’s MIS method.)
- **Total Runtime (including I/O)**: 293.70 seconds  
  (This includes loading the input file, saving the output, and any overhead.)

It is indeed a MIS, comfirmed by verify MIS function. 


b. Run `LubyMIS` on `twitter_original_edges.csv` with 4x2 cores (vCPUs) and then 2x2 cores (vCPUs). Compare the running times between the 3 jobs with varying core specifications that you submitted in **3a** and **3b**.

## Submission via GitHub
Delete your project's current **README.md** file (the one you're reading right now) and include your report as a new **README.md** file in the project root directory. Have no fear—the README with the project description is always available for reading in the template repository you created your repository from. For more information on READMEs, feel free to visit [this page](https://docs.github.com/en/github/creating-cloning-and-archiving-repositories/about-readmes) in the GitHub Docs. You'll be writing in [GitHub Flavored Markdown](https://guides.github.com/features/mastering-markdown). Be sure that your repository is up to date and you have pushed all changes you've made to the project's code. When you're ready to submit, simply provide the link to your repository in the Canvas assignment's submission.

## You must do the following to receive full credit:
1. Create your report in the ``README.md`` and push it to your repo.
2. In the report, you must include your (and your group member's) full name in addition to any collaborators.
3. Submit a link to your repo in the Canvas assignment.

## Late submission penalties
Please refer to the course policy.
