import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.spark.graphx.{Edge, Graph, VertexId, VertexRDD}
import org.apache.spark.rdd.RDD
import org.scalatest.FunSuite

class TestComputeAtRisk extends FunSuite with SharedSparkContext {

  test("Nothing changes when there are no infections") {
    val vertices: RDD[(VertexId, (String, Map[Int, Long]))] = sc.parallelize(Seq(
      1L -> ("foo", Map()),
      2L -> ("bar", Map())
    ))
    val edges: RDD[Edge[Long]] = sc.parallelize(Seq(
      Edge(1L, 2L, 100L)
    ))
    val graph: Graph[(String, Map[Int, VertexId]), VertexId] = Graph(vertices, edges)

    val atRiskGraph = ComputeAtRisk(graph)

    assert(atRiskGraph.vertices.collect === graph.vertices.collect)
    assert(atRiskGraph.edges.collect === graph.edges.collect)
  }

  test("Can propagate to later connected user") {
    // Arrange
    val vertices: RDD[(VertexId, (String, Map[Int, Long]))] = sc.parallelize(Seq(
      1L -> ("foo", Map(0 -> 50L)), // 1 was infected before connecting with 2
      2L -> ("bar", Map())
    ))
    val edges: RDD[Edge[Long]] = sc.parallelize(Seq(
      Edge(1L, 2L, 100L)
    ))
    val graph: Graph[(String, Map[Int, VertexId]), VertexId] = Graph(vertices, edges)

    // Act
    val atRiskGraph = ComputeAtRisk(graph)

    // Assert
    val expectedVertices = Array(
      1L -> ("foo", Map(0 -> 50L)),
      2L -> ("bar", Map(1 -> 100L)) // 2 got marked as at-risk at the time the contact happened
    )
    assert(atRiskGraph.vertices.collect === expectedVertices)
    assert(atRiskGraph.edges.collect === graph.edges.collect)
  }

  test("Cannot propagate to earlier connected user") {
    // Arrange
    val vertices: RDD[(VertexId, (String, Map[Int, Long]))] = sc.parallelize(Seq(
      1L -> ("foo", Map(0 -> 150L)),  // 1 was infected after connecting with 2
      2L -> ("bar", Map())
    ))
    val edges: RDD[Edge[Long]] = sc.parallelize(Seq(
      Edge(1L, 2L, 100L)
    ))
    val graph: Graph[(String, Map[Int, VertexId]), VertexId] = Graph(vertices, edges)

    // Act
    val atRiskGraph = ComputeAtRisk(graph)

    // Assert
    val expectedVertices = Array(
      1L -> ("foo", Map(0 -> 150L)),
      2L -> ("bar", Map()) // 2 did not get marked as at risk
    )
    assert(atRiskGraph.vertices.collect === expectedVertices)
    assert(atRiskGraph.edges.collect === graph.edges.collect)
  }

  test("Cannot propagate against the direction of the connection") {
    // Arrange
    val vertices: RDD[(VertexId, (String, Map[Int, Long]))] = sc.parallelize(Seq(
      1L -> ("foo", Map()),
      2L -> ("bar", Map(0 -> 50L)) // Infection starts at user 2
    ))
    val edges: RDD[Edge[Long]] = sc.parallelize(Seq(
      Edge(1L, 2L, 100L)
    ))
    val graph: Graph[(String, Map[Int, VertexId]), VertexId] = Graph(vertices, edges)

    // Act
    val atRiskGraph = ComputeAtRisk(graph)

    // Assert
    val expectedVertices = Array(
      1L -> ("foo", Map()), // User 1 is not infected
      2L -> ("bar", Map(0 -> 50L))
    )
    assert(atRiskGraph.vertices.collect === expectedVertices)
    assert(atRiskGraph.edges.collect === graph.edges.collect)
  }

  test("Can propagate multiple steps") {
    // Arrange
    val vertices: RDD[(VertexId, (String, Map[Int, Long]))] = sc.parallelize(Seq(
      1L -> ("foo", Map(0 -> 50L)),
      2L -> ("bar", Map()),
      3L -> ("baz", Map())
    ))
    val edges: RDD[Edge[Long]] = sc.parallelize(Seq(
      Edge(1L, 2L, 100L),
      Edge(2L, 3L, 200L)
    ))
    val graph: Graph[(String, Map[Int, VertexId]), VertexId] = Graph(vertices, edges)

    // Act
    val atRiskGraph = ComputeAtRisk(graph)

    // Assert
    val expectedVertices = Array(
      1L -> ("foo", Map(0 -> 50L)),
      2L -> ("bar", Map(1 -> 100L)),
      3L -> ("baz", Map(2 -> 200L))
    )
    assert(atRiskGraph.vertices.collect === expectedVertices)
    assert(atRiskGraph.edges.collect === graph.edges.collect)
  }

  test("Cannot exceed defined depth") {
    // Arrange
    val vertices: RDD[(VertexId, (String, Map[Int, Long]))] = sc.parallelize(Seq(
      2L -> ("bar", Map(1 -> 100L)),
      3L -> ("baz", Map())
    ))
    val edges: RDD[Edge[Long]] = sc.parallelize(Seq(
      Edge(2L, 3L, 200L)
    ))
    val graph: Graph[(String, Map[Int, VertexId]), VertexId] = Graph(vertices, edges)

    // Act
    val atRiskGraph = ComputeAtRisk(graph, 1) // Call with the depth limited to 1

    // Assert
    val expectedVertices = Array(
      2L -> ("bar", Map(1 -> 100L)),
      3L -> ("baz", Map())
    )
    assert(atRiskGraph.vertices.collect === expectedVertices)
    assert(atRiskGraph.edges.collect === graph.edges.collect)
  }

  test("Updates time when an earlier infection traverses") {
    // Arrange
    val vertices: RDD[(VertexId, (String, Map[Int, Long]))] = sc.parallelize(Seq(
      1L -> ("foo", Map(1 -> 100L)),
      2L -> ("bar", Map(2 -> 200L))
    ))
    val edges: RDD[Edge[Long]] = sc.parallelize(Seq(
      Edge(1L, 2L, 150L)
    ))
    val graph: Graph[(String, Map[Int, VertexId]), VertexId] = Graph(vertices, edges)

    // Act
    val atRiskGraph = ComputeAtRisk(graph)

    // Assert
    val expectedVertices = Array(
      1L -> ("foo", Map(1 -> 100L)),
      2L -> ("bar", Map(2 -> 150L)) // This should match the connection occurrence time,
    )
    assert(atRiskGraph.vertices.collect === expectedVertices)
    assert(atRiskGraph.edges.collect === graph.edges.collect)
  }

}
