import org.apache.spark.graphx.{EdgeTriplet, Graph, VertexId}

object ComputeAtRisk {

  /**
   * Computes who's at risk as they got in contact with someone who was infected or at-risk themselves.
   *
   * @param infectedGraph A graph with as the vertices a tuple for each user, this tuple consists of user data U
   *                      and a map which tracks the infection contact.
   *                      <br/>
   *                      The infection contact is stored as a map, which has as keys the distance from infection
   *                      (0 if the user is infected themselves) and as values the earliest time this user got into contact
   *                      on this distance.
   *                      <br/>
   *                      The edges of the graph represent the connections between users, with as attribute the timestamp
   *                      at which this connection occurred.
   * @param maxDepth      (Optional) The maximum level of indirect infection which should be propagated. This is inclusive,
   *                      so e.g. setting it to 3 will allow the maximum infection depth to be detected to be 3.
   * @return A graph with the infection contact vertex attributes being more complete.
   */
  def apply[U](infectedGraph: Graph[(U, Map[Int, Long]), VertexId], maxDepth: Int = Int.MaxValue): Graph[(U, Map[Int, Long]), VertexId] = {
    val initialMessage: (Int, Long) = (-1, -1L)

    def handleIncomingMessage = (_: VertexId, vertexAttribute: (U, Map[Int, Long]), message: (Int, Long)) => {
      val (vertexData, infectionData) = vertexAttribute

      if (message == initialMessage) {
        // Drop the message
        (vertexData, infectionData)
      } else {
        // Overwrite the entry in the infectionData
        val previousInfectionTimestamp = infectionData.getOrElse(message._1, Long.MaxValue)
        val lowestInfectionTimestamp = message._2.min(previousInfectionTimestamp)
        val updatedInfectionData = infectionData.+(message._1 -> lowestInfectionTimestamp)
        (vertexData, updatedInfectionData)
      }
    }

    def emitMessages = (triplet: EdgeTriplet[(U, Map[Int, VertexId]), VertexId]) => {
      val dstMap = triplet.dstAttr._2
      val srcMap = triplet.srcAttr._2
      val contactTime = triplet.attr

      // Iterate through all our infection levels and emit messages if they would
      // impact the receiving vertice
      srcMap
        // Only include infections prior to this contact of happening
        .filter { case (level, ts) => ts < contactTime }
        .filter { case (level, ts) => ts < maxDepth }
        // Only include infections which would mark them as at risk at an earlier time
        .filter { case (level, ts) => dstMap.getOrElse(level + 1, Long.MaxValue) > contactTime }
        .map { case (level, ts) => triplet.dstId -> (level + 1, contactTime) }
        .toIterator
    }

    def combineMessages = (m1: (Int, VertexId), m2: (Int, VertexId)) => {
      // Prefer the message with the lowest level
      if (m1._1 < m2._1) {
        m1
      } else if (m2._1 < m1._1) {
        m2
      } else { // Else take the oldest message
        m1._1 -> m1._2.min(m2._2)
      }
    }

    infectedGraph.pregel[(Int, Long)](initialMessage, maxDepth)(
      handleIncomingMessage,
      emitMessages,
      combineMessages
    )
  }
}
