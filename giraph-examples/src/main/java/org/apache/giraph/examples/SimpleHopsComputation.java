/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.giraph.examples;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;

import java.io.IOException;

/**
 * Calculates how many hops there are between different vertices in the graph.
 * Hops could be also seen as the degree of separation.
 */
public class SimpleHopsComputation extends BasicComputation<LongWritable,
        SimpleHopsVertexValue, NullWritable, SimpleHopsMessage> {
  /**
   * The maximal degree of separation in our graphs. If a hops cannot be
   * calculated within 20 steps, the vertices probably aren't connected.
   */
  private static final int MAX_DEGREE_OF_SEPARATION = 20;

  /**
   * The current vertex
   */
  private Vertex<LongWritable, SimpleHopsVertexValue, NullWritable> vertex;

  @Override
  public void compute(
          Vertex<LongWritable, SimpleHopsVertexValue, NullWritable> vertex,
          Iterable<SimpleHopsMessage> messages) throws IOException {
    this.vertex = vertex;

    if (super.getSuperstep() == 0) {
      askForNumberOfHops();
    }
    processMessages(messages);

    if (isTimeToStop()) {
      this.vertex.voteToHalt();
    }
  }

  /**
   * Each vertex has a list of vertices to which it wants to calculate the
   * number of hops. It does that by sending a message to all its neighbors.
   */
  private void askForNumberOfHops() {
    for (Edge<LongWritable, NullWritable> edge : this.vertex.getEdges()) {
      if (isLoopBack(edge)) {
        continue; // skip loop back to current vertex
      }

      for (Long vertexId : this.vertex.getValue().getVertices()) {
        super.sendMessage(new LongWritable(edge.getTargetVertexId().get()),
                new SimpleHopsMessage(this.vertex.getId().get(), vertexId));
      }
    }
  }

  /**
   * Since each vertex sends messages, they will also receive some from
   * others. A message can be either answered, forwarded,
   * skipped or simply stored because it is already the answer with the
   * number of hops.
   *
   * @param messages messages received by the vertex
   */
  private void processMessages(Iterable<SimpleHopsMessage> messages) {
    for (SimpleHopsMessage message : messages) {
      if (message.isDestinationFound()) {
        this.vertex.getValue().updateHopsCounts(message);
      } else {
        replyOrForwardCurrentMessage(message);
      }
    }
  }

  /**
   * If the message wasn't an answer, we check if we have already processed
   * such a message, in that case we will ignore it, otherwise we will answer
   * it, if it was for us, or simply forward to our neighbors.
   *
   * @param message the message received from one of our neighbors
   */
  private void replyOrForwardCurrentMessage(SimpleHopsMessage message) {
    if (this.vertex.getValue().hasProcessed(message)) {
      return;
    }
    this.vertex.getValue().isProcessing(message);
    message.incrementHopsCount();

    if (isCurrentVertexDestination(message)) {
      reply(message);
    } else {
      forward(message);
    }
  }

  /**
   * If the message wasn't sent to the current vertex, we will forward it to
   * all our neighbors.
   *
   * @param message the received message which will be forwarded.
   */
  private void forward(SimpleHopsMessage message) {
    for (Edge<LongWritable, NullWritable> edge : this.vertex.getEdges()) {
      if (isLoopBack(edge)) {
        continue; // skip loop back to current vertex
      }

      super.sendMessage(edge.getTargetVertexId(), message);
    }
  }

  /**
   * Check if an Edge is a loop back edge back to the current vertex. It
   * isn't necessary to send a message to myself.
   *
   * @param edge the edge to be checked
   * @return true if the targetID is the id of the current vertex
   */
  private boolean isLoopBack(Edge<LongWritable, NullWritable> edge) {
    return edge.getTargetVertexId().equals(this.vertex.getId());
  }

  /**
   * If a messages has finally reached it's destination and it's the current
   * vertex, than set the destinationFound-flag and send it back to the source.
   *
   * @param message the message which needs an answer
   */
  private void reply(SimpleHopsMessage message) {
    message.setDestinationFound();
    super.sendMessage(new LongWritable(message.getSourceId()), message);
  }

  /**
   * Check if the current vertex is the final destination for the message
   *
   * @param message the messages received
   * @return true if the destinationId of the message is the id of the
   *         current vertex, otherwise false
   */
  private boolean isCurrentVertexDestination(SimpleHopsMessage message) {
    return message.getDestinationId() == this.vertex.getId().get();
  }

  /**
   * check if it is time to stop based on the number of steps or if all hops
   * are counted.
   *
   * @return true if the number of super-steps reaches
   *         MAX_DEGREE_OF_SEPARATION or all hops were collected.
   */
  private boolean isTimeToStop() {
    return (super.getSuperstep() >= MAX_DEGREE_OF_SEPARATION) ||
           this.vertex.getValue().hasAllHopsCounts();
  }
}
