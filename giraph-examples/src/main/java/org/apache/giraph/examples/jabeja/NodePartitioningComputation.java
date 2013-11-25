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
package org.apache.giraph.examples.jabeja;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;

import java.io.IOException;
import java.util.Random;

/**
 * Implement the original JaBeJa-Algorithm
 * (https://www.sics.se/~amir/files/download/papers/jabeja.pdf)
 */
public class NodePartitioningComputation extends
        BasicComputation<LongWritable, NodePartitioningVertexData,
                IntWritable, Message> {
  /**
   * The default number of different colors, if no
   * JaBeJa.NumberOfColors is provided.
   */
  private static final int DEFAULT_NUMBER_OF_COLORS = 2;

  /**
   * Since the JaBeJa algorithm is a Monte Carlo algorithm.
   */
  private Random randomGenerator = new Random();
  /**
   * The currently processed vertex
   */
  private Vertex<LongWritable, NodePartitioningVertexData, IntWritable> vertex;

  @Override
  public void compute(
          Vertex<LongWritable, NodePartitioningVertexData,
                  IntWritable> vertex,
          Iterable<Message> messages) throws IOException {
    this.vertex = vertex;

    if (super.getSuperstep() == 0) {
      initializeColor();
    }

    int mode = (int) (super.getSuperstep());

    switch (mode) {
    case 0:
      announceColor();
      break;
    case 1:
      storeColorsOfNodes(messages);
      replyWithColor(messages);
      break;
    case 2:
      storeColorsOfNodes(messages);
      this.vertex.voteToHalt();
      break;
    default:
      this.vertex.voteToHalt();
    }
  }

  /**
   * Store the color of all neighboring nodes. Neighboring through outgoing
   * as well as incoming edges.
   *
   * @param messages received messages from nodes with their colors.
   */
  private void storeColorsOfNodes(Iterable<Message> messages) {
    for (Message msg : messages) {
      this.vertex.getValue().setNeighborWithColor(msg.getVertexId(),
              msg.getColor());
    }
  }

  /**
   * Reply to all received messages with the current color.
   *
   * @param messages all received messages.
   */
  private void replyWithColor(Iterable<Message> messages) {
    for (Message msg : messages) {
      sendCurrentVertexColor(new LongWritable(msg.getVertexId()));
    }
  }

  /**
   * Announce the current color to all connected vertices.
   */
  private void announceColor() {
    for (Edge<LongWritable, IntWritable> edge : vertex.getEdges()) {
      sendCurrentVertexColor(edge.getTargetVertexId());
    }
  }

  /**
   * Send a message to a vertex with the current color and id,
   * so that this vertex would be able to reply.
   *
   * @param targetId id of the vertex to which the message should be sent
   */
  private void sendCurrentVertexColor(LongWritable targetId) {
    super.sendMessage(targetId, new Message(this.vertex.getId().get(),
            this.vertex.getValue()
                    .getNodeColor()));
  }

  /**
   * Set the color of the own node.
   */
  private void initializeColor() {
    int numberOfColors = getNumberOfColors();
    int myColor = (int) getRandomNumber(numberOfColors);

    this.vertex.getValue().setNodeColor(myColor);
  }

  /**
   * @return the configured or default number of different colors in
   *         the current graph.
   */
  private int getNumberOfColors() {
    return super.getConf().getInt("JaBeJa.NumberOfColors",
            DEFAULT_NUMBER_OF_COLORS);
  }

  /**
   * generates a 64bit random number within an upper boundary.
   *
   * @param exclusiveMaxValue the exclusive maximum value of the to be
   *                          generated random value
   * @return a random number between 0 (inclusive) and exclusiveMaxValue
   *         (exclusive)
   */
  private long getRandomNumber(long exclusiveMaxValue) {
    if (this.randomGenerator == null) {
      initializeRandomGenerator();
    }
    return (long) (this.randomGenerator.nextDouble() * exclusiveMaxValue);
  }

  /**
   * Initializes the random generator, either with the default constructor or
   * with a seed which will guarantee, that the same vertex in the same round
   * will always generate the same random values.
   */
  private void initializeRandomGenerator() {
    int configuredSeed = super.getConf().getInt("JaBeJa.RandomSeed", 0);

    // if you want to have a totally random number just set the config to -1
    if (configuredSeed == -1) {
      this.randomGenerator = new Random();
    } else {
      long seed = calculateHashCode(String.format("%d#%d#%d", configuredSeed,
              super.getSuperstep(),
              this.vertex.getId().get()));
      this.randomGenerator = new Random(seed);
    }
  }

  /**
   * Based on the hashCode function defined in {@link java.lang.String},
   * with the difference that it returns a long instead of only an integer.
   *
   * @param value the String for which the hash-code should be calculated
   * @return a 64bit hash-code
   */
  private long calculateHashCode(String value) {
    int hashCode = 0;

    for (int i = 0; i < value.length(); i++) {
      hashCode = 31 * hashCode + value.charAt(i);
    }

    return hashCode;
  }
}
