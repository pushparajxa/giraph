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
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;

import java.io.IOException;
import java.util.Random;

/**
 * Implement the original JaBeJa-Algorithm
 * (https://www.sics.se/~amir/files/download/papers/jabeja.pdf)
 */
public class NodePartitioningComputation extends
        BasicComputation<LongWritable, NodePartitioningVertexData,
                NullWritable, LongWritable> {
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
  private Vertex<LongWritable, NodePartitioningVertexData, NullWritable> vertex;

  @Override
  public void compute(
          Vertex<LongWritable, NodePartitioningVertexData,
                  NullWritable> vertex,
          Iterable<LongWritable> messages) throws IOException {
    this.vertex = vertex;

    if (super.getSuperstep() == 0) {
      initializeColor();
    }

    int mode = (int) (super.getSuperstep() % 4);

    switch (mode) {
    case 0:
      askForAColor(vertex);
      break;
    default:
    }
  }

  /**
   * send a message to a random node to ask for its color
   *
   * @param vertex - the current vertex
   */
  private void askForAColor(
          Vertex<LongWritable, NodePartitioningVertexData, NullWritable>
                  vertex) {
    int randomEdgeIndex = randomGenerator.nextInt(vertex.getNumEdges());

    for (Edge<LongWritable, NullWritable> edge : vertex.getEdges()) {
      if (randomEdgeIndex == 0) {
        sendMessage(edge.getTargetVertexId(), new LongWritable(1));
        break;
      } else {
        randomEdgeIndex--;
      }
    }
  }

  /**
   * Set the color of the own node.
   */
  private void initializeColor() {
    int numberOfColors = getNumberOfColors();
    int myColor = randomGenerator.nextInt(numberOfColors);

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
    int configuredSeed = super.getConf().getInt("JaBeJa.RandomSeed", -1);

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
