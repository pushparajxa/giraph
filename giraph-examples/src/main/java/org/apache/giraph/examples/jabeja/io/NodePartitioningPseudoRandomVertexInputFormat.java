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
package org.apache.giraph.examples.jabeja.io;

import java.util.Random;

import org.apache.giraph.examples.jabeja.NodePartitioningVertexData;
import org.apache.hadoop.io.IntWritable;

/**
 * Random graph generator for the NodePartitioning problem.
 */
public class NodePartitioningPseudoRandomVertexInputFormat extends
    PseudoRandomVertexInputFormat<NodePartitioningVertexData, IntWritable> {
  /**
   * Since the JaBeJa algorithm is a Monte Carlo algorithm.
   */
  private Random randomGenerator = null;
  /**
   * Total number of colors.Default is 2.
   */
  private int totalNumberOfColors = 2;

  @Override
  protected NodePartitioningVertexData getVertexValue(long vertexId) {
    NodePartitioningVertexData n = new NodePartitioningVertexData();
    // n.setRandVertexGen(new Random(vertexId));
    return n;
  }

  @Override
  protected IntWritable getEdgeValue(long vertexSourceId,
      long vertexDestinationId) {
    if (randomGenerator == null) {
      totalNumberOfColors = getConf().getInt("JaBeJa.NumberOfColors",
          totalNumberOfColors);
      randomGenerator = new Random(totalNumberOfColors);
    }
    int color = (int) ((randomGenerator.nextDouble()) * totalNumberOfColors);
    return new IntWritable(color);
  }
}
