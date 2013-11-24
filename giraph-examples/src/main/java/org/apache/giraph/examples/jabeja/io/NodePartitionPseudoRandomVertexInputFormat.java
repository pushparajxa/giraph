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

import org.apache.giraph.examples.jabeja.NodePartitioningVertexData;
import org.apache.hadoop.io.IntWritable;

/**
 * Random graph generator for the NodePartitioning problem.
 */
public class NodePartitionPseudoRandomVertexInputFormat extends
        PseudoRandomVertexInputFormat<NodePartitioningVertexData, IntWritable> {
  @Override
  protected NodePartitioningVertexData getVertexValue(long vertexId) {
    return new NodePartitioningVertexData();
  }

  @Override
  protected IntWritable getEdgeValue(
          long vertexSourceId, long vertexDestinationId) {
    return new IntWritable(0);
  }
}
