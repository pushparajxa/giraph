/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.giraph.examples.jabeja;

import java.io.IOException;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;

public class SimpleComputaion
    extends
    BasicComputation<LongWritable, NullWritable, NullWritable, SimpleComputationMessage> {

  @Override
  public void compute(Vertex<LongWritable, NullWritable, NullWritable> vertex,
      Iterable<SimpleComputationMessage> messages) throws IOException {

    if (getSuperstep() == 1)
      vertex.voteToHalt();
    else if (getSuperstep() == 0) {
// Send message to destination vertex of each edge, so as to activate vertices
// which do not have any outgoing edges
      for (Edge<LongWritable, NullWritable> e : vertex.getEdges()) {
        sendMessage((LongWritable) e.getTargetVertexId(),
            new SimpleComputationMessage());
      }
    }

  }

}
