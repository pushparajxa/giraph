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
package org.apache.giraph.examples.io.formats;

import org.apache.giraph.examples.SimpleHopsVertexValue;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.formats.TextVertexOutputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.Map;

/**
 * Default output format for {@link org.apache.giraph.examples
 * .SimpleHopsComputation}
 */
public class SimpleHopsTextVertexOutputFormat extends
        TextVertexOutputFormat<LongWritable, SimpleHopsVertexValue,
                NullWritable> {
  @Override
  public TextVertexWriter createVertexWriter(
          TaskAttemptContext context) throws IOException, InterruptedException {
    return new SimpleHopsTextVertexLineWriter();
  }

  /**
   * Outputs for each line the vertex id and the searched vertices with their
   * hop count
   */
  private class SimpleHopsTextVertexLineWriter extends
          TextVertexWriterToEachLine {

    @Override
    protected Text convertVertexToLine(
            Vertex<LongWritable, SimpleHopsVertexValue,
                    NullWritable> vertex) throws IOException {
      StringBuilder sb = new StringBuilder();

      sb.append(vertex.getValue());
      sb.append(" \t");

      for (Map.Entry<Long, Integer> entry : vertex.getValue()
              .getVerticesWithHopsCount().entrySet()) {
        sb.append(entry.getKey());
        sb.append("(");
        sb.append(entry.getValue());
        sb.append(")  ");
      }

      return new Text(sb.toString());
    }
  }
}
