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

import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.examples.SimpleHopsVertexValue;
import org.apache.giraph.io.formats.TextVertexInputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Default input format for {@link org.apache.giraph.examples
 * .SimpleHopsComputation}
 */
public class SimpleHopsTextVertexInputFormat extends
        TextVertexInputFormat<LongWritable,
                SimpleHopsVertexValue, NullWritable> {
  @Override
  public TextVertexReader createVertexReader(
          InputSplit split, TaskAttemptContext context) throws IOException {
    return new SimpleHostVertexReaderFromEachLine();
  }

  /**
   * Reads the line and parses them by the following schema:
   * vertexID \t <code>; delimited id's for finding hop counts</code> \t
   * <code>; delimited id's of neighbors</code>
   */
  private class SimpleHostVertexReaderFromEachLine extends
          TextVertexReaderFromEachLine {

    @Override
    protected LongWritable getId(Text line) throws IOException {
      String[] splitLine = line.toString().split("\t");
      long id = Long.parseLong(splitLine[0]);

      return new LongWritable(id);
    }

    @Override
    protected SimpleHopsVertexValue getValue(Text line) throws IOException {
      SimpleHopsVertexValue value = new SimpleHopsVertexValue();
      String[] splitLine = line.toString().split("\t");
      String[] vertexIds = splitLine[1].split(";");
      List<Long> parsedVertexIds = new ArrayList<Long>();

      for (int i = 0; i < vertexIds.length; i++) {
        parsedVertexIds.add(Long.parseLong(vertexIds[i]));
      }
      value.initializeVerticesWithHopsCount(parsedVertexIds);

      return value;
    }

    @Override
    protected Iterable<Edge<LongWritable, NullWritable>> getEdges(
            Text line) throws IOException {
      String[] splitLine = line.toString().split("\t");
      String[] connectedVertexIds = splitLine[2].split(";");
      List<Edge<LongWritable, NullWritable>> edges = new
              ArrayList<Edge<LongWritable, NullWritable>>();

      for (int i = 0; i < connectedVertexIds.length; i++) {
        long targetId = Long.parseLong(connectedVertexIds[i]);
        edges.add(EdgeFactory.create(new LongWritable(targetId)));
      }

      return edges;
    }
  }
}
