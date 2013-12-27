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

import java.io.IOException;

import org.apache.giraph.examples.jabeja.NodePartitioningVertexData;
import org.apache.giraph.io.formats.IntIntTextVertexValueInputFormat;
import org.apache.giraph.io.formats.TextVertexValueInputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class CitationsVertexInputFormat
    extends
    TextVertexValueInputFormat<LongWritable, NodePartitioningVertexData, IntWritable> {

  @Override
  public TextVertexValueReader createVertexValueReader(InputSplit split,
      TaskAttemptContext context) throws IOException {
    return new CitationsVertexReader();
  }

  /**
   * {@link org.apache.giraph.io.VertexValueReader} associated with
   * {@link IntIntTextVertexValueInputFormat}.
   */
  public class CitationsVertexReader extends
      TextVertexValueReaderFromEachLineProcessed<Integer> {

    @Override
    protected Integer preprocessLine(Text line) throws IOException {
      return Integer.valueOf(Integer.parseInt(line.toString()));
    }

    @Override
    protected LongWritable getId(Integer data) throws IOException {
      return new LongWritable(data);
    }

    @Override
    protected NodePartitioningVertexData getValue(Integer data)
        throws IOException {
      return new NodePartitioningVertexData();
    }
  }
}
