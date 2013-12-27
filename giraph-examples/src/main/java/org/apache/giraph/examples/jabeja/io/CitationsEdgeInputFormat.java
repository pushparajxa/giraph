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
import java.util.Random;
import java.util.regex.Pattern;

import org.apache.giraph.io.EdgeReader;
import org.apache.giraph.io.formats.IntNullTextEdgeInputFormat;
import org.apache.giraph.io.formats.TextEdgeInputFormat;
import org.apache.giraph.utils.IntPair;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/*
 * Takes an input file in the format source_vertex end_vertex
 */
public class CitationsEdgeInputFormat extends
    TextEdgeInputFormat<LongWritable, IntWritable> {
  /** Splitter for endpoints */
  private static final Pattern SEPARATOR = Pattern.compile("[\t ]");
  private int totalNumberOfColors = 2;
  private Random randomGenerator;

  @Override
  public EdgeReader<LongWritable, IntWritable> createEdgeReader(
      InputSplit split, TaskAttemptContext context) throws IOException {
    totalNumberOfColors = getConf().getInt("JaBeJa.NumberOfColors",
        totalNumberOfColors);
    randomGenerator = new Random(totalNumberOfColors);
    return new CitationsEdgeReader();
  }

  /**
   * {@link org.apache.giraph.io.EdgeReader} associated with
   * {@link IntNullTextEdgeInputFormat}.
   */
  public class CitationsEdgeReader extends
      TextEdgeReaderFromEachLineProcessed<IntPair> {
    @Override
    protected IntPair preprocessLine(Text line) throws IOException {
      String[] tokens = SEPARATOR.split(line.toString());
      return new IntPair(Integer.valueOf(tokens[0]), Integer.valueOf(tokens[1]));
    }

    @Override
    protected LongWritable getSourceVertexId(IntPair endpoints)
        throws IOException {
      return new LongWritable(endpoints.getFirst());
    }

    @Override
    protected LongWritable getTargetVertexId(IntPair endpoints)
        throws IOException {
      return new LongWritable(endpoints.getSecond());
    }

    @Override
    protected IntWritable getValue(IntPair endpoints) throws IOException {
      int ran = randomGenerator.nextInt();
      if (ran < 0) {
        ran = -ran;
      }
      return new IntWritable(ran % totalNumberOfColors);
    }
  }
}
