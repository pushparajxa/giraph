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

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;

/**
 * Class holding the source and destination vertices and the color of the edge
 * 
 * @author Pushparaj
 * 
 */
public class JabejaEdge {
  /**
   * The Source Vertex of this edge
   */
  public LongWritable sourceVetex;
  /**
   * The destination vertex of this edge.
   */
  public LongWritable destVertex;
  /**
   * The color of this edge
   */
  public IntWritable color;

  /**
   * Constructor Default.
   * 
   * @param longWritable
   * @param longWritable2
   * @param intWritable
   */
  public JabejaEdge(LongWritable longWritable, LongWritable longWritable2,
      IntWritable intWritable) {
    this.sourceVetex = longWritable;
    this.destVertex = longWritable2;
    this.color = intWritable;
  }

  public String getId() {
    return sourceVetex.toString() + "ID" + destVertex.toString();

  }
}
