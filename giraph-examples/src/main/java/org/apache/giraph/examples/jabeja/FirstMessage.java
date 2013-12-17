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

import java.util.HashMap;

import org.apache.hadoop.io.LongWritable;

/**
 * This is message is used in SuperStep 1
 * 
 * @author Pushparaj
 * 
 */
public class FirstMessage extends Message {
  /**
   * Contains the edges
   */
  private HashMap<Long, JabejaEdge> edges;

  public FirstMessage(LongWritable id, HashMap<Long, JabejaEdge> outEdges2) {
    setVertexId(id.get());
    setEdges(outEdges2);
  }

  public HashMap<Long, JabejaEdge> getEdges() {
    return edges;
  }

  public void setEdges(HashMap<Long, JabejaEdge> edges) {
    this.edges = edges;
  }

}
