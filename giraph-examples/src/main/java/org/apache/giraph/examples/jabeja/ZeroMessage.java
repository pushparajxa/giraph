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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;

/**
 * This message is used in super step 0.
 */
public class ZeroMessage extends Message {
  /**
   * The edge information this message carrying
   */
  private JabejaEdge edge;

  /**
   * Construtor with JabejaEdge and source vertex sending the message
   * 
   * @param longWritable
   * @param jabejaEdge
   */
  public ZeroMessage(LongWritable l, JabejaEdge jabejaEdge) {
    setVertexId(l.get());
    this.setEdge(jabejaEdge);
  }

  /**
   * return the JabejaEdge associsated with this
   * 
   * @return
   */
  public JabejaEdge getEdge() {
    return edge;
  }

  /**
   * Set the this Object's Jabeja Edge object
   * 
   * @param edge
   */
  public void setEdge(JabejaEdge edge) {
    this.edge = edge;
  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {
    setVertexId(dataInput.readLong());
    // int typeValue = dataInput.readInt();
    edge = new JabejaEdge(new LongWritable(dataInput.readLong()),
        new LongWritable(dataInput.readLong()), new IntWritable(
            dataInput.readInt()));
    // readNeighboringColorRatio(dataInput);

  }

  @Override
  public void write(DataOutput dataOutput) throws IOException {
    dataOutput.writeLong(getVertexId());
    dataOutput.writeLong(edge.sourceVetex.get());
    dataOutput.writeLong(edge.destVertex.get());
    dataOutput.writeInt(edge.color.get());
  }
}
