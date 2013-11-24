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

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Structure of messages sent between vertices
 */
public class Message implements Writable {
  /**
   * Id of the source vertex sending the message. Necessary for replies or
   * knowing your neighbors.
   */
  private long vertexId;
  /**
   * The color of the vertex sending the message.
   */
  private int color;

  /**
   * Default constructor for reflection
   */
  public Message() {
  }

  /**
   * Initializes the message
   *
   * @param vertexId the id of the vertex sending the message
   * @param color    the color of the vertex sending the message
   */
  public Message(long vertexId, int color) {
    this.vertexId = vertexId;
    this.color = color;
  }

  public long getVertexId() {
    return vertexId;
  }

  public void setVertexId(long vertexId) {
    this.vertexId = vertexId;
  }

  public int getColor() {
    return color;
  }

  public void setColor(int color) {
    this.color = color;
  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {
    this.vertexId = dataInput.readLong();
    this.color = dataInput.readInt();
  }

  @Override
  public void write(DataOutput dataOutput) throws IOException {
    dataOutput.writeLong(this.vertexId);
    dataOutput.writeInt(this.color);
  }
}
