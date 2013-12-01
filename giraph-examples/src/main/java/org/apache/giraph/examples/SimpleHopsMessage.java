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
package org.apache.giraph.examples;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Message-Type used by {@link org.apache.giraph.examples
 * .SimpleHopsComputation} for communication.
 */
public class SimpleHopsMessage implements Writable {
  /**
   * the id of the vertex initiating this message.
   */
  private long sourceId;
  /**
   * the id of the vertex of the final destination of this message.
   */
  private long destinationId;
  /**
   * the current number of hops, the message had to take.
   */
  private int hopsCount = 0;
  /**
   * Flag, if the destination has been found, and this message is an answer.
   */
  private boolean destinationFound = false;

  /**
   * Default constructor for reflection
   */
  public SimpleHopsMessage() {
  }

  /**
   * Constructor used by {@link org.apache.giraph.examples
   * .SimpleHopsComputation}
   *
   * @param sourceId      the id of the source vertex which wants to
   *                      calculate the hops count
   * @param destinationId the id of the destination vertex between which the
   *                      hops count will be calculated
   */
  public SimpleHopsMessage(long sourceId, long destinationId) {
    this.sourceId = sourceId;
    this.destinationId = destinationId;
  }

  /**
   * Increments the <code>hopsCount</code> by one. Usually when the message
   * arrives at a new node.
   */
  public void incrementHopsCount() {
    this.hopsCount++;
  }

  /**
   * Sets the flag <code>destinationFound</code>. Do this before sending the
   * message back to the source.
   */
  public void setDestinationFound() {
    this.destinationFound = true;
  }

  public boolean isDestinationFound() {
    return this.destinationFound;
  }

  public long getSourceId() {
    return this.sourceId;
  }

  public long getDestinationId() {
    return this.destinationId;
  }

  public int getHopsCount() {
    return this.hopsCount;
  }

  @Override
  public void write(DataOutput dataOutput) throws IOException {
    dataOutput.writeLong(this.sourceId);
    dataOutput.writeLong(this.destinationId);
    dataOutput.writeInt(this.hopsCount);
    dataOutput.writeBoolean(this.destinationFound);
  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {
    this.sourceId = dataInput.readLong();
    this.destinationId = dataInput.readLong();
    this.hopsCount = dataInput.readInt();
    this.destinationFound = dataInput.readBoolean();
  }
}