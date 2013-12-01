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
import java.util.AbstractMap.SimpleEntry;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;


/**
 * Value-Type for vertices used by {@link org.apache.giraph.examples
 * .SimpleHopsComputation}
 */
public class SimpleHopsVertexValue implements Writable {
  /**
   * The vertices for which the current vertex wants to find out the hop
   * count, together with the number of hops or Integer.MAX_VALUE if the
   * number of hops isn't known yet.
   */
  private Map<Long, Integer> verticesWithHopsCount =
          new HashMap<Long, Integer>();

  /**
   * A set of already processed request from one source to a target,
   * so that messages won't run too much in circles.
   */
  private Set<Entry<Long, Long>> processedMessages =
          new HashSet<Entry<Long, Long>>();

  /**
   * @return a list of vertex IDs for which the hop count should be calculated
   */
  public Iterable<Long> getVertices() {
    return this.verticesWithHopsCount.keySet();
  }

  /**
   * Checks if a similar message (same source and destination ID) has been
   * already processed in the past.
   *
   * @param message the message to be checked
   * @return true if the list has been already processed, otherwise false
   */
  public boolean hasProcessed(SimpleHopsMessage message) {
    return this.processedMessages.contains(getEntry(message));
  }

  /**
   * Defines that a message is being processed, so that the source and
   * destination IDs can be remembered.
   *
   * @param message the message which is processed by {@link org.apache
   *                .giraph.examples.SimpleHopsComputation}
   */
  public void isProcessing(SimpleHopsMessage message) {
    this.processedMessages.add(getEntry(message));
  }

  /**
   * Sets the vertex IDs of vertices for which the hops count should be
   * calculated together with the invalid hops count of Integer.MAX_VALUE.
   *
   * @param vertices list of vertex IDs
   */
  public void initializeVerticesWithHopsCount(Iterable<Long> vertices) {
    for (Long vertex : vertices) {
      this.verticesWithHopsCount.put(vertex, Integer.MAX_VALUE);
    }
  }

  /**
   * When the vertex receives an answer it will be stored. There shouldn't be
   * multiple answers arriving and the first one should be always the
   * shortest one. But since the initial value is Integer.MAX_VALUE this
   * logic will always work.
   *
   * @param message the answered message with the correct hops count
   */
  public void updateHopsCounts(SimpleHopsMessage message) {
    int currentHopsCount = this.verticesWithHopsCount.get(message
            .getDestinationId());
    if (message.getHopsCount() < currentHopsCount) {
      this.verticesWithHopsCount.put(
              message.getDestinationId(), message.getHopsCount());
    }
  }

  /**
   * Check if all answers have been received.
   *
   * @return true if there are no more entries in verticesWithHopsCounts with
   *         invalid values (Integer.MAX_VALUE)
   */
  public boolean hasAllHopsCounts() {
    for (Entry<Long, Integer> entry : this.verticesWithHopsCount.entrySet()) {
      if (entry.getValue() == Integer.MAX_VALUE) {
        return false;
      }
    }
    return true;
  }

  public Map<Long, Integer> getVerticesWithHopsCount() {
    return this.verticesWithHopsCount;
  }

  @Override
  public void write(DataOutput dataOutput) throws IOException {
    dataOutput.writeInt(this.verticesWithHopsCount.size());

    for (Entry<Long, Integer> entry : this.verticesWithHopsCount.entrySet()) {
      dataOutput.writeLong(entry.getKey());
      dataOutput.writeInt(entry.getValue());
    }

    dataOutput.writeInt(this.processedMessages.size());

    for (Entry<Long, Long> entry : this.processedMessages) {
      dataOutput.writeLong(entry.getKey());
      dataOutput.writeLong(entry.getValue());
    }
  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {
    int size = dataInput.readInt();

    for (int i = 0; i < size; i++) {
      this.verticesWithHopsCount.put(
              dataInput.readLong(), dataInput.readInt());
    }

    size = dataInput.readInt();

    for (int i = 0; i < size; i++) {
      this.processedMessages.add(new SimpleEntry<Long, Long>(
              dataInput.readLong(), dataInput.readLong()));
    }
  }

  /**
   * Converts a message into a SimpleEntry with sourceId and destinationId, to
   * be stored in the HashSet
   *
   * @param message which will be converted into the SimpleEntry
   * @return the converted Entry
   */
  private Entry<Long, Long> getEntry(SimpleHopsMessage message) {
    return new SimpleEntry<Long, Long>(
            message.getSourceId(), message.getDestinationId());
  }
}
