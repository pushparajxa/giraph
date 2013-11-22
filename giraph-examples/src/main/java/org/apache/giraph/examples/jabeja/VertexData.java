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
import java.util.HashMap;
import java.util.Map;

/**
 * Defines the base structure for all date necessary for vertices, for both
 * Edge- and Node-partitioning.
 */
public class VertexData implements Writable {
  /**
   * The key is the id of the neighbor, the value is the color.
   * this property is stored
   */
  private final Map<Long, Integer> neighboringColors =
          new HashMap<Long, Integer>();

  /**
   * The key is the color, the value is how often the color exists.
   * this property is calculated
   */
  private Map<Integer, Integer> neighboringColorRatio;

  /**
   * @return data for a histogram for the colors of all neighbors.
   *         How often each of the colors is represented between the neighbors.
   *         If a color isn't represented, it's not in the final Map.
   */
  public Map<Integer, Integer> getNeighboringColorRatio() {
    if (this.neighboringColorRatio == null) {
      initializeNeighboringColorRatio();
    }

    return this.neighboringColorRatio;
  }

  /**
   * Update the neighboringColors map with a new or updated entry of a
   * neighbor and its color
   *
   * @param neighborId    the id of the neighbor (could be node or edge)
   * @param neighborColor the color of the neighbor
   */
  public void setNeighborWithColor(long neighborId, int neighborColor) {
    this.neighboringColors.put(neighborId, neighborColor);
  }

  /**
   * Initializes the histogram for colors of all neighbors.
   * How often each of the colors is represented between the neighbors.
   * If a color isn't represented, it's not in the final Map.
   */
  public void initializeNeighboringColorRatio() {
    this.neighboringColorRatio = new HashMap<Integer, Integer>();

    for (Map.Entry<Long, Integer> item : this.neighboringColors.entrySet()) {
      addColorToNeighboringColoRatio(item.getValue());
    }
  }

  /**
   * Check if the color already exists in the neighboringColorRatio-map. If
   * not, create a new entry with the count 1, if yes than update the count +1
   *
   * @param color the color of one neighboring item
   */
  private void addColorToNeighboringColoRatio(int color) {
    Integer numberOfColorAppearances = this.neighboringColorRatio.get(color);

    if (numberOfColorAppearances == null) {
      numberOfColorAppearances = 1;
    } else {
      numberOfColorAppearances++;
    }

    this.neighboringColorRatio.put(color, numberOfColorAppearances);
  }

  @Override
  public void readFields(DataInput input) throws IOException {
    readNeighboringColors(input);
  }

  @Override
  public void write(DataOutput output) throws IOException {
    writeNeighboringColors(output);
  }

  /**
   * since the vertex doesn't have any information about its neighbors,
   * all the information needs to be stored in the vertex data. This
   * includes the colors of all the neighbors, to be able to compute the
   * algorithm for changing the color at all times
   *
   * @param input DataInput from readFields
   */
  private void readNeighboringColors(DataInput input) throws IOException {
    int numberOfNeighbors = input.readInt();

    for (int i = 0; i < numberOfNeighbors; i++) {
      Long neighborId = input.readLong();
      Integer neighborColor = input.readInt();

      this.neighboringColors.put(neighborId, neighborColor);
    }
  }

  /**
   * Opposite to readNeighboringColors this method is to store the
   * color information of all the neighbors.
   *
   * @param output DataOutput from write
   */
  private void writeNeighboringColors(DataOutput output) throws IOException {
    int numberOfNeighbors = this.neighboringColors.size();

    output.writeInt(numberOfNeighbors);

    for (Map.Entry<Long, Integer> item : this.neighboringColors.entrySet()) {
      long neighborId = item.getKey();
      int neighborColor = item.getValue();

      output.writeLong(neighborId);
      output.writeInt(neighborColor);
    }
  }
}
