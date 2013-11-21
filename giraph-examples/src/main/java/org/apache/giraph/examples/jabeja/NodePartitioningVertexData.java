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

/**
 * Vertex data for the NodePartitioning solution.
 */
public class NodePartitioningVertexData extends VertexData {
  /**
   * The color of the current node
   */
  private int nodeColor;

  public int getNodeColor() {
    return nodeColor;
  }

  public void setNodeColor(int nodeColor) {
    this.nodeColor = nodeColor;
  }

  @Override
  public void readFields(DataInput input) throws IOException {
    super.readFields(input);

    this.nodeColor = input.readInt();
  }

  @Override
  public void write(DataOutput output) throws IOException {
    super.write(output);

    output.writeInt(this.nodeColor);
  }
}
