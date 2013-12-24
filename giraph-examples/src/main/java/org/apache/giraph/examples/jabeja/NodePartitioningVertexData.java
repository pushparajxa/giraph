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
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;

/**
 * Vertex data for the NodePartitioning solution.
 */
public class NodePartitioningVertexData implements Writable {
  /**
   * This will contain information about the incoming edges from other vertices
   * to this vertex.
   */
  private HashMap<Long, JabejaEdge> inEdges = new HashMap<Long, JabejaEdge>();
  /**
   * This will contain information about the outGoing edges from this vertex.
   * And neighbor information for these edges
   */
  private HashMap<Long, OwnEdge> outEdges = new HashMap<Long, OwnEdge>();

  /**
   * This edge is locked and sent in Request to swap
   */
  private Long lockedEdgeTargetVertex = Long.MIN_VALUE;
  /**
   * Index pointing to the next edge to be locked
   */
  private int lockEdgeIndex = 0;

  /**
   * Default constructor for reflection
   * 
   * @param vertexId
   */
  public NodePartitioningVertexData() {
    super();
    // randVertexGen = new Random(vertexId);
  }

  @Override
  public void readFields(DataInput input) throws IOException {
    setLockEdgeIndex(input.readInt());
    setLockedEdgeTargetVertex(Long.valueOf(input.readLong()));
    mapReader(outEdges, input, new LongReaderWriter(),
        new OwnEdgeReaderWriter());
    mapReader(inEdges, input, new LongReaderWriter(),
        new JabejaEdgeReaderWriter());
  }

  @Override
  public void write(DataOutput output) throws IOException {
    output.writeInt(lockEdgeIndex);
    output.writeLong(lockedEdgeTargetVertex.longValue());
    mapWriter(outEdges, output, new LongReaderWriter(),
        new OwnEdgeReaderWriter());
    mapWriter(inEdges, output, new LongReaderWriter(),
        new JabejaEdgeReaderWriter());

  }

  public Long getLockedEdgeTargetVertex() {
    return lockedEdgeTargetVertex;
  }

  public void setLockedEdgeTargetVertex(Long lockedEdgeTargetVertex) {
    this.lockedEdgeTargetVertex = lockedEdgeTargetVertex;
  }

  public int getLockEdgeIndex() {
    return lockEdgeIndex;
  }

  public void setLockEdgeIndex(int lockEdgeIndex) {
    this.lockEdgeIndex = lockEdgeIndex;
  }

  /*
   * public Random getRandVertexGen() { return randVertexGen; }
   * 
   * public void setRandVertexGen(Random randVertexGen) { this.randVertexGen =
   * randVertexGen; }
   */

  public static <K, V> void mapWriter(HashMap<K, V> map, DataOutput output,
      FieldReaderWriter<K> keywriter, FieldReaderWriter<V> valueWriter)
      throws IOException {
    output.writeInt(map.size());
    for (Map.Entry<K, V> e : map.entrySet()) {

      keywriter.write(output, e.getKey());
      valueWriter.write(output, e.getValue());
    }
  }

  public static <K, V> void mapReader(HashMap<K, V> map, DataInput input,
      FieldReaderWriter<K> keyReaderWriter,
      FieldReaderWriter<V> valueReaderWriter) throws IOException {
    int size = input.readInt();
    while (size > 0) {
      K key = keyReaderWriter.read(input);
      V value = valueReaderWriter.read(input);
      map.put(key, value);
      size--;
    }
  }

  private interface FieldReaderWriter<K> {
    public void write(DataOutput output, K field) throws IOException;

    public K read(DataInput input) throws IOException;
  }

  private static class LongReaderWriter implements FieldReaderWriter<Long> {

    public void write(DataOutput output, Long val) throws IOException {
      output.writeLong(val.longValue());
    }

    public Long read(DataInput input) throws IOException {
      return Long.valueOf(input.readLong());
    }
  }

  private static class JabejaEdgeReaderWriter implements
      FieldReaderWriter<JabejaEdge> {
    public void write(DataOutput output, JabejaEdge je) throws IOException {
      output.writeLong(je.sourceVetex.get());
      output.writeLong(je.destVertex.get());
      output.writeInt(je.color.get());
    }

    public JabejaEdge read(DataInput input) throws IOException {
      long srcVertex = input.readLong();
      long destVertex = input.readLong();
      int color = input.readInt();
      return new JabejaEdge(new LongWritable(srcVertex), new LongWritable(
          destVertex), new IntWritable(color));
    }
  }

  private static class OwnEdgeReaderWriter implements
      FieldReaderWriter<OwnEdge> {
    public void write(DataOutput output, OwnEdge we) throws IOException {
      new JabejaEdgeReaderWriter().write(output, we.details);
      mapWriter(we.neighbours, output, new LongReaderWriter(),
          new JabejaEdgeReaderWriter());
    }

    public OwnEdge read(DataInput input) throws IOException {
      JabejaEdge je = new JabejaEdgeReaderWriter().read(input);
      OwnEdge eg = new OwnEdge(je);
      mapReader(eg.neighbours, input, new LongReaderWriter(),
          new JabejaEdgeReaderWriter());
      return eg;
    }
  }

  public HashMap<Long, JabejaEdge> getInEdges() {
    return inEdges;
  }

  public void setInEdges(HashMap<Long, JabejaEdge> inEdges) {
    this.inEdges = inEdges;
  }

  public HashMap<Long, OwnEdge> getOutEdges() {
    return outEdges;
  }

  public void setOutEdges(HashMap<Long, OwnEdge> outEdges) {
    this.outEdges = outEdges;
  }

}
