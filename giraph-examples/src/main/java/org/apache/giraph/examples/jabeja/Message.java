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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

/**
 * Structure of messages sent between vertices
 */
public class Message extends BaseWritable {
  /**
   * The neighboring color ratio represents how many neighbors (value) have a
   * specific color (key)
   */
  private Map<Integer, Integer> neighboringColorRatio = new HashMap<Integer, Integer>();

  private String messageType;
  public final static String FIRST_MESSAGE = "FirstMessage";
  public final static String ZERO_MESSAGE = "ZeroMessage";
  public final static String RQST_SUCC_MESSAGE = "RequestSuccessMessage";
  public final static String RQST_CANCL__MESSAGE = "RequestCancelledMessage";
  public final static String RQST_NOT_PRCSS_MESSAGE = "RequestNotProcessedMessage";
  public final static String UPDATE_MESSAGE = "UpdateMessage";
  public final static String RQST_MESSAGE = "RequestMessage";
  public final static String RQST_NOT_SUCC_MESSAGE = "RequestNotSuccessMessage";

  /**
   * Id of the source vertex sending the message. Necessary for replies or
   * knowing your neighbors.
   */
  private long vertexId;
  private JabejaEdge edge;
  private HashMap<Long, JabejaEdge> edges;
  private ArrayList<JabejaEdge> nghbrs;
  private Integer energy;
  private Text t;

  /**
   * Default constructor for reflection
   */
  public Message() {
  }

  /**
   * Initialize the message with the source vertex id
   * 
   * @param vertexId the id of the vertex sending the message
   */
  public Message(long vertexId, String type) {
    this.vertexId = vertexId;
    this.messageType = type;
  }

  public Message(long vertexId, JabejaEdge edge, String type) {
    this.vertexId = vertexId;
    this.edge = edge;
    this.messageType = type;
  }

  public Message(long vertexId, HashMap<Long, JabejaEdge> edges, String type) {
    this.vertexId = vertexId;
    this.edges = edges;
    this.messageType = type;
  }

  public Message(long vertexId, JabejaEdge je, ArrayList<JabejaEdge> nghbrs,
      int energy, String type) {
    this.vertexId = vertexId;
    this.edge = je;
    this.nghbrs = nghbrs;
    this.energy = energy;
    this.messageType = type;
  }

  public long getVertexId() {
    return vertexId;
  }

  public void setVertexId(long id) {
    this.vertexId = id;
  }

  public String getMessageType() {
    return messageType;
  }

  public Map<Integer, Integer> getNeighboringColorRatio() {
    return neighboringColorRatio;
  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {
    this.vertexId = dataInput.readLong();
    this.messageType = Text.readString(dataInput);
    // t.readFields(dataInput);
    // this.messageType = t.toString();
    if (messageType.equals(FIRST_MESSAGE)) {
      // Read the Map edges<Long,JabegaEdge>.1)Read the size 2) Then
// Map entries.
      int size = dataInput.readInt();
      edges = new HashMap<Long, JabejaEdge>();
      int i = 0, color;
      JabejaEdge je;
      Long l;
      long srcVertex, destVertex;
      for (i = 0; i < size; i++) {
        l = Long.valueOf(dataInput.readLong());
        srcVertex = dataInput.readLong();
        destVertex = dataInput.readLong();
        color = dataInput.readInt();
        je = new JabejaEdge(new LongWritable(srcVertex), new LongWritable(
            destVertex), new IntWritable(color));

        edges.put(l, je);
      }

    } else if (messageType.equals(RQST_CANCL__MESSAGE)) {
      // nothing
    } else if (messageType.equals(RQST_MESSAGE)) {

      long srcVertex = dataInput.readLong();
      long destVertex = dataInput.readLong();
      int color = dataInput.readInt();
      edge = new JabejaEdge(new LongWritable(srcVertex), new LongWritable(
          destVertex), new IntWritable(color));

      energy = dataInput.readInt();
      // Read the arrayList<JabejaEdge>.1)Read Size of the list then values
      int size = dataInput.readInt();
      nghbrs = new ArrayList<JabejaEdge>();
      int i = 0, clr;
      JabejaEdge je;
      for (i = 0; i < size; i++) {
        srcVertex = dataInput.readLong();
        destVertex = dataInput.readLong();
        clr = dataInput.readInt();
        je = new JabejaEdge(new LongWritable(srcVertex), new LongWritable(
            destVertex), new IntWritable(clr));
        nghbrs.add(je);
      }

    } else if (messageType.equals(RQST_NOT_PRCSS_MESSAGE)) {
// nothing
    } else if (messageType.equals(RQST_NOT_SUCC_MESSAGE)) {
      // nothing
    } else if (messageType.equals(RQST_SUCC_MESSAGE)) {

      long srcVertex = dataInput.readLong();
      long destVertex = dataInput.readLong();
      int color = dataInput.readInt();
      edge = new JabejaEdge(new LongWritable(srcVertex), new LongWritable(
          destVertex), new IntWritable(color));

    } else if (messageType.equals(UPDATE_MESSAGE)) {

      long srcVertex = dataInput.readLong();
      long destVertex = dataInput.readLong();
      int color = dataInput.readInt();
      edge = new JabejaEdge(new LongWritable(srcVertex), new LongWritable(
          destVertex), new IntWritable(color));

    } else if (messageType.equals(ZERO_MESSAGE)) {

      long srcVertex = dataInput.readLong();
      long destVertex = dataInput.readLong();
      int color = dataInput.readInt();
      edge = new JabejaEdge(new LongWritable(srcVertex), new LongWritable(
          destVertex), new IntWritable(color));

    }
  }

  @Override
  public void write(DataOutput dataOutput) throws IOException {
    dataOutput.writeLong(this.vertexId);
    t = new Text(this.messageType);
    t.write(dataOutput);

    if (messageType.equals(FIRST_MESSAGE)) {
      // Write the Map edges<Long,JabegaEdge>.1)Write the size 2) Then
// JabejaEdge
      dataOutput.writeInt(edges.size());
      for (Map.Entry<Long, JabejaEdge> e : edges.entrySet()) {
        dataOutput.writeLong(e.getKey().longValue());
        dataOutput.writeLong(e.getValue().sourceVetex.get());
        dataOutput.writeLong(e.getValue().destVertex.get());
        dataOutput.writeInt(e.getValue().color.get());
      }

    } else if (messageType.equals(RQST_CANCL__MESSAGE)) {
      // nothing
    } else if (messageType.equals(RQST_MESSAGE)) {

      dataOutput.writeLong(edge.sourceVetex.get());
      dataOutput.writeLong(edge.destVertex.get());
      dataOutput.writeInt(edge.color.get());

      dataOutput.writeInt(energy);
      // Write the arrayList<JabejaEdge>.1)Wrire Size of the list then values
      dataOutput.writeInt(nghbrs.size());

      for (JabejaEdge je : nghbrs) {
        dataOutput.writeLong(je.sourceVetex.get());
        dataOutput.writeLong(je.destVertex.get());
        dataOutput.writeInt(je.color.get());
      }

    } else if (messageType.equals(RQST_NOT_PRCSS_MESSAGE)) {
// nothing
    } else if (messageType.equals(RQST_NOT_SUCC_MESSAGE)) {
      // nothing
    } else if (messageType.equals(RQST_SUCC_MESSAGE)) {

      dataOutput.writeLong(edge.sourceVetex.get());
      dataOutput.writeLong(edge.destVertex.get());
      dataOutput.writeInt(edge.color.get());

    } else if (messageType.equals(UPDATE_MESSAGE)) {

      dataOutput.writeLong(edge.sourceVetex.get());
      dataOutput.writeLong(edge.destVertex.get());
      dataOutput.writeInt(edge.color.get());

    } else if (messageType.equals(ZERO_MESSAGE)) {

      dataOutput.writeLong(edge.sourceVetex.get());
      dataOutput.writeLong(edge.destVertex.get());
      dataOutput.writeInt(edge.color.get());

    }

  }

  /**
   * @return the edge
   */
  public JabejaEdge getEdge() {
    return edge;
  }

  /**
   * @param edge the edge to set
   */
  public void setEdge(JabejaEdge edge) {
    this.edge = edge;
  }

  public HashMap<Long, JabejaEdge> getEdges() {
    return edges;
  }

  public void setEdges(HashMap<Long, JabejaEdge> edges) {
    this.edges = edges;
  }

  public Integer getEnergy() {
    return energy;
  }

  public void setEnergy(Integer energy) {
    this.energy = energy;
  }

  public ArrayList<JabejaEdge> getNghbrs() {
    return nghbrs;
  }

  public void setNghbrs(ArrayList<JabejaEdge> nghbrs) {
    this.nghbrs = nghbrs;
  }
}
