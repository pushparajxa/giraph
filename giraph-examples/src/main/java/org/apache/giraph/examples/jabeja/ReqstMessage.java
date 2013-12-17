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

import org.apache.hadoop.io.LongWritable;

/**
 * This message is used to send Request to swap.
 * 
 * @author Pushparaj
 * 
 */
public class ReqstMessage extends Message {
  /**
   * Default Constructor
   * 
   * @param id
   * @param details
   * @param neighbrs
   */
  /**
   * The requested edge
   */
  public JabejaEdge requstEdge;
  /**
   * The neighbours of this edge from the request vertex side end
   */
  public ArrayList<JabejaEdge> nghbrs;
  /**
   * Energy of this Edge or Request
   */
  private Integer energy;

  /**
   * Default Constructor
   * 
   * @param id
   * @param details
   * @param neighbrs
   */
  public ReqstMessage(LongWritable id, JabejaEdge details,
      ArrayList<JabejaEdge> neighbrs) {
    setVertexId(id.get());
    this.requstEdge = details;
    this.nghbrs = neighbrs;
  }

  /**
   * @return the energy
   */
  public Integer getEnergy() {
    return energy;
  }

  /**
   * @param energy the energy to set
   */
  public void setEnergy(Integer energy) {
    this.energy = energy;
  }

  @Override
  public void readFields(DataInput arg0) throws IOException {
    // TODO Auto-generated method stub

  }

  @Override
  public void write(DataOutput arg0) throws IOException {
    // TODO Auto-generated method stub

  }

}
