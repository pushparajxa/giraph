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

/**
 * This class conatins the outgoing edges of a vertex and its neighbours on the
 * TargetVertex Side
 * 
 * @author Pushparaj
 * 
 */
public class OwnEdge {
  /**
   * Contains this edge details
   */
  public JabejaEdge details;
  /**
   * The directed edges are A->B, B->C, B->E, E->D, E->F Contains the
   * information about A D \ / \ / B------------------/E / \ / \ C F Say details
   * conatins edge BE. Then neighbour will conatin information about edges ED
   * and EF
   */
  public HashMap<Long, JabejaEdge> neighbours = new HashMap<Long, JabejaEdge>();

  /**
   * Default Constructor
   * 
   * @param j
   */
  public OwnEdge(JabejaEdge j) {
    this.details = j;
  }
}
