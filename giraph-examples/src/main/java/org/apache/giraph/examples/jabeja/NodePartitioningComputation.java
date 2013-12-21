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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

/**
 * Implement the original JaBeJa-Algorithm
 * (https://www.sics.se/~amir/files/download/papers/jabeja.pdf)
 */
public class NodePartitioningComputation
    extends
    BasicComputation<LongWritable, NodePartitioningVertexData, IntWritable, Message> {
  /**
   * The default number of different colors, if no JaBeJa.NumberOfColors is
   * provided.
   */
  // private static final int DEFAULT_NUMBER_OF_COLORS = 2;
  private static final Logger LOG = Logger
      .getLogger(NodePartitioningComputation.class);
  /**
   * The currently processed vertex
   */
  private Vertex<LongWritable, NodePartitioningVertexData, IntWritable> vertex;
  /**
   * This edge is locked and sent in Request to swap
   */
  private Long lockedEdgeTargetVertex = null;
  /**
   * Index pointing to the next edge to be locked
   */
  private int lockEdgeIndex = 0;
  /**
   * Variable storing this vertex data.
   */
  private NodePartitioningVertexData verData;

  @Override
  public void compute(
      Vertex<LongWritable, NodePartitioningVertexData, IntWritable> vertex,
      Iterable<Message> messages) throws IOException {
    this.vertex = vertex;
    this.verData = this.vertex.getValue();

    if (isTimeToStop()) {
      this.vertex.voteToHalt();
      return;
    }

    if (super.getSuperstep() < 3) {
      initializeGraph(messages);
    } else {
      if (getSuperstep() % 3 == 0) {
        processReqeustMessages(messages);
      }
      if (getSuperstep() % 3 == 1) {
        processRequestResponses(messages);
      }
      if (getSuperstep() % 3 == 2) {
        processUpdateMessages(messages);
      }
    }
    /*
     * Calculate energies After sending the Request to swap messages and before
     * receiving the Request Messages.
     */
    /*
     * if (getSuperstep() % 3 == 2) { aggregate(JabejaMasterCompute.ENEREGY,
     * calculateEnergy()); }
     */

  }

  /**
   * This method is used to calculate the energy
   * 
   * @return
   */
  private IntWritable calculateEnergy() {
    int energy = 0;
    for (Map.Entry<Long, OwnEdge> e : verData.outEdges.entrySet()) {
      energy = energy
          + calculateEnergyOfEdge(e.getKey(), e.getValue().details.color.get());
    }
    return new IntWritable(energy);
  }

  /**
   * This method is used to update the edge colors which are neighbors of some
   * this vertex's edges.
   * 
   * @param messages
   */
  private void processUpdateMessages(Iterable<Message> messages) {
    for (Message msg : messages) {
      // UpdateMessage upm = (UpdateMessage) msg;
      JabejaEdge je = msg.getEdge();
      /*
       * 1. Search for this edge in inEdges, if not 2. Search in outEdges.
       */
      boolean done = false;
      for (Long l : verData.inEdges.keySet()) {
        if (l.longValue() == je.sourceVetex.get()) {
          verData.inEdges.get(l).color = new IntWritable(je.color.get());
          done = true;
        }
      }
      if (done) {
        continue;
      } else {
        ArrayList<JabejaEdge> edges = new ArrayList<JabejaEdge>();
        for (Map.Entry<Long, OwnEdge> e : verData.outEdges.entrySet()) {
          edges.addAll(e.getValue().neighbours.values());
        }
        for (JabejaEdge jedge : edges) {
          if (jedge.sourceVetex.get() == je.sourceVetex.get()
              && jedge.destVertex.get() == je.sourceVetex.get()) {
            jedge.color = new IntWritable(je.color.get());
          }
        }
      }

    }

    // Send Request to swap.
    /**
     * 1. Select one edge[ownEdgs9outGoign edges)] and mark it as locked. 2.
     * Select some random vertex and send all the edge data[neighbour
     * information] about the locked one in above.
     */
    ArrayList<Long> al = new ArrayList<Long>(verData.outEdges.keySet());

    if (al.size() == 0) {
      lockedEdgeTargetVertex = null;
// don't send request messages since you do not have any outward edges.
    } else {

      lockedEdgeTargetVertex = al.get(lockEdgeIndex % al.size());
      lockEdgeIndex++;
      /**
       * Until Random vertex getter functionality implemented lets send it to
       * the lockedEdgeTargetVertex. Send a RequestMessage which conatins all
       * the neighbouring edges of this edge.And the sourceVertexId, that is the
       * request sending vertex's id.
       */

      ArrayList<JabejaEdge> neighbrs = new ArrayList<JabejaEdge>(
          verData.outEdges.get(lockedEdgeTargetVertex).neighbours.values());

      /*
       * Add the outEdges of this vertex
       */
      for (Long l : verData.outEdges.keySet()) {
        if (l.longValue() != lockedEdgeTargetVertex.longValue())
          neighbrs.add(verData.outEdges.get(l).details);
      }
      /*
       * Adds the incoming edges at this vertex.
       */
      neighbrs.addAll(verData.inEdges.values());

      /*
       * ReqstMessage rm = new ReqstMessage(this.vertex.getId(),
       * verData.outEdges.get(lockedEdgeTargetVertex).details, neighbrs); int
       * myColor = rm.requstEdge.color.get();
       * rm.setEnergy(calculateEnergyOfRequest(rm, myColor)); sendMessage(new
       * LongWritable(lockedEdgeTargetVertex), rm);
       */

      Message rm = new Message(this.vertex.getId().get(),
          verData.outEdges.get(lockedEdgeTargetVertex).details, neighbrs, 0,
          Message.RQST_MESSAGE);
      int myColor = rm.getEdge().color.get();
      rm.setEnergy(calculateEnergyOfRequest(rm, myColor));
      sendMessage(new LongWritable(lockedEdgeTargetVertex), rm);

    }
  }

  /**
   * This method will process the responses received after sending the Request
   * Messages
   */
  private void processRequestResponses(Iterable<Message> messages) {
    String msgType;
    for (Message msg : messages) {
      msgType = msg.getMessageType();

      if (msgType.equals(Message.RQST_NOT_PRCSS_MESSAGE)) {

      } else if (msgType.equals(Message.RQST_NOT_SUCC_MESSAGE)) {

      } else if (msgType.equals(Message.RQST_CANCL__MESSAGE)) {

      } else if (msgType.equals(Message.RQST_SUCC_MESSAGE)) {
        JabejaEdge edge = msg.getEdge();
        verData.outEdges.get(edge.destVertex.get()).details.color = new IntWritable(
            edge.color.get());
        /* Send this update to neighbors of this edge */
        /*
         * 1. To inedges of this vertex. 2. To neighbour edges at the end of
         * other vetex for this vertex.
         */
        for (Long l : verData.inEdges.keySet()) {
          /*
           * sendMessage(new LongWritable(l), new
           * UpdateMessage(this.vertex.getId(), edge));
           */
          sendMessage(new LongWritable(l), new Message(this.vertex.getId()
              .get(), edge, Message.UPDATE_MESSAGE));
        }
        for (JabejaEdge je : verData.outEdges.get(edge.destVertex.get()).neighbours
            .values()) {
          /*
           * sendMessage(je.sourceVetex, new UpdateMessage(this.vertex.getId(),
           * edge));
           */
          sendMessage(je.sourceVetex, new Message(this.vertex.getId().get(),
              edge, Message.UPDATE_MESSAGE));
        }
      } else {

      }
    }

  }

  /**
   * This method processes the RequestMessage for swapping the color of the edge
   * 
   * @param messages
   */
  private void processReqeustMessages(Iterable<Message> messages) {
    /*
     * 1. Select the edge with which after swapping the energy is less 2.
     * Calculate the energy of the request vertex 3. If the swap acceptable send
     * SuccessMessage 4. If swap not successfully send Not successful message 5.
     * If request is satisfied send RequestNot processed message. 6. In case of
     * swap success send propagation of changes to the other vertices.
     * 
     * HashMap<Long, Integer> energies = new HashMap<Long, Integer>();
     * 
     * for (Long l : verData.outEdges.keySet()) { if (l !=
     * lockedEdgeTargetVertex) energies.put(l, calculateEnergyOfEdge(l,
     * verData.outEdges.get(l))); }
     */

    if (verData.outEdges.keySet().size() == 1
        || verData.outEdges.keySet().size() == 0) {
      /*
       * Only one outgoing edge and that one is locked so send ReqeustCancelled
       * message to all the requests.
       */
      for (Message msg : messages) {
        /*
         * sendMessage(new LongWritable(msg.getVertexId()), new
         * RequestCancelledMessage(this.vertex.getId().get()));
         */
        sendMessage(new LongWritable(msg.getVertexId()), new Message(
            this.vertex.getId().get(), Message.RQST_CANCL__MESSAGE));
      }
      return;
    }

    boolean done = false;
    for (Message m : messages) {
      // ReqstMessage rm = (ReqstMessage) m;
      Message rm = m;
      if (done == true) {
        /*
         * sendMessage(new LongWritable(rm.getVertexId()), new
         * RequestNotProcessedMessage(this.vertex.getId().get()));
         */
        sendMessage(new LongWritable(rm.getVertexId()), new Message(this.vertex
            .getId().get(), Message.RQST_NOT_PRCSS_MESSAGE));
        continue;
      } else {
        for (Long l : verData.outEdges.keySet()) {
          if (lockedEdgeTargetVertex == null) {
            System.out.println("LockedEdge Vertex is null.");
            LOG.log(Level.INFO, "LockedEdge Vertex is null.");
          } else {
            if (l.longValue() != lockedEdgeTargetVertex.longValue()) {
              if (swapPossible(l, rm)) {
                /*
                 * 1.Update local edge's colour 2.Send Update of the edge color
                 * to the reuqetsed vertex
                 */
                int k = rm.getEdge().color.get();
                rm.getEdge().color = verData.outEdges.get(l).details.color;
                verData.outEdges.get(l).details.color = new IntWritable(k);
                /*
                 * sendMessage(new LongWritable(rm.getVertexId()), new
                 * RequestSucceededMessage(this.vertex.getId(), rm.requstEdge));
                 */
                sendMessage(new LongWritable(rm.getVertexId()), new Message(
                    this.vertex.getId().get(), rm.getEdge(),
                    Message.RQST_SUCC_MESSAGE));
                done = true;
                break;
              }
            }
          }

        }
        /*
         * Send Request for Swap Not Succeeded. Couldn't able to lower the
         * energy by swapping. Process next request.
         */
        if (done == false) {
          /*
           * sendMessage(new LongWritable(m.getVertexId()), new
           * RequestNotSucceededMessage(this.vertex.getId().get()));
           */
          sendMessage(new LongWritable(m.getVertexId()), new Message(
              this.vertex.getId().get(), Message.RQST_NOT_SUCC_MESSAGE));
        }

      }

    }
  }

  private boolean swapPossible(Long l, Message rm) {
    int edgeOldEnergy = calculateEnergyOfEdge(l,
        verData.outEdges.get(l).details.color.get());
    int requestOldEnergy = calculateEnergyOfRequest(rm,
        rm.getEdge().color.get());
    int totalEneryOld = edgeOldEnergy + requestOldEnergy;
    int edgeNewEnergy = calculateEnergyOfEdge(l, rm.getEdge().color.get());
    int requestNewEnergy = calculateEnergyOfRequest(rm,
        verData.outEdges.get(l).details.color.get());
    int totalEnergyNew = edgeNewEnergy + requestNewEnergy;
    if (totalEnergyNew < totalEneryOld)
      return true;
    else
      return false;
  }

  /**
   * Calculates the energy of a RequestMessage for swapping
   * 
   * @param rm
   */
  private Integer calculateEnergyOfRequest(Message rm, int color) {

    int energy = 0;
    for (JabejaEdge je : rm.getNghbrs()) {
      if (color != je.color.get())
        energy++;
    }
    return Integer.valueOf(energy);
  }

  /**
   * Calculates the energy of a given edge
   */
  private Integer calculateEnergyOfEdge(Long lg, int color) {
    OwnEdge ownEdge = verData.outEdges.get(lg);
    // int myColor = ownEdge.details.color.get();
    int energy = 0;
    ArrayList<JabejaEdge> edges = new ArrayList<JabejaEdge>(
        ownEdge.neighbours.values());

    /*
     * Add all the inedges
     */
    edges.addAll(verData.inEdges.values());
    /*
     * Add all the outgoing edges from this vertex except the edge in question
     * and the lockedEdge which is sent as a request for swapping
     */
    for (Long l : verData.outEdges.keySet()) {
      if (l.longValue() != lockedEdgeTargetVertex.longValue()
          && l.longValue() != lg.longValue())
        edges.add(verData.outEdges.get(l).details);
    }

    /*
     * Now we have all edges , let's calculate the energu of the edge. Energy =
     * no.of edges with color different than it's.
     */
    for (JabejaEdge j : edges) {
      if (j.color.get() != color)
        energy++;
    }
    return Integer.valueOf(energy);
  }

  /**
   * This function is used in the first stage, when the graph is being
   * initialized. It contains features for taking a random color as well as
   * announcing the color and finding all neighbors.
   * 
   * @param messages The messages sent to this Vertex
   */
  private void initializeGraph(Iterable<Message> messages) {
    if (getSuperstep() == 0) {
      /**
       * Announce colour of the outgoing edges
       */
      announceColor();
      // LOG.trace("Successfully announec the color");
      LOG.log(Level.INFO, "Successfully announecd the color");
    } else if (getSuperstep() == 1) {
      storeZeroMessages(messages);
      // LOG.trace("Successfully stored zero messages");
      LOG.log(Level.INFO, "Successfully stored Zero Messages");
    } else if (getSuperstep() == 2) {
      storeFirstMessages(messages);
      // Send Request to swap.
      /**
       * 1. Select one edge[ownEdgs9outGoign edges)] and mark it as locked. 2.
       * Select some random vertex and send all the edge data[neighbour
       * information] about the locked one in above.
       */
      ArrayList<Long> al = new ArrayList<Long>(verData.outEdges.keySet());
      if (al.size() == 0) {
        lockedEdgeTargetVertex = null;
// don't send request messages since you do not have any outward edges.
      } else {
        lockedEdgeTargetVertex = al.get(lockEdgeIndex % al.size());
        lockEdgeIndex++;
        System.out.println("Locked Edge is "
            + lockedEdgeTargetVertex.longValue());
        /**
         * Until Random vertex getter functionality implemented lets send it to
         * the lockedEdgeTargetVertex. Send a RequestMessage which conatins all
         * the neighbouring edges of this edge.And the sourceVertexId, that is
         * the request sending vertex's id.
         */

        ArrayList<JabejaEdge> neighbrs = new ArrayList<JabejaEdge>(
            verData.outEdges.get(lockedEdgeTargetVertex).neighbours.values());

        /*
         * Add the outEdges of this vertex
         */
        for (Long l : verData.outEdges.keySet()) {
          if (l.longValue() != lockedEdgeTargetVertex.longValue())
            neighbrs.add(verData.outEdges.get(l).details);
        }
        /*
         * Adds the incoming edges at this vertex.
         */
        neighbrs.addAll(verData.inEdges.values());

        /*
         * ReqstMessage rm = new ReqstMessage(this.vertex.getId(),
         * verData.outEdges.get(lockedEdgeTargetVertex).details, neighbrs);
         */
        Message rm = new Message(this.vertex.getId().get(),
            verData.outEdges.get(lockedEdgeTargetVertex).details, neighbrs, 0,
            Message.RQST_MESSAGE);

        int myColor = rm.getEdge().color.get();
        rm.setEnergy(calculateEnergyOfRequest(rm, myColor));

        sendMessage(new LongWritable(lockedEdgeTargetVertex), rm);
      }
    }
  }

  /**
   * Store the FirstMessages and send request for color swapping.
   * 
   * @param messages
   */
  private void storeFirstMessages(Iterable<Message> messages) {
    for (Message m : messages) {
      // FirstMessage fm = (FirstMessage) m;

      /*
       * this.vertex.getValue().outEdges.get(fm.getVertexId()).neighbours
       * .putAll(fm.getEdges());
       */
      this.vertex.getValue().outEdges.get(m.getVertexId()).neighbours.putAll(m
          .getEdges());
    }
  }

  /**
   * Stores the incoming edges's information. These messages are sent in
   * SuperStep 0 and received in Superstep 1. And send the outEdges of yours to
   * the vertex which sent you this message.
   * 
   * @param messages
   */
  private void storeZeroMessages(Iterable<Message> messages) {
    /*
     * Store all the incoming edges
     */
    for (Message m : messages) {
      // ZeroMessage zm = (ZeroMessage) m;
      JabejaEdge je = m.getEdge();
      this.vertex.getValue().inEdges.put(je.sourceVetex.get(), je);

    }
    for (Message msg : messages) {
      /* Populate the map , which will be sent */
      HashMap<Long, JabejaEdge> outEdges2 = new HashMap<Long, JabejaEdge>();
      for (Map.Entry<Long, OwnEdge> e : this.vertex.getValue().outEdges
          .entrySet()) {
        outEdges2.put(e.getKey(), e.getValue().details);
      }
      /*
       * put the in-edges since they are neighbors of the edge from which we got
       * this incoming message
       */
      for (Map.Entry<Long, JabejaEdge> e : verData.inEdges.entrySet()) {
        if (e.getKey() != msg.getVertexId()) {
          outEdges2.put(e.getKey(), e.getValue());
        }
      }
      sendMessage(new LongWritable(msg.getVertexId()), new Message(this.vertex
          .getId().get(), outEdges2, Message.FIRST_MESSAGE));
    }

  }

  /**
   * After the graph has been initialized in the first 2 steps, this function is
   * executed and will run the actual JaBeJa algorithm * @param mode The JaBeJa
   * algorithm has multiple rounds, and in each round several sub-steps are
   * performed, mode indicates which sub-step should be performed.
   * 
   * @param messages The messages sent to this Vertex
   */
  /*
   * private void runJaBeJaAlgorithm(int mode, Iterable<Message> messages) {
   * switch (mode) { case 0: // new round started, announce your new color
   * announceColorIfChanged(); break; case 1: // update colors of your
   * neighboring nodes, // and announce your new degree for each color
   * storeColorsOfNodes(messages); announceColoredDegreesIfChanged(); break;
   * case 2: // updated colored degrees of all nodes and find a partner to //
   * initiate the exchange with storeColoredDegreesOfNodes(messages); Long
   * partnerId = findPartner();
   * 
   * if (partnerId != null) { initiateColoExchangeHandshake(partnerId); } else {
   * // if no partner could be found, this node probably has already the // best
   * possible color this.vertex.voteToHalt(); } break; default: mode = mode - 3;
   * continueColorExchange(mode, messages); } }
   */

  /**
   * Checks if it is time to stop (if enough steps have been done)
   * 
   * @return true if it is time to stop, if the number of supersteps exceeds the
   *         maximum allowed number
   */
  private boolean isTimeToStop() {
    return getSuperstep() >= getMaxNumberOfSuperSteps();
  }

  /**
   * Announce the current color to all connected vertices.
   */
  private void announceColor() {
    /**
     * for (Long neighborId : this.vertex.getValue().getNeighbors()) {
     * sendCurrentVertexColor(new LongWritable(neighborId)); }
     */
    for (Edge<LongWritable, IntWritable> e : this.vertex.getEdges()) {
      this.vertex.getValue().outEdges.put(e.getTargetVertexId().get(),
          new OwnEdge(new JabejaEdge(this.vertex.getId(),
              e.getTargetVertexId(), e.getValue())));
      /*
       * sendMessage( e.getTargetVertexId(), new
       * ZeroMessage(this.vertex.getId(), new JabejaEdge(this.vertex .getId(),
       * e.getTargetVertexId(), e.getValue())));
       */

      sendMessage(
          e.getTargetVertexId(),
          new Message(this.vertex.getId().get(), new JabejaEdge(this.vertex
              .getId(), e.getTargetVertexId(), e.getValue()),
              Message.ZERO_MESSAGE));

    }
  }

  private long getMaxNumberOfSuperSteps() {
    // return 2;
    return getConf().getInt("JaBeJa.MaxNumberOfSupersteps", 2);
    /*
     * if (MAX_NUMBER_OF_SUPERSTEPS == null) { MAX_NUMBER_OF_SUPERSTEPS =
     * super.getConf().getInt( "JaBeJa.MaxNumberOfSupersteps",
     * DEFAULT_MAX_NUMBER_OF_SUPERSTEPS); } return MAX_NUMBER_OF_SUPERSTEPS;
     */
  }

}
