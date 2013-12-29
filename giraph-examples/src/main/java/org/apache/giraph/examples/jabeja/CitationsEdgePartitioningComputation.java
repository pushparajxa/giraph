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
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.examples.jabeja.aggregators.JabejaMasterCompute;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.formats.PseudoRandomInputFormatConstants;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

/**
 * Implement the original JaBeJa-Algorithm
 * (https://www.sics.se/~amir/files/download/papers/jabeja.pdf)
 */
public class CitationsEdgePartitioningComputation
    extends
    BasicComputation<LongWritable, NodePartitioningVertexData, IntWritable, Message> {
  /**
   * The default number of different colors, if no JaBeJa.NumberOfColors is
   * provided.
   */
  // private static final int DEFAULT_NUMBER_OF_COLORS = 2;this that then after
  private static final Logger LOG = Logger
      .getLogger(NodePartitioningComputation.class);
  /**
   * The currently processed vertex
   */
  private Vertex<LongWritable, NodePartitioningVertexData, IntWritable> vertex;
  /**
   * Store random vertex givers
   */
  // private static HashMap<Long, Random> rnds = new HashMap<Long, Random>();
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
    /*
     * if (!(rnds.containsKey(Long.valueOf(vertex.getId().get())))) {
     * rnds.put(Long.valueOf(vertex.getId().get()), new Random(vertex.getId()
     * .get())); } System.out.println("The size of rands table is= " +
     * rnds.size());
     */

    if (isTimeToStop()) {
      this.vertex.voteToHalt();
      return;
    }

    if (super.getSuperstep() < 3) {
      initializeGraph(messages);
    } else {
      if (getSuperstep() % 4 == 3) {
        processLockedEdgeMessages(messages);
      }
      if (getSuperstep() % 4 == 0) {
        processReqeustMessages(messages);
      }
      if (getSuperstep() % 4 == 1) {
        processRequestResponses(messages);
      }
      if (getSuperstep() % 4 == 2) {
        processUpdateMessages(messages);
      }
    }
    /*
     * Calculate energies After sending the Request to swap messages and before
     * receiving the Request Messages.
     */

    if (getSuperstep() % 4 == 3) { // if you change this if condition change in
// SimpleAggregatorWriter also.
      System.out.println("Energy at Vertex =" + vertex.getId().get()
          + ".SuperStep=" + getSuperstep() + " is =" + calculateEnergy());
      aggregate(JabejaMasterCompute.ENEREGY, calculateEnergy());
    }

  }

  private void processLockedEdgeMessages(Iterable<Message> messages) {

    ArrayList<JabejaEdge> al = new ArrayList<JabejaEdge>();
    HashSet<String> hset = new HashSet<String>();

    for (Message msg : messages) {
      al.add(msg.getEdge());
      hset.add(msg.getEdge().getId());
    }
    verData.setLockedEdges(al);
    verData.setLockedEdgedIds(hset);

    /*
     * Now send the Request Message, if this vertex has any outward edges.
     */
    if (verData.getOutEdges().size() == 0) {
      // Do not send any request messages since this vertex doesn't have any
// outWard Edges.
    } else {
      ArrayList<JabejaEdge> neighbrs = new ArrayList<JabejaEdge>(
          verData.getOutEdges().get(this.verData.getLockedEdgeTargetVertex()).neighbours
              .values());

      /*
       * Add the outEdges of this vertex
       */
      for (Long l : verData.getOutEdges().keySet()) {
        if (l.longValue() != this.verData.getLockedEdgeTargetVertex()
            .longValue())
          neighbrs.add(verData.getOutEdges().get(l).details);
      }
      /*
       * Adds the incoming edges at this vertex.
       */
      neighbrs.addAll(verData.getInEdges().values());

      /*
       * ReqstMessage rm = new ReqstMessage(this.vertex.getId(),
       * verData.outEdges.get(lockedEdgeTargetVertex).details, neighbrs);
       */
      Message rm = new Message(this.vertex.getId().get(), verData.getOutEdges()
          .get(this.verData.getLockedEdgeTargetVertex()).details, neighbrs, 0,
          Message.RQST_MESSAGE);

      int myColor = rm.getEdge().color.get();
      rm.setEnergy(calculateEnergyOfRequest(rm, myColor));

      if (getConf().getBoolean("JaBeJa.SendRequestToRandomVertex", false)) {
        long vid = this.vertex.getId().get(), dest, tmp;
        do {
          // tmp = rnds.get(Long.valueOf(vertex.getId().get())).nextLong();
          tmp = 0;
          // tmp = (new Random(vid)).nextLong();
          // tmp = r.nextLong();
          if (tmp < 0) {
            tmp = -tmp;
          }
          dest = tmp
              % (getConf().getLong(
                  PseudoRandomInputFormatConstants.AGGREGATE_VERTICES, 10));
        } while (dest == vid);

        sendMessage(new LongWritable(dest), rm);
      } else {
        sendMessage(new LongWritable(this.verData.getLockedEdgeTargetVertex()),
            rm);
      }
    }
  }

  /**
   * This method is used to calculate the energy
   * 
   * @return
   */
  private IntWritable calculateEnergy() {
    int energy = 0;
    for (Map.Entry<Long, OwnEdge> e : verData.getOutEdges().entrySet()) {
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
    // Random r = new Random(this.vertex.getId().get());
    for (Message msg : messages) {
      // UpdateMessage upm = (UpdateMessage) msg;
      JabejaEdge je = msg.getEdge();
      /*
       * 1. Search for this edge in inEdges, if not 2. Search in outEdges.
       */
      boolean done = false;
      for (Long l : verData.getInEdges().keySet()) {
        if (l.longValue() == je.sourceVetex.get()) {
          verData.getInEdges().get(l).color = new IntWritable(je.color.get());
          done = true;
        }
      }
      if (done) {
        continue;
      } else {
        ArrayList<JabejaEdge> edges = new ArrayList<JabejaEdge>();
        for (Map.Entry<Long, OwnEdge> e : verData.getOutEdges().entrySet()) {
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

    /*
     * 1. Identify the edge you want to swap its color.If there are no outgoing
     * edges then do send any message. 2. Send information of locking this edge
     * to the vertices who are the owners of the neighbors of this edge.
     */

    ArrayList<Long> al = new ArrayList<Long>(verData.getOutEdges().keySet());
    if (al.size() == 0) {
      this.verData.setLockedEdgeTargetVertex(Long.MIN_VALUE);
// don't send request messages since you do not have any outward edges to swap
// with other vertex's edge.
    } else {
      this.verData.setLockedEdgeTargetVertex(al.get(this.verData
          .getLockEdgeIndex() % al.size()));
      this.verData.setLockEdgeIndex(this.verData.getLockEdgeIndex() + 1);
      System.out.println("Edge selected for swapping's Target Vertex= "
          + this.verData.getLockedEdgeTargetVertex().longValue());

      // Send that this edge has been selected for swapping to its neighbors.
      Set<LongWritable> vrtces = new HashSet<LongWritable>();
      for (JabejaEdge je : verData.getOutEdges().get(
          verData.getLockedEdgeTargetVertex()).neighbours.values()) {
        vrtces.add(je.sourceVetex);
      }
      /*
       * Add the source vertices of in-edges to this vertex.
       */
      for (Long l : verData.getInEdges().keySet()) {
        vrtces.add(new LongWritable(l));
      }

      /*
       * Send message to each of the vertices, which are owners of the
       * neighboring edges of this edge.
       */
      Message LockedEdgeMessage = new Message(vertex.getId().get(), verData
          .getOutEdges().get(this.verData.getLockedEdgeTargetVertex()).details,
          Message.LOCKED_EDGE);

      for (LongWritable l : vrtces) {
        sendMessage(l, LockedEdgeMessage);
      }
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
        verData.getOutEdges().get(edge.destVertex.get()).details.color = new IntWritable(
            edge.color.get());
        /* Send this update to neighbors of this edge */
        /*
         * 1. To inedges of this vertex. 2. To neighbour edges at the end of
         * other vetex for this vertex.
         */
        for (Long l : verData.getInEdges().keySet()) {
          /*
           * sendMessage(new LongWritable(l), new
           * UpdateMessage(this.vertex.getId(), edge));
           */
          sendMessage(new LongWritable(l), new Message(this.vertex.getId()
              .get(), edge, Message.UPDATE_MESSAGE));
        }
        for (JabejaEdge je : verData.getOutEdges().get(edge.destVertex.get()).neighbours
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

    if (verData.getOutEdges().keySet().size() == 1
        || verData.getOutEdges().keySet().size() == 0) {
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
        for (Long l : verData.getOutEdges().keySet()) {
          if (this.verData.getLockedEdgeTargetVertex().longValue() == Long.MIN_VALUE) {
            System.out
                .println("LockedEdge Vertex  was not set.It means something is wrong");
            LOG.log(Level.INFO,
                "LockedEdge Vertex was not set.It means something is wrong");
          } else {
            if (l.longValue() != this.verData.getLockedEdgeTargetVertex()
                .longValue()) {
              if (swapPossible(l, rm)) {
                /*
                 * 1.Update local edge's colour 2.Send Update of the edge color
                 * to the reuqetsed vertex
                 */
                int k = rm.getEdge().color.get();
                rm.getEdge().color = verData.getOutEdges().get(l).details.color;
                verData.getOutEdges().get(l).details.color = new IntWritable(k);
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
        verData.getOutEdges().get(l).details.color.get());
    int requestOldEnergy = calculateEnergyOfRequest(rm,
        rm.getEdge().color.get());
    double totalEneryOld;
    int edgeNewEnergy = calculateEnergyOfEdge(l, rm.getEdge().color.get());
    int requestNewEnergy = calculateEnergyOfRequest(rm, verData.getOutEdges()
        .get(l).details.color.get());
    double totalEnergyNew;
    int alpha = getConf().getInt("JaBeJa.alpha", 1);

    if (getConf().getBoolean("JaBeJa.annealingEnabled", false)) {
      float temp = getConf().getFloat("JaBeJa.initTemp", 2);
      float delta = getConf().getFloat("JaBeJa.delta", 0.0001f);

      totalEneryOld = Math.pow(edgeOldEnergy, alpha)
          + Math.pow(requestOldEnergy, alpha);

      totalEnergyNew = Math.pow(edgeNewEnergy, alpha)
          + Math.pow(requestNewEnergy, alpha);

      totalEneryOld = totalEneryOld
          * (temp - ((int) (getSuperstep() / 4) * delta));

    } else {

      totalEneryOld = Math.pow(edgeOldEnergy, alpha)
          + Math.pow(requestOldEnergy, alpha);

      totalEnergyNew = Math.pow(edgeNewEnergy, alpha)
          + Math.pow(requestNewEnergy, alpha);

    }

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
    OwnEdge ownEdge = verData.getOutEdges().get(lg);
    // int myColor = ownEdge.details.color.get();
    int energy = 0;
    ArrayList<JabejaEdge> edges = new ArrayList<JabejaEdge>(
        ownEdge.neighbours.values());

    /*
     * Add all the inedges
     */
    edges.addAll(verData.getInEdges().values());
    /*
     * Add all the outgoing edges from this vertex except the edge in question
     * and the lockedEdge which is sent as a request for swapping
     */
    for (Long l : verData.getOutEdges().keySet()) {
      if (l.longValue() != this.verData.getLockedEdgeTargetVertex().longValue()
          && l.longValue() != lg.longValue())
        edges.add(verData.getOutEdges().get(l).details);
    }

    /*
     * Now we have all edges , let's calculate the energu of the edge. Energy =
     * no.of edges with color different than it's. And make sure that the
     * neighbor is not a locked edge.
     */
    HashSet<String> lockedIds = verData.getLockedEdgedIds();
    for (JabejaEdge j : edges) {
      if (!lockedIds.contains(j.getId())) {
        if (j.color.get() != color) {
          energy++;
        }
      }
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
       * Announce color of the outgoing edges
       */
      announceColor();
      // LOG.trace("Successfully announce the color");
      LOG.log(Level.INFO, "Successfully announecd the color");
    } else if (getSuperstep() == 1) {
      storeZeroMessages(messages);
      // LOG.trace("Successfully stored zero messages");
      LOG.log(Level.INFO, "Successfully stored Zero Messages");
    } else if (getSuperstep() == 2) {
      storeFirstMessages(messages);
      /*
       * 1. Identify the edge you want to swap its color.If there are no
       * outgoing edges then do send any message. 2. Send information of locking
       * this edge to the vertices who are the owners of the neighbors of this
       * edge.
       */

      ArrayList<Long> al = new ArrayList<Long>(verData.getOutEdges().keySet());
      if (al.size() == 0) {
        this.verData.setLockedEdgeTargetVertex(Long.MIN_VALUE);
// don't send request messages since you do not have any outward edges to swap
// with other vertex's edge.
      } else {
        this.verData.setLockedEdgeTargetVertex(al.get(this.verData
            .getLockEdgeIndex() % al.size()));
        this.verData.setLockEdgeIndex(this.verData.getLockEdgeIndex() + 1);
        System.out.println("Edge selected for swapping's Target Vertex= "
            + this.verData.getLockedEdgeTargetVertex().longValue());

        // Send that this edge has been selected for swapping to its neighbors.
        Set<LongWritable> vrtces = new HashSet<LongWritable>();
        for (JabejaEdge je : verData.getOutEdges().get(
            verData.getLockedEdgeTargetVertex()).neighbours.values()) {
          vrtces.add(je.sourceVetex);
        }
        /*
         * Add the source vertices of in-edges to this vertex.
         */
        for (Long l : verData.getInEdges().keySet()) {
          vrtces.add(new LongWritable(l));
        }

        /*
         * Send message to each of the vertices, which are owners of the
         * neighboring edges of this edge.
         */
        Message LockedEdgeMessage = new Message(
            vertex.getId().get(),
            verData.getOutEdges().get(this.verData.getLockedEdgeTargetVertex()).details,
            Message.LOCKED_EDGE);

        for (LongWritable l : vrtces) {
          sendMessage(l, LockedEdgeMessage);
        }
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
      this.vertex.getValue().getOutEdges().get(m.getVertexId()).neighbours
          .putAll(m.getEdges());
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
      this.vertex.getValue().getInEdges().put(je.sourceVetex.get(), je);

    }
    for (Message msg : messages) {
      /* Populate the map , which will be sent */
      HashMap<Long, JabejaEdge> outEdges2 = new HashMap<Long, JabejaEdge>();
      for (Map.Entry<Long, OwnEdge> e : this.vertex.getValue().getOutEdges()
          .entrySet()) {
        outEdges2.put(e.getKey(), e.getValue().details);
      }
      /*
       * put the in-edges since they are neighbors of the edge from which we got
       * this incoming message
       */
      for (Map.Entry<Long, JabejaEdge> e : verData.getInEdges().entrySet()) {
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
      this.vertex
          .getValue()
          .getOutEdges()
          .put(
              e.getTargetVertexId().get(),
              new OwnEdge(new JabejaEdge(this.vertex.getId(), e
                  .getTargetVertexId(), new IntWritable(e.getValue().get()))));
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
