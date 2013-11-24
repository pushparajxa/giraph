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
package org.apache.giraph.examples.jabeja.io;

import com.google.common.collect.Sets;
import org.apache.giraph.bsp.BspInputSplit;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.edge.OutEdges;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.VertexInputFormat;
import org.apache.giraph.io.VertexReader;
import org.apache.giraph.io.formats.PseudoRandomInputFormatConstants;
import org.apache.giraph.io.formats.PseudoRandomLocalEdgesHelper;
import org.apache.giraph.io.formats.PseudoRandomUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.List;
import java.util.Random;
import java.util.Set;

/**
 * Copy from {@link org.apache.giraph.io.formats.PseudoRandomVertexInputFormat}
 * with the difference of generic data types.
 *
 * @param <V> Vertex value
 * @param <E> Edge value
 */
public abstract class PseudoRandomVertexInputFormat<V extends Writable,
        E extends Writable> extends VertexInputFormat<LongWritable, V, E> {
  @Override
  public void checkInputSpecs(Configuration conf) {
  }

  @Override
  public final List<InputSplit> getSplits(
          final JobContext context,
          final int minSplitCountHint) throws IOException,
                                              InterruptedException {
    return PseudoRandomUtils.getSplits(minSplitCountHint);
  }

  @Override
  public VertexReader<LongWritable, V, E> createVertexReader(
          InputSplit split, TaskAttemptContext context) throws IOException {
    PseudoRandomVertexReader reader = new PseudoRandomVertexReader();
    reader.initialize(this);
    return reader;
  }

  /**
   * Create a data object for the randomly generated vertex with the id
   * <code>vertexId</code>
   *
   * @param vertexId the id of the vertex
   * @return the data object of the vertex
   */
  protected abstract V getVertexValue(long vertexId);

  /**
   * Create a data object for the randomly generated edge between the
   * vertices with the id <code>vertexSourceId</code> and
   * <code>vertexDestinationId</code>
   *
   * @param vertexSourceId      the id of the vertex which knows the edge as
   *                            outgoing.
   * @param vertexDestinationId the id of the vertex to which the edge is
   *                            directed
   * @return the data object for the described edge
   */
  protected abstract E getEdgeValue(
          long vertexSourceId,
          long vertexDestinationId);

  /**
   * Used by {@link PseudoRandomVertexInputFormat} to read
   * pseudo-randomly generated data.
   */
  private class PseudoRandomVertexReader extends
          VertexReader<LongWritable, V, E> {
    /**
     * The default value for how many outgoing edges each vertex should
     * have. With 3 edges per vertex, the graph has a very high probability
     * to be connected. Can be overwritten with
     * <code>giraph.pseudoRandomInputFormat.edgesPerVertex</code>
     */
    private final int defaultNumberOfEdgesPerVertex = 3;
    /**
     * The default value for how many vertices there should be.
     * Can be overwritten with
     * <code>giraph.pseudoRandomInputFormat.aggregateVertices</code>
     */
    private final int defaultNumberOfVertices = 10;
    /**
     * Logger.
     */
    private final Logger log =
            Logger.getLogger(PseudoRandomVertexReader.class);
    /**
     * An instance of the abstract {@link org.apache.giraph.examples.jabeja
     * .io.PseudoRandomVertexInputFormat} class to get data for random
     * vertices and edges.
     */
    private PseudoRandomVertexInputFormat<V, E> inputFormat;
    /**
     * Starting vertex id.
     */
    private long startingVertexId = -1;
    /**
     * Vertices read so far.
     */
    private long verticesRead = 0;
    /**
     * Total vertices to read (on this split alone).
     */
    private long totalSplitVertices = -1;
    /**
     * Aggregate vertices (all input splits).
     */
    private long aggregateVertices = -1;
    /**
     * Edges per vertex.
     */
    private int edgesPerVertex = -1;
    /**
     * BspInputSplit (used only for index).
     */
    private BspInputSplit bspInputSplit;
    /**
     * Helper for generating pseudo-random local edges.
     */
    private PseudoRandomLocalEdgesHelper localEdgesHelper;

    /**
     * Default constructor for reflection
     */
    public PseudoRandomVertexReader() {
    }

    /**
     * Initialize the inputFormat to be able to retrieve values for vertices
     * and edges.
     *
     * @param inputFormat link to the parent class to get data for vertices
     *                    and edges
     */
    public void initialize(PseudoRandomVertexInputFormat<V, E> inputFormat) {
      this.inputFormat = inputFormat;
    }

    @Override
    public void initialize(
            InputSplit inputSplit,
            TaskAttemptContext context) throws IOException {
      aggregateVertices = getConf().getLong(
              PseudoRandomInputFormatConstants.AGGREGATE_VERTICES,
              defaultNumberOfVertices);
      if (aggregateVertices <= 0) {
        throw new IllegalArgumentException(
                PseudoRandomInputFormatConstants.AGGREGATE_VERTICES + " <= 0");
      }
      if (inputSplit instanceof BspInputSplit) {
        bspInputSplit = (BspInputSplit) inputSplit;
        long extraVertices =
                aggregateVertices % bspInputSplit.getNumSplits();
        totalSplitVertices =
                aggregateVertices / bspInputSplit.getNumSplits();
        if (bspInputSplit.getSplitIndex() < extraVertices) {
          ++totalSplitVertices;
        }
        startingVertexId = (bspInputSplit.getSplitIndex() *
                            (aggregateVertices /
                             bspInputSplit.getNumSplits())) +
                           Math.min(bspInputSplit.getSplitIndex(),
                                   extraVertices);
      } else {
        throw new IllegalArgumentException(
                "initialize: Got " + inputSplit.getClass() +
                " instead of " + BspInputSplit.class);
      }
      edgesPerVertex = getConf().getInt(
              PseudoRandomInputFormatConstants.EDGES_PER_VERTEX,
              this.defaultNumberOfEdgesPerVertex);
      if (edgesPerVertex <= 0) {
        throw new IllegalArgumentException(
                PseudoRandomInputFormatConstants.EDGES_PER_VERTEX + " <= 0");
      }
      float minLocalEdgesRatio = getConf().getFloat(
              PseudoRandomInputFormatConstants.LOCAL_EDGES_MIN_RATIO,
              PseudoRandomInputFormatConstants.LOCAL_EDGES_MIN_RATIO_DEFAULT);
      localEdgesHelper = new PseudoRandomLocalEdgesHelper(aggregateVertices,
              minLocalEdgesRatio,
              getConf());
    }

    @Override
    public boolean nextVertex() throws IOException, InterruptedException {
      return totalSplitVertices > verticesRead;
    }

    @Override
    public Vertex<LongWritable, V, E>
    getCurrentVertex() throws IOException, InterruptedException {
      Vertex<LongWritable, V, E>
              vertex = getConf().createVertex();
      long vertexId = startingVertexId + verticesRead;
      LongWritable writableVertexId = new LongWritable(vertexId);

      // Seed on the vertex id to keep the vertex data the same when
      // on different number of workers, but other parameters are the
      // same.
      Random rand = new Random(vertexId);
      V vertexValue = this.inputFormat.getVertexValue(vertexId);
      // In order to save memory and avoid copying, we add directly to a
      // OutEdges instance.
      OutEdges<LongWritable, E> edges =
              getConf().createAndInitializeOutEdges(edgesPerVertex);
      Set<LongWritable> destVertices = Sets.newHashSet();

      // to prevent heaving loop edges.
      destVertices.add(writableVertexId);

      for (long i = 0; i < edgesPerVertex; ++i) {
        LongWritable destVertexId = new LongWritable();
        do {
          destVertexId.set(localEdgesHelper.generateDestVertex(vertexId, rand));
        } while (destVertices.contains(destVertexId));
        edges.add(EdgeFactory.create(destVertexId,
                this.inputFormat.getEdgeValue(vertexId, destVertexId.get())));
        destVertices.add(destVertexId);
      }
      vertex.initialize(writableVertexId, vertexValue, edges);
      ++verticesRead;
      if (log.isTraceEnabled()) {
        log.trace("next: Return vertexId=" +
                  vertex.getId().get() +
                  ", vertexValue=" + vertex.getValue() +
                  ", edges=" + vertex.getEdges());
      }
      return vertex;
    }

    @Override
    public void close() throws IOException {
    }

    @Override
    public float getProgress() throws IOException {
      return verticesRead * 100.0f / totalSplitVertices;
    }
  }
}
