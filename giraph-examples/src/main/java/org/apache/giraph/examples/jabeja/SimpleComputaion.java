package org.apache.giraph.examples.jabeja;

import java.io.IOException;

import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;

public class SimpleComputaion extends
    BasicComputation<IntWritable, NullWritable, NullWritable, Message> {

  @Override
  public void compute(Vertex<IntWritable, NullWritable, NullWritable> vertex,
      Iterable<Message> messages) throws IOException {
    // TODO Auto-generated method stub
    if (getSuperstep() == 1)
      vertex.voteToHalt();

  }

}
