package org.apache.giraph.examples.jabeja.io;

import java.io.IOException;

import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.formats.TextVertexOutputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class VertexIdOutputFormat extends
    TextVertexOutputFormat<IntWritable, NullWritable, NullWritable> {

  @Override
  public TextVertexWriter createVertexWriter(TaskAttemptContext context) {
    return new VertexIdWriter();

  }

  protected class VertexIdWriter extends TextVertexWriterToEachLine {

    @Override
    protected Text convertVertexToLine(
        Vertex<IntWritable, NullWritable, NullWritable> vertex)
        throws IOException {

      StringBuilder str = new StringBuilder();

      str.append(vertex.getId().toString() + "\n");

      return new Text(str.toString());
    }

  }
}
