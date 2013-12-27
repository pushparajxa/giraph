package org.apache.giraph.examples.jabeja.io;

import java.io.IOException;
import java.util.regex.Pattern;

import org.apache.giraph.io.EdgeReader;
import org.apache.giraph.io.formats.IntNullTextEdgeInputFormat;
import org.apache.giraph.io.formats.TextEdgeInputFormat;
import org.apache.giraph.utils.IntPair;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class EdgeInputFormat extends
    TextEdgeInputFormat<IntWritable, NullWritable> {
  /** Splitter for endpoints */
  private static final Pattern SEPARATOR = Pattern.compile("[\t ]");

  @Override
  public EdgeReader<IntWritable, NullWritable> createEdgeReader(
      InputSplit split, TaskAttemptContext context) throws IOException {
    return new IntNullTextEdgeReader();
  }

  /**
   * {@link org.apache.giraph.io.EdgeReader} associated with
   * {@link IntNullTextEdgeInputFormat}.
   */
  public class IntNullTextEdgeReader extends
      TextEdgeReaderFromEachLineProcessed<IntPair> {
    @Override
    protected IntPair preprocessLine(Text line) throws IOException {
      String[] tokens = SEPARATOR.split(line.toString());
      return new IntPair(Integer.valueOf(tokens[0]), Integer.valueOf(tokens[1]));
    }

    @Override
    protected IntWritable getSourceVertexId(IntPair endpoints)
        throws IOException {
      return new IntWritable(endpoints.getFirst());
    }

    @Override
    protected IntWritable getTargetVertexId(IntPair endpoints)
        throws IOException {
      return new IntWritable(endpoints.getSecond());
    }

    @Override
    protected NullWritable getValue(IntPair endpoints) throws IOException {
      return NullWritable.get();
    }
  }
}
