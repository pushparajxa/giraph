package org.apache.giraph.examples.jabeja.aggregator;

import org.apache.giraph.aggregators.DoubleSumAggregator;
import org.apache.giraph.master.DefaultMasterCompute;
import org.apache.hadoop.io.DoubleWritable;

 class JabejaMasterCompute extends DefaultMasterCompute {
	
	public static final String Energy ="Energy";
	@Override
	public void compute() {
		DoubleWritable val  = getAggregatedValue(Energy);//This gives the enery after each super step.
		
	}
	@Override
	public void initialize() throws InstantiationException,
    IllegalAccessException {
  registerAggregator(Energy,DoubleSumAggregator.class);
  
}
	
}
