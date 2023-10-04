package cdle.temperatures.mr;

import org.apache.hadoop.io.FloatWritable;
//import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class MaxTemperaturePartitioner 
		extends Partitioner<TemperaturePair, FloatWritable> {

	@Override
	public int getPartition(TemperaturePair key, FloatWritable value, int numPartitions) {

		// multiply by 127 to perform some mixing
		return Math.abs( key.getYear().get() * 127) % numPartitions;
    }
  }
