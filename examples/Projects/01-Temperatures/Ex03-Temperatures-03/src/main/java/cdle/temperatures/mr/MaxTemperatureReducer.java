package cdle.temperatures.mr;

import java.io.IOException;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class MaxTemperatureReducer
			extends Reducer<IntWritable, FloatWritable, IntWritable, FloatWritable> {
	
	@Override
	public void reduce(IntWritable key, Iterable<FloatWritable> values, Context context)
			throws IOException, InterruptedException {
		float maxValue = Float.MIN_VALUE;
		for (FloatWritable value : values) {
			maxValue = Math.max(maxValue, value.get() );
		}
		context.write(
			key, 
			new FloatWritable( maxValue ) );
	}
}
