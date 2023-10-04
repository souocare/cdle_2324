package cdle.temperatures.mr;
import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SortByTemperatureReducer 
		extends Reducer<FloatWritable, Text, FloatWritable, Text> {

	@Override
	public void reduce(FloatWritable key, Iterable<Text> values, Context context) 
			throws IOException, InterruptedException {
		for (Text value : values) {
			context.write(key, value );
		}
	}
}
