package cdle.temperatures.mr;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SortByTemperatureMapper 
		extends Mapper<FloatWritable, Text, FloatWritable, Text> {

	@Override
	public void map(FloatWritable key, Text value, Context context) throws IOException, InterruptedException {

		context.write( key, value );
	}
}
