package cdle.temperatures.mr;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CleanerMapper 
		extends Mapper<LongWritable, Text, FloatWritable, Text> {

	private FloatWritable theKey = new FloatWritable();

	private NcdcRecordParser parser = new NcdcRecordParser();

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		parser.parse( value );
		
		if ( parser.isValidTemperature() ) {
			if ( parser.getAirTemperature()>100.0F ) {
				System.err.printf( "Temperature over 100 degrees for input: %s\n", value.toString() );
			}
			else {
				theKey.set( parser.getAirTemperature() );
				
				context.write( theKey, value );
			}
		}
	}
}
