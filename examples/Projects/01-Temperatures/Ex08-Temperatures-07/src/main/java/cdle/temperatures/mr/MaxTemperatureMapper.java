package cdle.temperatures.mr;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MaxTemperatureMapper 
		extends Mapper<LongWritable, Text, TemperaturePair, FloatWritable> {

	private TemperaturePair theKey;

	private NcdcRecordParser parser;
	
	@Override
    public void setup(Context context) 
    		throws IOException, InterruptedException {
		this.parser = new NcdcRecordParser();
		
		this.theKey = new TemperaturePair();
	}

	@Override
	public void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException {

		this.parser.parse( value );
		
		if ( parser.isValidTemperature() ) {
			if ( parser.getAirTemperature()>100.0F ) {
				System.err.printf( "Temperature over 100 degrees for input: %s\n", value.toString() );
			}
			else {
				this.theKey.setYear( this.parser.getYear() );
				this.theKey.setTemperature( this.parser.getAirTemperature() );
				
				context.write( this.theKey, this.theKey.getTemperature() );
			}
		}
	}
}
