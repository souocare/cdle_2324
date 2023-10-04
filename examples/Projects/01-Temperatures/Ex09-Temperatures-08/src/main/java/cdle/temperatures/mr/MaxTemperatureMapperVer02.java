package cdle.temperatures.mr;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MaxTemperatureMapperVer02 
		extends Mapper<LongWritable, Text, Text, IntWritable> {

	private NcdcRecordParser parser = new NcdcRecordParser();
	
	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		
		this.parser.parse(value);

		if ( this.parser.isValidTemperature() ) {
	    	context.write(
	    			new Text( this.parser.getYear() ) ,
	    			new IntWritable( this.parser.getAirTemperature() ) );
	    }
	}
}

