package cdle.temperatures.mr;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MaxTemperatureMapper 
		extends Mapper<LongWritable, Text, IntWritable, FloatWritable> {

	enum Temperature {
		OVER_100,
		BELLOW_100
	}

	private NcdcRecordParser parser = new NcdcRecordParser();
	
	private static final Log LOG = LogFactory.getLog( MaxTemperatureMapper.class );
	
	private static int recordsProcessed = 0;

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		parser.parse( value );

		if ( parser.isValidTemperature() ) {
			float airTemperature = parser.getAirTemperature();
			if (airTemperature > 100.0F) {
				System.err.printf("Temperature over 100 degrees for input: %s\n", value.toString());
				context.setStatus("Detected possibly corrupt record: see logs.");
				context.getCounter( Temperature.OVER_100 ).increment( 1 );
			}
			else {
				context.getCounter( Temperature.BELLOW_100 ).increment( 1 );
			}
			if ( MaxTemperatureMapper.recordsProcessed==0 ) {
				if ( LOG.isInfoEnabled() ) {
					LOG.info( String.format( "[%s] going to write first output.", Thread.currentThread().getName() ) );
				}
				
				if ( LOG.isDebugEnabled() ) {
					LOG.debug( String.format( "Map value: %s", value.toString() ) );
				}
				++MaxTemperatureMapper.recordsProcessed;
			}
			context.write( new IntWritable( parser.getYear() ), new FloatWritable(airTemperature) );
		}
	}
}
