package cdle.wordcount.mr;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import cdle.wordcount.mr.formaters.input.MyLogUtils;

public class WordCountReducerVer02
		extends Reducer<Text, IntWritable, Text, IntWritable> {
	
	private static Log log;
	
	static {
		Class<?> klass;
		klass = org.apache.hadoop.mapreduce.Reducer.class;
		
		log = LogFactory.getLog( klass );
		MyLogUtils.showDebugLevel( log, klass );
		
		klass = WordCountReducerVer02.class;
		
		log = LogFactory.getLog( klass );
		MyLogUtils.showDebugLevel( log, klass );
	}
	
	private IntWritable result = new IntWritable();
	
	@Override
    public void setup(Context context) 
    		throws IOException, InterruptedException {
    	
    	if ( log.isInfoEnabled() ) {
    		String msg;
    		msg = String.format( "%s#setup(%s) called", WordCountReducerVer02.class.getSimpleName(), context.getJobName() );
    		
    		log.info( msg );
    		System.out.printf( "[INFO] %s\n", msg );
		}
    	super.setup( context );
	}
	
	@Override
	public void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		
		int sum = 0;
		for (IntWritable val : values) {
			sum += val.get();
		}
		
		this.result.set( sum );
		
		context.write(key, this.result );
		
		context.getCounter( WordCountUtils.Statistics.Distincts ).increment( 1 );
		
		if ( sum==1 ) {
			context.getCounter( WordCountUtils.Statistics.Singletons ).increment( 1 );
		}
	}
	
	@Override
	public void cleanup(Context context) 
			throws IOException, InterruptedException {

		if ( log.isInfoEnabled() ) {
			String msg = String.format( "%s#cleanup(%s) called", WordCountReducerVer02.class.getSimpleName(), context.getJobName() );
			
    		log.info( msg );
    		System.out.printf( "[INFO] %s\n", msg );
		}
		
		super.cleanup( context );
	}
}
