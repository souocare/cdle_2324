package cdle.wordcount.mr;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import cdle.wordcount.mr.formaters.input.MyFileInputFormat;
import cdle.wordcount.mr.formaters.input.MyLogUtils;

public class WordCountApplicationVer02 
		extends Configured 
		implements Tool {
	
	private static Log log;
	
	static {
		Class<?> klass;
		klass = WordCountApplicationVer02.class;
		
		log = LogFactory.getLog( klass );
		MyLogUtils.showDebugLevel( log, klass );
	}
	
	public static void main(String[] args) throws Exception {
		System.exit( ToolRunner.run( new WordCountApplicationVer02(), args) );
	}	

	@Override
	public int run(String[] args) throws Exception {
		if ( args.length!=2 ) {
			System.err.println( "hadoop ... [generic options] <input path> <output path>" );
			
			ToolRunner.printGenericCommandUsage(System.err);
			
			return -1;
		}		
		
		System.out.printf( "MyLogUtils.getLogFile() -> %s\n", MyLogUtils.getLogFile() );
		
		// Create a job
		Job job = Job.getInstance( getConf() );
		
		// Set job name
		job.setJobName( "Word Count - Version 02" );
		
		// Set jar file
		job.setJarByClass( WordCountApplicationVer02.class );
		
		// Set map, reducer and combiner
		job.setMapperClass( WordCountMapperVer02.class );
		// -D mapreduce.job.combine.class=...
		//job.setCombinerClass( WordCountCombinerVer02.class );
		job.setReducerClass( WordCountReducerVer02.class );
		
		
		
		// Set output types of map function
		job.setMapOutputKeyClass( Text.class );
		job.setMapOutputValueClass( IntWritable.class );
				
		// Set output types of reduce function
		job.setOutputKeyClass( Text.class );
		job.setOutputValueClass( IntWritable.class );
		
		// Set input path
		MyFileInputFormat.addInputPath(job, new Path(args[0]) );
		
		// Set input class format
		// Can be done using properties
		// -D mapreduce.job.inputformat.class=cdle.wordcount.mr.formaters.input.MyTextInputFormat
		//job.setInputFormatClass( cdle.wordcount.mr.formaters.input.MyTextInputFormat.MyTextInputFormat.class );
		
		//job.setOutputFormatClass( ... );
				
		// -D mapreduce.output.fileoutputformat.compress=true
		// -D mapreduce.output.fileoutputformat.compress=false
		
		// Set output path
		FileOutputFormat.setOutputPath(job, new Path(args[1]) );

		boolean retCode = job.waitForCompletion( true );

		if ( retCode==true ) {
			Counter distinctsCounter = job.getCounters().findCounter( WordCountUtils.Statistics.Distincts );
			Counter singletonsCounter = job.getCounters().findCounter( WordCountUtils.Statistics.Singletons );
			Counter totalCounter = job.getCounters().findCounter( WordCountUtils.Statistics.TotalWords );
			
			System.out.printf( "Number of distinct n-grams: %d\n", distinctsCounter.getValue() );
			System.out.printf( "Number of singletons n-grams: %d\n", singletonsCounter.getValue() );
			System.out.printf( "Number of total n-grams: %d\n", totalCounter.getValue() );
		}
		else {
			System.out.printf( "Job failed!\n" );
		}


		return retCode ? 0 : 1;
	}
}
