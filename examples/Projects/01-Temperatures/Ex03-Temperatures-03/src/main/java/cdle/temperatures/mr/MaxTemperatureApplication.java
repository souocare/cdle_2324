package cdle.temperatures.mr;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MaxTemperatureApplication 
		extends Configured 
		implements Tool {
	
	@Override
	public int run(String[] args) throws Exception {
		if ( args.length!=2 ) {
			System.err.println( "hadoop ... [generic options] <input path> <output path>" );
			
			ToolRunner.printGenericCommandUsage(System.err);
			
			return -1;
		}
		
		Job job = Job.getInstance( getConf() );
		job.setJobName( "Max temperature - Version 4" );
		
		job.setJarByClass( MaxTemperatureApplication.class );
		
		FileInputFormat.addInputPath(job, new Path(args[0]) );
		FileOutputFormat.setOutputPath(job, new Path(args[1]) );
		
		job.setMapperClass( MaxTemperatureMapper.class );
		job.setReducerClass( MaxTemperatureReducer.class );
		
		// Output types of map function
		job.setMapOutputKeyClass( IntWritable.class );
		job.setMapOutputValueClass( FloatWritable.class );
		
		// Output types of reduce function
		job.setOutputKeyClass( IntWritable.class );
		job.setOutputValueClass( FloatWritable.class );
		
		return job.waitForCompletion(true) ? 0 : 1;
	}
	
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run( new MaxTemperatureApplication(), args);
		
		System.exit( exitCode );
	}
}