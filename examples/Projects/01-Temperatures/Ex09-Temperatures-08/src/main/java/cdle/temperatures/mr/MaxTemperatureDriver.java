package cdle.temperatures.mr;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MaxTemperatureDriver 
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
		job.setJobName( "Max temperature" );
		
		job.setJarByClass( getClass() );
		
		FileInputFormat.addInputPath(job, new Path(args[0]) );
		FileOutputFormat.setOutputPath(job, new Path(args[1]) );

		//job.setMapperClass( MaxTemperatureMapperVer01.class );
		job.setMapperClass( MaxTemperatureMapperVer02.class );

		job.setCombinerClass( MaxTemperatureReducer.class );
		job.setReducerClass( MaxTemperatureReducer.class );
		job.setOutputKeyClass( Text.class );
		job.setOutputValueClass( IntWritable.class );

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new MaxTemperatureDriver(), args);
		System.exit(exitCode);
	}
}