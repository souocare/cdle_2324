package cdle.mapreduce.mr;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MinimalMapReduce 
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
		
		job.setJarByClass( getClass() );
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new MinimalMapReduce(), args);
		System.exit(exitCode);
	}
}