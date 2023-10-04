 package cdle.mapreduce.mr;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MinimalMapReduceWithDefaults 
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
		
		job.setInputFormatClass( TextInputFormat.class );
	    job.setMapperClass( Mapper.class);
	    
	    job.setMapOutputKeyClass( LongWritable.class );
	    job.setMapOutputValueClass( Text.class );
	    
	    job.setPartitionerClass( HashPartitioner.class );
	    
	    job.setNumReduceTasks(1);
	    job.setReducerClass( Reducer.class );
	    job.setOutputKeyClass( LongWritable.class );
	    job.setOutputValueClass( Text.class );
	    job.setOutputFormatClass( TextOutputFormat.class );
		
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new MinimalMapReduceWithDefaults(), args);
		System.exit(exitCode);
	}
}