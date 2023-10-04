package cdle.temperatures.mr;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
//import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.Text;
//import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
//import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class CleanerApplication 
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
		
		// Generic job options
		job.setJobName( "Max temperature - Version 5" );
		job.setJarByClass( CleanerApplication.class );
		
		// Paths for input/output 
		FileInputFormat.addInputPath(job, new Path(args[0]) );
		FileOutputFormat.setOutputPath(job, new Path(args[1]) );
		
		// Map options/configurations
		job.setMapperClass( CleanerMapper.class );
		
		job.setMapOutputKeyClass( FloatWritable.class );
		job.setMapOutputValueClass( Text.class );
		
		// Reduce options/configurations
		job.setNumReduceTasks( 0 );
		
		job.setOutputKeyClass( FloatWritable.class );
		job.setOutputValueClass( Text.class );
		
		// Output options/configurations
		// -Dmapreduce.job.outputformat.class=org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat
		//job.setOutputFormatClass( SequenceFileOutputFormat.class );
		
		// -Dmapreduce.output.fileoutputformat.compress=true
		//SequenceFileOutputFormat.setCompressOutput( job, true);
		
		// -Dmapreduce.output.fileoutputformat.compress.codec=org.apache.hadoop.io.compress.GzipCodec
	    //SequenceFileOutputFormat.setOutputCompressorClass( job, GzipCodec.class );
	    
	    // -Dmapreduce.output.fileoutputformat.compress.type=BLOCK
	    //SequenceFileOutputFormat.setOutputCompressionType(job, CompressionType.BLOCK );
		
		return job.waitForCompletion(true) ? 0 : 1;
	}
	
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run( new CleanerApplication(), args);
		
		System.exit( exitCode );
	}
}