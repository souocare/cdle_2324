package cdle.temperatures.mr;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
//import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.Text;
//import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
//import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
//import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class SortByTemperatureUsingTotalOrderPartitionerApplication 
		extends Configured 
		implements Tool {
	
	@Override
	public int run(String[] args) throws Exception {
		if ( args.length!=2 ) {
			System.err.println( "hadoop ... [generic options] <input path> <output path>" );
			
			ToolRunner.printGenericCommandUsage(System.err);
			
			return -1;
		}
		
		Configuration conf;
		conf = getConf();
		
		Job job = Job.getInstance( conf );
		
		// Generic job options
		job.setJobName( "Max temperature - Version 7" );
		job.setJarByClass( SortByTemperatureUsingTotalOrderPartitionerApplication.class );
		
		// Paths for input/output 
		FileInputFormat.addInputPath(job, new Path(args[0]) );
		FileOutputFormat.setOutputPath(job, new Path(args[1]) );
		
		// -Dmapreduce.job.inputformat.class=import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat
		//job.setInputFormatClass( SequenceFileInputFormat.class );
		
		// Map options/configurations
		job.setMapperClass( SortByTemperatureMapper.class );
		
		job.setMapOutputKeyClass( FloatWritable.class );
		job.setMapOutputValueClass( Text.class );
		
		// Reducer options/configurations
		job.setReducerClass( SortByTemperatureReducer.class );
		job.setOutputKeyClass( FloatWritable.class );
		job.setOutputValueClass( Text.class );
	    
	    // -Dmapreduce.job.outputformat.class=org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat
 		//job.setOutputFormatClass( SequenceFileOutputFormat.class );
 		
 		// -Dmapreduce.output.fileoutputformat.compress=true
 		//SequenceFileOutputFormat.setCompressOutput( job, true);
 		
 		// -Dmapreduce.output.fileoutputformat.compress.codec=org.apache.hadoop.io.compress.GzipCodec
 	    //SequenceFileOutputFormat.setOutputCompressorClass( job, GzipCodec.class );
 		
 		// -Dmapreduce.output.fileoutputformat.compress.type=BLOCK
 	    //SequenceFileOutputFormat.setOutputCompressionType(job, CompressionType.BLOCK );
	    
	    job.setPartitionerClass( TotalOrderPartitioner.class );
	    
	    InputSampler.Sampler<FloatWritable, Text> sampler;
	    sampler = new InputSampler.RandomSampler<FloatWritable, Text>(0.1, 10000, 10);
	    
	    InputSampler.writePartitionFile( job, sampler );
	    
	    String partitionFile;
	    partitionFile = TotalOrderPartitioner.getPartitionFile( conf );
	    URI partitionUri = new URI( partitionFile );
	    job.addCacheFile( partitionUri );
	    
	    System.out.printf( 
	    		"Partition file absolute location: hdfs:///user/%s/%s\n", 
	    		System.getenv("USER"), partitionUri.getPath() );

	    return job.waitForCompletion(true) ? 0 : 1;
	}
	
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run( new SortByTemperatureUsingTotalOrderPartitionerApplication(), args);
		
		System.exit( exitCode );
	}
}