package cdle.wordcount.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;

public class WordCountApplication {
	
	public static void main(String[] args) throws Exception {
		if ( args.length<2 ) {
			System.err.println( "hadoop ... <input path> <output path> [number of reducers] [compression codec]" );
			System.exit(-1);
		}
		
		Job job = Job.getInstance( new Configuration() );
		
		job.setJarByClass( WordCountApplication.class );
		job.setJobName( "Word Count Ver 1" );
		
		FileInputFormat.addInputPath( job, new Path(args[0]) );
		FileOutputFormat.setOutputPath( job, new Path(args[1]) );
		
		job.setMapperClass( WordCountMapper.class );
		job.setCombinerClass( WordCountReducer.class );
		job.setReducerClass( WordCountReducer.class );
		
		// Output types of map function
		job.setMapOutputKeyClass( Text.class );
		job.setMapOutputValueClass( IntWritable.class );
		
		try {
			int numberOfReducers;
			numberOfReducers = Integer.parseInt( args[2] );
			job.setNumReduceTasks( numberOfReducers );
			System.out.printf( "Setting number of reducers to %d\n", numberOfReducers );
		}
		catch (Exception e) {
			System.out.println( "Using default number (1) of reducers" );
		}
		
		// Output types of reduce function
		job.setOutputKeyClass( Text.class );
		job.setOutputValueClass( IntWritable.class );

		if (args.length > 3) {
			String codecName = args[3].toLowerCase();
			if (codecName.equals("gzip")) {
				job.getConfiguration().setBoolean("mapreduce.output.fileoutputformat.compress", true);
				job.getConfiguration().setClass("mapreduce.output.fileoutputformat.compress.codec", GzipCodec.class, CompressionCodec.class);
				System.out.println("Using Gzip compression codec for output");
			} 
			else{
				System.out.println("It only works with gzip compression");
			}
			// Você pode adicionar outras opções de codec de compressão aqui, se necessário.
		}
		
		System.exit( job.waitForCompletion(true) ? 0 : 1 );
	}
}
