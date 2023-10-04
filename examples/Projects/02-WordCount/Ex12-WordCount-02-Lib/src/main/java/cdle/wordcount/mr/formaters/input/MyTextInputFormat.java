package cdle.wordcount.mr.formaters.input;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.SplittableCompressionCodec;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

public class MyTextInputFormat 
		extends MyFileInputFormat<LongWritable, Text> {

	private static Log log;
	
	static {
		log = LogFactory.getLog( MyTextInputFormat.class );
		
		MyLogUtils.showDebugLevel( log, MyTextInputFormat.class );
	}

	private TextInputFormat theFormater;
	
	public MyTextInputFormat() {
		super();
		
		this.theFormater = new TextInputFormat();
		
		if ( log.isDebugEnabled() ) {
			MyLogUtils.debug( log, "MyTextInputFormat#ctor" );
		}
	}

	@Override
	public RecordReader<LongWritable, Text> createRecordReader(InputSplit split, TaskAttemptContext context) {
		RecordReader<LongWritable, Text> result;
		result = theFormater.createRecordReader(split, context);
		
		if ( log.isDebugEnabled() ) {
			MyLogUtils.debug( log, String.format(
					"MyTextInputFormat#createRecordReader(%s, %s) -> %s",
					split.toString(), context.getJobName(), result.toString() ) );
		}
		return result;
	}

	@Override
	protected boolean isSplitable(JobContext context, Path file) {
		boolean result;

		final CompressionCodec codec = new CompressionCodecFactory(context.getConfiguration()).getCodec(file);
		if ( codec==null ) {
			result = true;
		}
		result = codec instanceof SplittableCompressionCodec;

		if ( log.isDebugEnabled() ) {
			MyLogUtils.debug( log, String.format( 
					"MyTextInputFormat#isSplitable(%s, %s) -> %b",
					context.getJobName(), file.getName(), result ) );
		}
		return result;
	}
}
