package cdle.wordcount.mr.formaters.input;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public abstract class MyFileInputFormat<K, V> 
		extends FileInputFormat<K, V> {
	
	private static Log log;
	
	static {
		log = LogFactory.getLog( MyFileInputFormat.class );
		
		MyLogUtils.showDebugLevel( log, MyFileInputFormat.class );
		
		if ( log.isDebugEnabled() ) {
			MyLogUtils.debug( log, "MyFileInputFormat<K, V>#ctor" );
		}
	}

	public static void setInputDirRecursive(Job job, boolean inputDirRecursive) {
		FileInputFormat.setInputDirRecursive(job, inputDirRecursive);

		if ( log.isDebugEnabled() ) {
			MyLogUtils.debug( log, String.format( 
					"MyFileInputFormat#setInputDirRecursive(%s, %b)",
					job.getJobName(), inputDirRecursive ) );
		}
	}
	
	public static boolean getInputDirRecursive(JobContext job) {
		boolean result;
		result = FileInputFormat.getInputDirRecursive( job );

		if ( log.isDebugEnabled() ) {
			MyLogUtils.debug( log, String.format( 
					"MyFileInputFormat#getInputDirRecursive(%s) -> %b",
					job.getJobName(), result ) );
		}

		return result;
	}
	
	@Override
	protected long getFormatMinSplitSize() {
		long result;
		result = super.getFormatMinSplitSize();

		if ( log.isDebugEnabled() ) {
			MyLogUtils.debug( log, String.format( 
					"MyFileInputFormat#getFormatMinSplitSize() -> %d",
					result ) );
		}
		
	    return result;
	}
	
	@Override
	protected boolean isSplitable(JobContext context, Path filename) {
		boolean result;
		result = super.isSplitable(context, filename);
		
		if ( log.isDebugEnabled() ) {
			MyLogUtils.debug( log, String.format( 
					"MyFileInputFormat#isSplitable(%s, %s) -> %b",
					context.getJobName(), filename.getName(), result ) );
		}
		
	    return result;
	}
	
	public static void setInputPathFilter(Job job, Class<? extends PathFilter> filter) {
		FileInputFormat.setInputPathFilter( job, filter);
		
		if ( log.isDebugEnabled() ) {		
			MyLogUtils.debug( log, String.format( 
					"MyFileInputFormat#setInputPathFilter(%s, %s)",
					job.getJobName(), filter.getName() ) );
		}
	}
	
	public static void setMinInputSplitSize(Job job, long size) {
		FileInputFormat.setMinInputSplitSize(job, size);

		if ( log.isDebugEnabled() ) {
			MyLogUtils.debug( log, String.format( 
					"MyFileInputFormat#setMinInputSplitSize(%s, %d)",
					job.getJobName(), size ) );
		}
	}

	public static long getMinSplitSize(JobContext job) {
		long result;
		result = FileInputFormat.getMinSplitSize( job );
		
		if ( log.isDebugEnabled() ) {
			MyLogUtils.debug( log, String.format( 
					"MyFileInputFormat#getMinSplitSize(%s) -> %d",
					job.getJobName(), result ) );
		}
		
		return result;
	}

	public static void setMaxInputSplitSize(Job job, long size) {
		FileInputFormat.setMaxInputSplitSize( job, size );
		
		if ( log.isDebugEnabled() ) {
			MyLogUtils.debug( log, String.format( 
					"MyFileInputFormat#setMaxInputSplitSize(%s, %d)",
					job.getJobName(), size ) );
		}
	}

	public static long getMaxSplitSize(JobContext context) {
		long result;
		result = FileInputFormat.getMaxSplitSize( context );
		
		if ( log.isDebugEnabled() ) {
			MyLogUtils.debug( log, String.format( 
					"MyFileInputFormat#getMaxSplitSize(%s) -> %d",
					context.getJobName(), result ) );
		}
		
		return result;
	}
	
	public static PathFilter getInputPathFilter(JobContext context) {
		PathFilter result;
		result = FileInputFormat.getInputPathFilter( context );

		if ( log.isDebugEnabled() ) {
			MyLogUtils.debug( log, String.format( 
					"MyFileInputFormat#getInputPathFilter(%s) -> %s",
					context.getJobName(), result.toString() ) );
		}
		
		return result;
	}
	
	@Override
	protected List<FileStatus> listStatus(JobContext job) throws IOException {
		List<FileStatus> result;
		result = super.listStatus(job);

		if ( log.isDebugEnabled() ) {
			StringBuilder resultAsString;
			resultAsString = new StringBuilder();
			resultAsString.append( "[" );
			resultAsString.append( String.format( "{<Size=%d>},{", result.size() ) );
			boolean isFirst = true;
			for (FileStatus currentStatus : result) {
				
				if ( isFirst==true ) {
					isFirst=false;
				}
				else {
					resultAsString.append( " ," );
				}
				
				resultAsString.append( String.format( "{Name=%s}", currentStatus.getPath().getName() ) );
			}
			resultAsString.append( "}]" );
			
			MyLogUtils.debug( log, String.format( 
					"MyFileInputFormat#listStatus(%s) -> %s",
					job.getJobName(), resultAsString.toString() ) );
		}
		
		return result;
	}
	
	@SuppressWarnings("deprecation")
	@Override
	protected void addInputPathRecursively(
			List<FileStatus> result, 
			FileSystem fs, 
			Path path, 
			PathFilter inputFilter) throws IOException {
		
		super.addInputPathRecursively(result, fs, path, inputFilter);
		
		if ( log.isDebugEnabled() ) {
			MyLogUtils.debug( log, String.format( 
					"MyFileInputFormat#addInputPathRecursively(%s, %s, %s, %s) -> %s",
					result.toString(), fs.getName(), path.getName(), inputFilter.toString() ) );
		}
	}
	
	@Override
	protected FileSplit makeSplit (Path file, long start, long length, String[] hosts) {
		FileSplit result;
		result = super.makeSplit(file, start, length, hosts);
		
		if ( log.isDebugEnabled() ) {
			MyLogUtils.debug( log, String.format( 
					"MyFileInputFormat#makeSplit(%s, %d, %d, %s) -> %s",
					file.getName(), start, length, java.util.Arrays.toString( hosts ), result.toString() ) );
		}
		
		return result;
	}
	
	@Override
	protected FileSplit makeSplit(Path file, long start, long length, String[] hosts, String[] inMemoryHosts) {
		FileSplit result;
		result = super.makeSplit(file, start, length, hosts, inMemoryHosts);
		
		if ( log.isDebugEnabled() ) {
			MyLogUtils.debug( log, String.format( 
					"MyFileInputFormat#makeSplit(%s, %d, %d, %s, %s) -> %s",
					file.getName(), start, length, java.util.Arrays.toString( hosts ), java.util.Arrays.toString( inMemoryHosts ), result.toString() ) );
		}
		
		return result;
	}
	
	@Override
	public List<InputSplit> getSplits(JobContext job) throws IOException {
		List<InputSplit> result;
		result = super.getSplits( job );
		
		if ( log.isDebugEnabled() ) {
			try {
				StringBuilder resultAsString;
				resultAsString = new StringBuilder();
				resultAsString.append( "[" );
				boolean isFirst = true;
				for (InputSplit inputSplit : result) {
					
					if ( isFirst==true ) {
						isFirst=false;
					}
					else {
						resultAsString.append( " ," );
					}
					
					resultAsString.append( String.format( "{Length=%d}", inputSplit.getLength() ) );
				}
				resultAsString.append( "]" );
				
				MyLogUtils.debug( log, String.format( 
						"MyFileInputFormat#getSplits(%s) -> %s",
						job.getJobName(), resultAsString.toString() ) );
			}
			catch (InterruptedException iex) {
				log.debug("Interrupted exception in MyFileInputFormat#getSplits()", iex);
			}
		}
	
		return result;
	}
	
	@Override
	protected long computeSplitSize(long blockSize, long minSize, long maxSize) {
		long result;
		result = super.computeSplitSize( blockSize, minSize, maxSize);
		
		if ( log.isDebugEnabled() ) {
			MyLogUtils.debug( log, String.format( 
					"MyFileInputFormat#computeSplitSize(%d, %d, %d) -> %d",
					blockSize, minSize, maxSize, result ) );
		}
		
		return result;
	}

	@Override
	protected int getBlockIndex(BlockLocation[] blkLocations, long offset) {
		int result;
		result = super.getBlockIndex( blkLocations, offset);

		if ( log.isDebugEnabled() ) {
			MyLogUtils.debug( log, String.format( 
					"MyFileInputFormat#getBlockIndex(%s, %d) -> %d",
					blkLocations.toString(), offset, result ) );
		}
		
		return result;
	}
	
	public static void setInputPaths(Job job, String commaSeparatedPaths) throws IOException {
		FileInputFormat.setInputPaths(job, commaSeparatedPaths);

		if ( log.isDebugEnabled() ) {
			MyLogUtils.debug( log, String.format( 
					"MyFileInputFormat#setInputPaths(%s, %s)",
					job.getJobName(), commaSeparatedPaths ) );
		}
	}
	
	public static void setInputPaths(Job job, Path... inputPaths) throws IOException {
		FileInputFormat.setInputPaths(job, inputPaths);
		
		if ( log.isDebugEnabled() ) {
			MyLogUtils.debug( log, String.format( 
					"MyFileInputFormat#setInputPaths(%s, Path...)",
					job.getJobName() ) );
		}
	}

	public static void addInputPaths(Job job, String commaSeparatedPaths) throws IOException {
		FileInputFormat.addInputPaths(job, commaSeparatedPaths);
		
		if ( log.isDebugEnabled() ) {
			MyLogUtils.debug( log, String.format( 
					"MyFileInputFormat#addInputPaths(%s, %s)",
					job.getJobName(), commaSeparatedPaths ) );
		}
	}

	public static void addInputPath(Job job, Path path) throws IOException {
		FileInputFormat.addInputPath(job, path);
		
		if ( log.isDebugEnabled() ) {
			MyLogUtils.debug( log, String.format( 
					"MyFileInputFormat#addInputPath(%s, %s)",
					job.getJobName(), path.getName() ) );
		}
	}

	public static Path[] getInputPaths(JobContext context) {
		Path[] result;
		result = FileInputFormat.getInputPaths(context);
		
		if ( log.isDebugEnabled() ) {
			MyLogUtils.debug( log, String.format( 
					"MyFileInputFormat#getInputPaths(%s) -> %s",
					context.getJobName(), java.util.Arrays.toString( result ) ) );
		}
		
		return result;
	}
}
