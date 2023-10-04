package cdle.filesystem.mr;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

public class MyCatFileSystemTwice {
	public static void main(String[] args) throws Exception {
		if ( args.length!=1 ) {
			System.err.println( "hadoop ... <input file>" );
			System.exit(-1);
		}
		
		System.out.println( "HDFS example" );
		System.out.println( "Displaying files from a Hadoop filesystem using seek()" );
		
		String uri = args[0];
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(uri), conf);
		FSDataInputStream in = null;
		try {
			in = fs.open(new Path(uri));
			IOUtils.copyBytes(in, System.out, 4096, false);
			// go back to the start of the file
			in.seek(0);
			IOUtils.copyBytes(in, System.out, 4096, false);
		}
		finally {
		  IOUtils.closeStream(in);
		}
	}
}