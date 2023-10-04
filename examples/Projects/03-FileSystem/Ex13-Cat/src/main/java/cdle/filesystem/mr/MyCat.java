package cdle.filesystem.mr;

import java.io.InputStream;
import java.net.URL;

import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.hadoop.io.IOUtils;

public class MyCat {
	static {
		URL.setURLStreamHandlerFactory( new FsUrlStreamHandlerFactory() );
	}
  
	public static void main(String args[]) throws Exception {
		if ( args.length!=1 ) {
			System.err.println( "hadoop ... <input file>" );
			System.exit(-1);
		}
		
		System.out.println( "HDFS example" );
		System.out.println( "Displaying files from a Hadoop filesystem using a URLStreamHandler" );
		
		InputStream in = null;
		try {
			in = new URL(args[0]).openStream();
			IOUtils.copyBytes(in, System.out, 4096, false);
		} finally {
			IOUtils.closeStream(in);
		}
	}
}