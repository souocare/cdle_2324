package cdle.filesystem.mr;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;

class MyProgressable implements Progressable {
	
	@Override
	public void progress() {
		System.out.print(".");
	}
}

public class MyFileCopyWithProgress {
	public static void main(String[] args) throws Exception {
		if ( args.length<2 ) {
			System.err.println( "hadoop ... <local input file> <remote output file> [block size]" );
			System.exit(-1);
		}
		
		String source = args[0];
		String destination = args[1];
		
		int blockSize = 4096;		
		try {
			blockSize = Integer.parseInt( args[2] );
		}
		catch (Exception e) {}
		
		System.out.println( "Progressable example" );
		System.out.println( "Copyping files showing progress of the copy" );
		
		System.out.printf( "Source file: %s\n", source);
		System.out.printf( "Destination file: %s\n", destination);
		System.out.printf( "Block size: %s\n", blockSize);
    
		InputStream in = new BufferedInputStream( new FileInputStream( source ) );
    
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get( URI.create(destination), conf );
		OutputStream out = fs.create( new Path( destination ), new MyProgressable() );
		
		IOUtils.copyBytes(in, out, blockSize, true);
		
		System.out.println( "" );
	}
}