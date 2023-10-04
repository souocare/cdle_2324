package cdle.filesystem.mr;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;

public class MyListStatus {
	public static void main(String[] args) throws Exception {
		if ( args.length!=1 ) {
			System.err.println( "hadoop ... <input path>" );
			System.exit(-1);
		}
		
		System.out.println( "File system stats example" );
		System.out.println( "List files in a directory" );
		
		String uri = args[0];
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(uri), conf);
    
		Path[] paths = new Path[args.length];
		for (int i = 0; i < paths.length; i++) {
			paths[i] = new Path(args[i]);
		}
    
		FileStatus[] status = fs.listStatus(paths);
		Path[] listedPaths = FileUtil.stat2Paths(status);
		for (Path p : listedPaths) {
		  System.out.println(p);
		}
	}
}
