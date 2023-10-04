package cdle.configuration.mr;

import static org.junit.Assert.*;

import static org.hamcrest.CoreMatchers.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

public class ReadConfigurationVer01 {

	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws Exception {
		if ( args.length!=1 ) {
			System.err.println( "hadoop ... <input path>" );
			System.exit(-1);
		}
		
		String uri = args[0];
		Configuration conf = new Configuration();

		/*
		org.apache.hadoop.fs.FileSystem fs = org.apache.hadoop.fs.FileSystem.get( java.net.URI.create(uri), conf );
		System.out.printf( "File system scheme: %s\n", fs.getScheme() );
		System.out.printf( "File system home directory: %s\n", fs.getHomeDirectory() );
		System.out.printf( "File system URI: %s\n", fs.getUri().toString() );
		*/

		conf.addResource( new Path( uri, "configuration-1.xml" ) );

		System.out.println( "Asserting property \"color\"..." );
		assertThat( conf.get("color"), is("yellow") );

		System.out.println( "Asserting property \"size\"..." );
		assertThat( conf.getInt("size", 0), is(10) );

		System.out.println( "Asserting property \"breadth\"..." );
		assertThat( conf.get("breadth", "wide"), is("wide") );
	}
}
