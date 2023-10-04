package cdle.configuration.mr;

import static org.junit.Assert.*;

import static org.hamcrest.CoreMatchers.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

public class ReadConfigurationVer02 {

	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws Exception {
		if ( args.length!=1 ) {
			System.err.println( "hadoop ... <input path>" );
			System.exit(-1);
		}
		
		String uri = args[0];
		Configuration conf = new Configuration();
		
		conf.addResource( new Path( uri, "configuration-1.xml" ) );
		conf.addResource( new Path( uri, "configuration-2.xml" ) );

		System.out.println( "Property \"size\" is overriden in second configuration file." );
		assertThat( conf.getInt("size", 0), is(12) );
		
		System.out.println( "Property marked with final aren't overriden" );
		System.out.println( "Property \"weight\" is defined in the first configuration file." );
		assertThat( conf.get("weight"), is("heavy") );
	}
}
