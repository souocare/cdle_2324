package cdle.configuration.mr;

import static org.junit.Assert.*;

import static org.hamcrest.CoreMatchers.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

public class ReadConfigurationVer03 {

	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws Exception {
		if ( args.length!=1 ) {
			System.err.println( "hadoop ... <input path>" );
			System.exit(-1);
		}
		
		String uri = args[0];
		Configuration conf = new Configuration();
		
		System.out.println( "Adding resource from first configuration..." );
		conf.addResource( new Path( uri, "configuration-1.xml" ) );

		try {
			System.out.println( "Asserting property \"color\"..." );
			assertThat( conf.get("color"), is("yellow") );
		} catch (Exception e) { e.printStackTrace( System.err); }

		try {
			System.out.println( "Asserting property \"size\"..." );
			assertThat( conf.getInt("size", 0), is(10) );
		} catch (Exception e) { e.printStackTrace( System.err); }

		try {
			System.out.println( "Asserting property \"breadth\"..." );
			assertThat( conf.get("breadth", "wide"), is("wide") );
		} catch (Exception e) { e.printStackTrace( System.err); }

		System.out.println( "Adding resource from second configuration..." );
		conf.addResource( new Path( uri, "configuration-2.xml" ) );

		try {
			System.out.println( "Property \"size\" is overriden in second configuration file." );
			assertThat( conf.getInt("size", 0), is(12) );
		} catch (Exception e) { e.printStackTrace( System.err); }

		try {
			System.out.println( "Property marked with final aren't overriden" );
			System.out.println( "Property \"weight\" is defined in the first configuration file." );
			assertThat( conf.get("weight"), is("heavy") );
		} catch (Exception e) { e.printStackTrace( System.err); }
	}
}
