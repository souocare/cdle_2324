package cdle.wordcount.mr;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.util.StringUtils;

public class WordCountUtils {

	private static final Log log = LogFactory.getLog( WordCountUtils.class );
	
	public static enum Statistics { 
		TotalWords, 
		Distincts, 
		Singletons };

	public static void parseSkipFile(Set<String> patternsToSkip, String fileName) {
    	if ( log.isDebugEnabled() ) {
    		String msg = String.format( "WordCountUtils#parseSkipFile(Set<String>, %s) called", fileName );
    		
    		log.debug( msg );
    		System.out.printf( "[DEBUG] %s\n", msg );
		}
    	
        try (BufferedReader fis=new BufferedReader(new FileReader( fileName ) ) ) {
        	String pattern = null;
        	
        	while ( ( pattern=fis.readLine() )!=null ) {
        		patternsToSkip.add( pattern );
        	}
        }
        catch (IOException ioe) {
        	String msg = String.format( 
        			"Caught exception while parsing the cached file. Details: %s", 
        			StringUtils.stringifyException(ioe) );
        	
        	log.error( msg );
    		System.out.printf( "[ERROR] %s\n", msg );
        }
    }
}
