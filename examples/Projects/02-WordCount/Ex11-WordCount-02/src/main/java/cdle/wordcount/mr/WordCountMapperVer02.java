package cdle.wordcount.mr;

import java.io.IOException;
import java.net.URI;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;

import cdle.wordcount.mr.formaters.input.MyLogUtils;

public class WordCountMapperVer02
		extends Mapper<Object, Text, Text, IntWritable> {
	
	private static Log log;
	
	static {
		Class<?> klass;
		klass = org.apache.hadoop.mapreduce.Mapper.class;
		
		log = LogFactory.getLog( klass );
		MyLogUtils.showDebugLevel( log, klass );
		
		klass = WordCountMapperVer02.class;
		
		log = LogFactory.getLog( klass );
		MyLogUtils.showDebugLevel( log, klass );
	}
	
	private static final IntWritable one = new IntWritable(1);
	
    private Text word = new Text();
    
    private boolean caseSensitive;
    private Set<String> patternsToSkip = new HashSet<String>();
        
    @Override
    public void setup(Context context) 
    		throws IOException, InterruptedException {
    	
    	if ( log.isInfoEnabled() ) {
    		String msg = String.format( "%s#setup(%s) called", WordCountMapperVer02.class.getSimpleName(), context.getJobName() );
    		
    		log.info( msg );
    		System.out.printf( "[INFO] %s\n", msg );
		}
    	
    	Configuration conf;
    	conf = context.getConfiguration();
    	
    	this.caseSensitive = conf.getBoolean( "wordcount.case.sensitive", true );
    	if ( log.isDebugEnabled() ) {
    		String msg = String.format( "wordcount.case.sensitive=%b", this.caseSensitive );
    		
    		log.debug( msg );
    		System.out.printf( "[DEBUG] %s\n", msg );
		}
    	
    	boolean skipPatterns;
    	skipPatterns = conf.getBoolean( "wordcount.skip.patterns", false);
    	if ( log.isDebugEnabled() ) {
    		String msg = String.format( "wordcount.skip.patterns=%b", skipPatterns );
    		
    		log.debug( msg );
    		System.out.printf( "[DEBUG] %s\n", msg );
		}
    	
    	if ( skipPatterns==true ) {
    		URI[] patternsURIs = Job.getInstance( conf ).getCacheFiles();
    		
    		for (URI patternsURI : patternsURIs) {
    			if ( log.isDebugEnabled() ) {
    				String msg = String.format( "Current pattern file: %s", patternsURI.getPath() );
    	    		
    				log.debug( msg );
    	    		System.out.printf( "[DEBUG] %s\n", msg );
    			}
    			
    			Path patternsPath;
    			patternsPath = new Path( patternsURI.getPath() );
    			String patternsFileName;
    			patternsFileName = patternsPath.getName().toString();
    			
    			if ( patternsFileName.endsWith( ".txt" ) ) {
    				if ( log.isDebugEnabled() ) {
    					String msg = String.format( "Parsing %s", patternsFileName );
    				
    					log.debug( msg );
        	    		System.out.printf( "[DEBUG] %s\n", msg );
    				}
    				WordCountUtils.parseSkipFile( this.patternsToSkip, patternsFileName);
    			}
    			
        }
      }
    }
	
	@Override
	public void map(Object key, Text value, Context context) 
			throws IOException, InterruptedException {

		String line;
		line = ( this.caseSensitive) ? value.toString() : value.toString().toLowerCase();
		
		for (String pattern : this.patternsToSkip ) {
			line = line.replaceAll(pattern, "" );
		}
		
		StringTokenizer itr = new StringTokenizer( line );
		
		while ( itr.hasMoreTokens() ) {
			this.word.set( itr.nextToken() );
			context.write( this.word, WordCountMapperVer02.one);
			
			context.getCounter( WordCountUtils.Statistics.TotalWords ).increment( 1 );
		}
	}
	
	@Override
	public void cleanup(Context context) 
			throws IOException, InterruptedException {

		if ( log.isInfoEnabled() ) {
			String msg = String.format( "%s#cleanup(%s) called", WordCountMapperVer02.class.getSimpleName(), context.getJobName() );
    		
			log.info( msg );
    		System.out.printf( "[INFO] %s\n", msg );
		}
		
		super.cleanup( context );
	}
}
