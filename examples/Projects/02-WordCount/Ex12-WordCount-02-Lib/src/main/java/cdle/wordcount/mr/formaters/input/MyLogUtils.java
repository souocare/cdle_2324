package cdle.wordcount.mr.formaters.input;

import java.io.FileOutputStream;
import java.io.PrintWriter;

import org.apache.commons.logging.Log;

public class MyLogUtils {
	
	private static String logFile = "/tmp/dbg.log";

	public static void setLogFile(String logFile) {
		MyLogUtils.logFile = logFile;
	}
	
	public static String getLogFile() {
		return MyLogUtils.logFile; 
	}
	
	private static void print(String logLevel, String className) {
		System.out.printf( "For class %s log level %s is active\n", className, logLevel.toUpperCase() );
	}
	
	@SuppressWarnings("rawtypes")
	public static void showDebugLevel(Log log, Class klass) {
		if ( log.isDebugEnabled() ) {
			print( "Debug", klass.getName() );
		}
		if ( log.isErrorEnabled() ) {
			print( "Error", klass.getName() );
		}
		if ( log.isFatalEnabled() ) {
			print( "Fatal", klass.getName() );
		}
		if ( log.isInfoEnabled() ) {
			print( "Info", klass.getName() );
		}
		if ( log.isTraceEnabled() ) {
			print( "Trace", klass.getName() );
		}
		if ( log.isWarnEnabled() ) {
			print( "Warn", klass.getName() );
		}
	}
	
	public static void debug(Log log, String msg) {
		try (PrintWriter out= new PrintWriter(new FileOutputStream( MyLogUtils.getLogFile(), true))){
			out.println(msg);
			out.flush();
			out.close();
		} catch (Exception e) {
			e.printStackTrace( System.err );
			log.error( "MyLogUtils#debug(Log, String)", e);
		}

		System.out.println( msg );
		System.out.flush();

		log.debug(msg);
	}

}
