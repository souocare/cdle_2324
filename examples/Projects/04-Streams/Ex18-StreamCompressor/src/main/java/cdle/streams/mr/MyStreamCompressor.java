package cdle.streams.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.util.ReflectionUtils;

public class MyStreamCompressor {
	public static void main(String[] args) throws Exception {
		if ( args.length!=1 ) {
			System.err.println( "hadoop ... <class name>" );
			System.exit(-1);
		}
		
		String codecClassname = args[0];
		Class<?> codecClass = Class.forName( codecClassname );
		Configuration conf = new Configuration();
		CompressionCodec codec = ( CompressionCodec )
		ReflectionUtils.newInstance(codecClass, conf);
		
		CompressionOutputStream out = codec.createOutputStream( System.out );
		IOUtils.copyBytes(System.in, out, 4096, false);
		out.finish();
	}
}