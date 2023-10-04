package cdle.streams.mr;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;

public class MyFileDecompressorVer02 {

	public static void main(String[] args) throws Exception {
		if ( args.length!=1 ) {
			System.err.println( "hadoop ... <input file>" );
			System.exit(-1);
		}
		
		String uri = args[0];
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(uri), conf);

		Path[] inputPath = { new Path(args[0]) };
		FileStatus[] status = fs.listStatus(inputPath);

		List<FileStatus> files;
		files = new ArrayList<FileStatus>();

		for (FileStatus currentStatus : status) {
			if (currentStatus.isFile()) {
				files.add(currentStatus);
			}
		}

		Path[] listedPaths = FileUtil.stat2Paths(status);
		for (Path currentPath : listedPaths) {
			System.out.println(currentPath);

			CompressionCodecFactory factory = new CompressionCodecFactory(conf);
			CompressionCodec codec = factory.getCodec(currentPath);

			if (codec == null) {
				System.err.println("No codec found for " + currentPath.getName());
			}
			else {
				String outputUri = CompressionCodecFactory.removeSuffix(currentPath.toString(),
						codec.getDefaultExtension());

				InputStream in = null;
				OutputStream out = null;
				try {
					in = codec.createInputStream(fs.open(currentPath));
					out = fs.create(new Path(outputUri));
					IOUtils.copyBytes(in, out, conf);
				}
				finally {
					IOUtils.closeStream(in);
					IOUtils.closeStream(out);
				}
			}
		}
	}
}