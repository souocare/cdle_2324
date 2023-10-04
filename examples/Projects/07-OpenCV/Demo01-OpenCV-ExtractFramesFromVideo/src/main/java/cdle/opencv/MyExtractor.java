package cdle.opencv;

import java.io.File;
import java.io.IOException;

import org.opencv.core.Core;
import org.opencv.core.Mat;
import org.opencv.imgcodecs.Imgcodecs;
import org.opencv.imgproc.Imgproc;
import org.opencv.videoio.VideoCapture;
import org.opencv.videoio.Videoio;

public class MyExtractor 
		implements Runnable {

	private File inputFile;
	private File outputDirectory;

	public MyExtractor(File inputFile, File outputDirectory) {
		try {
			this.inputFile = new File( inputFile.getCanonicalPath() );
			this.outputDirectory = new File( outputDirectory.getCanonicalPath() );
		}
		catch (IOException e) {
			throw new IllegalArgumentException(e);
		}
		LibraryLoader.loadLibraries(
				new String[] {},
				/*
				// The following libraries are needed by OpenCV core library
				// However, if the OpenCV and FFmpeg are installed in the Operating System 
				// they are loaded automatically
				// This is only required for Linux/Unix/MacOS systens
				new String[] {
						"swresample",	// libswresample.so
						"avutil",		// libavutil.so
						"avcodec",		// libavcodec.so
						"avformat",		// libswscale.so
						"swscale"		// libswscale.so
						}
				*/
				new String[] { Core.NATIVE_LIBRARY_NAME } 
		);		
	}

	@Override
	public void run() {
		String input, output;
		input = inputFile.getAbsoluteFile().toString();
		output = outputDirectory.getAbsoluteFile().toString();
			
		System.out.printf( "Processing %s\n", input );
		System.out.printf( "Output directory: %s\n", output );
			
		outputDirectory.mkdirs();
			
		VideoCapture cap;
		cap = new VideoCapture();
			
		cap.open( input );

	    int numberOfFramesInVideo, framesPerSecond, frameNumber;
	    numberOfFramesInVideo = (int) cap.get( Videoio.CAP_PROP_FRAME_COUNT );
	    framesPerSecond = (int) cap.get( Videoio.CAP_PROP_FPS );

	    Mat frame, frameGray;
	    frame = new Mat();
	    frameGray = new Mat();
		    
	    if ( cap.isOpened() ) {
	    	System.out.printf( "Video is opened\n" );
	    	System.out.printf( "Frames per second: %d\n", framesPerSecond );
	        System.out.printf( "Number of frames in video: %d\n", numberOfFramesInVideo );
	        System.out.printf( "Video length (in minutes): %1.2f\n", ((float)(numberOfFramesInVideo))/(60.0*framesPerSecond) );
			
	        String frameName, frameNameGray;
	        
	        String formater, formaterGray;
	        formater = String.format( "%%s%%s%%0%dd.jpeg", (int) (Math.log10(numberOfFramesInVideo) + 1) );
	        formaterGray = String.format( "%%s%%s%%0%dd-gray.jpeg", (int) (Math.log10(numberOfFramesInVideo) + 1) );
	        
	        int seconds;
	        seconds = 0;
	        
	        cap.set( Videoio.CAP_PROP_POS_FRAMES, framesPerSecond*seconds );
	        
	        frameNumber = 0;
	        while( cap.read( frame ) ) {
	        	frameName = String.format( formater, output, File.separator, frameNumber );
	        	frameNameGray = String.format( formaterGray, output, File.separator, frameNumber );
	        	
	        	Imgproc.cvtColor( frame, frameGray, Imgproc.COLOR_RGB2GRAY );

	        	Imgcodecs.imwrite( frameName, frame );
	        	Imgcodecs.imwrite( frameNameGray, frameGray );
	            ++frameNumber;
	            
	            ++seconds;
	            
	            cap.set( Videoio.CAP_PROP_POS_FRAMES, framesPerSecond*seconds );
	        }
	        
	        cap.release();

	        System.out.printf( "%d frames extracted\n", frameNumber);
	    }
	    else {
	    	System.out.println( "Failed to open input video." );
	    }
	}
}
