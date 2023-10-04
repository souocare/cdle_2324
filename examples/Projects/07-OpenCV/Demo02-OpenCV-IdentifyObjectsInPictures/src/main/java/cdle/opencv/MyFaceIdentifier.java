package cdle.opencv;

import static org.opencv.imgproc.Imgproc.rectangle;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;

import org.opencv.core.Core;
import org.opencv.core.Mat;
import org.opencv.core.MatOfRect;
import org.opencv.core.Point;
import org.opencv.core.Rect;
import org.opencv.core.Scalar;
import org.opencv.imgcodecs.Imgcodecs;
import org.opencv.imgproc.Imgproc;
import org.opencv.objdetect.CascadeClassifier;

public class MyFaceIdentifier 
		implements Runnable {
	
	private File inputDirectory;
	private File outputDirectory;
	private File classifier;
	
	public MyFaceIdentifier(File inputDirectory, File outputDirectory, File classifier) {
		try {
			this.inputDirectory = new File( inputDirectory.getCanonicalPath() );
			this.outputDirectory = new File( outputDirectory.getCanonicalPath() );
			this.classifier = new File( classifier.getCanonicalPath() );
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
	
	public void run() {
		String input, output;
		input = inputDirectory.getAbsoluteFile().toString();
		output = outputDirectory.getAbsoluteFile().toString();
			
		System.out.printf( "Processing %s\n", input );
		System.out.printf( "Output directory: %s\n", output );
			
		outputDirectory.mkdirs();
		
		CascadeClassifier faceDetector ;
		faceDetector  = new CascadeClassifier( this.classifier.getAbsolutePath() );
		
		FilenameFilter nameFilter;
		nameFilter = new FilenameFilter() {	
			@Override
			public boolean accept(File dir, String name) {
				if ( name.endsWith( "jpg") || name.endsWith( "jpeg") ) {
					return true;
				}

				return false;
			}
		};
		
		File[] images;
		images = inputDirectory.listFiles( nameFilter );
		
		Mat currentImageAsGray;
		currentImageAsGray = new Mat();
		
		for (File currentImageFile : images) {
			String currentImageFileName;
			currentImageFileName = currentImageFile.getAbsolutePath();
			
			System.out.printf( "Processing image %s\n", currentImageFileName );
			
			Mat currentInputImage, currentOutputImage;
			currentInputImage = Imgcodecs.imread( currentImageFileName );
			
			Imgproc.cvtColor( currentInputImage, currentImageAsGray, Imgproc.COLOR_RGB2GRAY );
			Imgproc.equalizeHist(currentImageAsGray, currentImageAsGray);
			
			MatOfRect faceDetections;
			faceDetections = new MatOfRect();
		    faceDetector.detectMultiScale( currentImageAsGray, faceDetections );
		    
		    System.out.printf( "Detected %s faces\n\n", faceDetections.toArray().length );
		    
		    File currentOutputImageFile;
		    currentOutputImageFile = new File( outputDirectory, currentImageFile.getName() );
		    
		    String currentOutputImageFileName;
		    currentOutputImageFileName = currentOutputImageFile.getAbsolutePath();
		    
		    currentOutputImage = currentInputImage.clone();
		    		    
		    for (Rect rect : faceDetections.toArray()) {
		        rectangle(
		        		currentOutputImage, 
		        		new Point(rect.x, rect.y), 
		        		new Point(rect.x + rect.width, rect.y + rect.height), 
		        		new Scalar(255, 255, 0),
		        		3);
		    }
			
		    Imgcodecs.imwrite(currentOutputImageFileName, currentOutputImage);
		}
	}
}
