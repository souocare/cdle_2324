package cdle.opencv;

import java.io.File;

import javax.swing.JFileChooser;

public class MyExtractorLaunch {
	private static final JFileChooser chooser = new JFileChooser( "." );
	
	private static File getFile(boolean onlyDirectories, String tile) {
		if ( onlyDirectories==true ) {
			chooser.setFileSelectionMode( JFileChooser.DIRECTORIES_ONLY );
		}
		else {
			chooser.setFileSelectionMode( JFileChooser.FILES_AND_DIRECTORIES );
		}
		
		chooser.setDialogTitle( tile );
		
		if ( chooser.showOpenDialog( null )==JFileChooser.APPROVE_OPTION ) {
			return chooser.getSelectedFile();
		}
		
		return null;
	}

	public static void main(String[] args) {
		File inputFile, outputDirectory;
		
		inputFile = ( args.length>=1) ? new File( args[0] ) : getFile( false, "Input video" );
		outputDirectory = ( args.length>=2) ? new File( args[1] ) : getFile( true, "Output directory" );
		
		if ( inputFile!=null && outputDirectory!=null ) {
			MyExtractor extractor;
			extractor = new MyExtractor( inputFile, outputDirectory );
			extractor.run();
		}
	}
}
