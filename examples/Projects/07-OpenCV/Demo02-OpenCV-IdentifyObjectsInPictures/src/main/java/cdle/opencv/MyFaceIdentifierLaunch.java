package cdle.opencv;

import java.io.File;

import javax.swing.JFileChooser;

public class MyFaceIdentifierLaunch {
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
		File inputDirectory, outputDirectory, classfier;
		
		inputDirectory = ( args.length>=1) ? new File( args[0] ) : getFile( true, "Input directory" );
		outputDirectory = ( args.length>=2) ? new File( args[1] ) : getFile( true, "Output directory" );
		classfier = ( args.length>=3) ? new File( args[2] ) : getFile( false, "Classifier" );
		
		if ( inputDirectory!=null && outputDirectory!=null && classfier!=null ) {
			MyFaceIdentifier identifier;
			identifier = new MyFaceIdentifier( inputDirectory, outputDirectory, classfier );
			identifier.run();
		}
		
		
	}
}
