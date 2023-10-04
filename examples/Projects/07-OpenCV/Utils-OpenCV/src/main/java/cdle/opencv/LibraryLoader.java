package cdle.opencv;

public class LibraryLoader {
	
	public static void loadLibrary(String libraryName) {
		try {
			System.out.printf( "Trying to load library %s\n", libraryName);
			System.loadLibrary( libraryName );
		}
		catch (Throwable t) {
			System.out.printf( "Error loading library %s\n", libraryName );
			System.out.printf( "Details:\n%s\n", t.getMessage()  );
		}
	}
	
	public static void loadLibraries(String[] linuxLibrariesNames, String[] librariesNames) {
		System.out.println( "java.library.path: " + System.getProperty( "java.library.path" ) );
		
		for (String libraryName : linuxLibrariesNames) {
			if ( OSValidator.isUnix() ) {
				loadLibrary( libraryName );
			}
		}
		
		for (String libraryName : librariesNames) {
			loadLibrary( libraryName );
		}
	}

}
