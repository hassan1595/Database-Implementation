package de.tuberlin.dima.minidb.util;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class InputStreamFileReader {
	
	/**
	 * Helper method to convert an InputStream to a file
	 * Useful to read a file from a jar
	 * @param is the input stream to read from
	 * @param fileLocation the path of the file to create
	 * @return the converted file
	 */
	public static File inputStreamToFile(Class<?> clazz, String fileLocation) {
		InputStream is = clazz.getResourceAsStream(fileLocation);
		if(is == null){
			System.err.println("Cannot read resource at: " + fileLocation);
			return null;
		}
	
		File temp = null;
		try {
			temp = File.createTempFile(fileLocation, null);
			OutputStream os = new FileOutputStream(temp);
			byte[] buffer = new byte[1024];
			int bytesRead;
			while((bytesRead = is.read(buffer)) != -1){
				os.write(buffer,0,bytesRead);
			}
			temp.deleteOnExit();
			is.close();
			os.flush();
			os.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return temp;
	}

}
