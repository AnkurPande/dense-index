/**
 * @author Ankurp 
 * 
 */


package input_output;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;

public class IOFile {

	FileInputStream fInStream;
	DataInputStream in;
	BufferedReader br;
	
	public IOFile(String filename) throws FileNotFoundException{
		fInStream = new FileInputStream(filename);
		in = new DataInputStream(fInStream);
		br = new BufferedReader(new InputStreamReader(in));
	}
	
	public String readLineFromFile(String filename) {
		try {
			String strline;
			while ((strline = br.readLine()) != null) {
					return strline;
			}
			in.close();
		} catch (Exception e) {
			System.out.println("ERROR :" + e.getMessage());
		}
		return "";

	}
	
	public String readTupleFromFile(String filename) throws IOException{
		char[] chars = new char[100];
	    int offset = 0;
	    while (offset < 100) {
	        int charsRead = br.read(chars, offset, 100 - offset);
	        if (charsRead <= 0) {
	            throw new IOException("Stream terminated early");
	        }
	        offset += charsRead;
	    }
	    return new String(chars);
		
	}
	
	
	public void writeToFile(String filename,String[] linesToWrite) throws FileNotFoundException, UnsupportedEncodingException{
		PrintWriter writer = new PrintWriter(filename, "UTF-8");
		for (String str : linesToWrite) {
			writer.println(str);			
		}
		writer.close();
	}
	
	

}
