/**
 * @author Ankurp 
 * 
 */

package input_output;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class IOFile {
	public static final int BLOCK_SIZE = 4096;
	
	private static int reads = 0;
	private static int writes = 0;

	FileInputStream fInStream;
	FileChannel fc;

	public IOFile(String filename) throws FileNotFoundException {
		fInStream = new FileInputStream(filename);
		fc = fInStream.getChannel();
	}

//	public String readLineFromFile(String filename) {
//		try {
//			String strline;
//			while ((strline = br.readLine()) != null) {
//				return strline;
//			}
//			in.close();
//		} catch (Exception e) {
//			System.out.println("ERROR :" + e.getMessage());
//		}
//		return "";
//
//	}

//	public String readTupleFromFile(String filename) throws IOException {
//		char[] chars = new char[100];
//		int offset = 0;
//		while (offset < 100) {
//			int charsRead = fc.read(chars, offset, 100 - offset);
//			if (charsRead <= 0) {
//				throw new IOException("Stream terminated early");
//			}
//			offset += charsRead;
//		}
//		return new String(chars);
//
//	}

	public void writeToFile(String filename, String[] linesToWrite)
			throws FileNotFoundException, UnsupportedEncodingException {
		PrintWriter writer = new PrintWriter(filename, "UTF-8");
		for (String str : linesToWrite) {
			writer.println(str);
		}
		writer.close();
	}

	/**
	 * @author Julian
	 */
	public ByteBuffer readBlock(int offset) throws IOException {
		ByteBuffer block = ByteBuffer.allocate(BLOCK_SIZE);
		fc.read(block);
		block.flip();
		++reads;
		return block;
	}
	
	/**
	 * @author Richard
	 * @throws IOException 
	 */

	public int getReads() {
		return reads;
	}
	
	public int getWrites() {
		return writes;
	}
}
