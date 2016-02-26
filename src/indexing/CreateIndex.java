/**
 * @author Ankurp 
 * 
 */

package indexing;

import input_output.IOFile;
import input_output.IndexWriter;
import input_output.WrongRecordOffsetSizeException;

import java.io.IOException;
import java.nio.ByteBuffer;

public class CreateIndex {
	
	//static String path = "C:\\Users\\Ankurp\\DENSE-INDEX\\";
	static String path = "./";
	static String FILE_NAME = path+"person.txt";
	static int  NO_OF_TOUPLES = 10000;
	IndexWriter writer ;
	
	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		new CreateIndex().runCases();
	}

	private void runCases() throws IOException{
		IOFile f = new IOFile(FILE_NAME);
		writer = new IndexWriter();
		//Calculate available memory at start.
		float startmem = Runtime.getRuntime().freeMemory();
		
		//start time
		long startTime = System.currentTimeMillis();
		
		//Calculate number of runs required
		int runs = (int) Math.ceil((double)NO_OF_TOUPLES/(40));
		
		byte[] tuple = new byte[100];
		ByteBuffer block = ByteBuffer.allocate(4096 + tuple.length);
		
		for (int i = 0; i <= runs; i++) {
			block.put(f.readBlock(i));
			block.flip();
			for (short j = 0; j < block.limit() / tuple.length; j++) {
				if (block.remaining() < tuple.length) {
					// Fewer than 100 bytes remaining in buffer. Read next block.
					break;
				}
				
				// Read a tuple from block.
				block.get(tuple, 0, tuple.length);
				
				//Converting values of age attributes and block sequence into 5 bytes offset.
				String age =  new String(tuple, 39, 2);
				short ageVal = Short.parseShort(age);
			
				//Block offset within file
				int blockOffset = i;
			
				//Record offset within block
				short recordOffset = j;
		
				//Add entry to index file.
				try {
					writer.addEntry(ageVal, blockOffset, recordOffset);
				} catch (WrongRecordOffsetSizeException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			// Move remaining bytes to beginning of buffer
			block.compact();
		}
		
		writer.close();
		//Calculate the end time
		long endTime = System.currentTimeMillis();
		
		//Calculate the end memory.
		long endmem = Runtime.getRuntime().totalMemory();
		
		//Print performance.
		System.out.println("Time Taken : " + (endTime - startTime) + "ms");
		System.out.println("Memory Taken (in bytes): " + (endmem - startmem) + " bytes");
		System.out.println("Memory Taken (in MB): " + (double) (endmem - startmem) /(1024*1024) + " MB");
		System.out.println("Number of I/Os (creating index): " + f.getReads() + " reads, " + writer.getWrites() + " writes");
		}	
}
