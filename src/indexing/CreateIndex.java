/**
 * @author Ankurp 
 * 
 */

package indexing;

import input_output.IOFile;
import input_output.IndexWriter;
import input_output.WrongRecordOffsetSizeException;

import java.io.IOException;

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
		String[] tempStringArr = new String[40];
		IOFile f = new IOFile(FILE_NAME);
		writer = new IndexWriter();
		//Calculate available memory at start.
		float startmem = Runtime.getRuntime().freeMemory();
		
		//start time
		long startTime = System.currentTimeMillis();	
		
		//Calculate number of runs required
		int runs = (int) Math.ceil((double)NO_OF_TOUPLES/(40));
		
		for (int i = 0; i <= runs; i++) {
			if (i == runs) {
				if (NO_OF_TOUPLES % (40) != 0)
					tempStringArr = new String[NO_OF_TOUPLES % 40];
				else
					tempStringArr = new String[40];
			} else {
				tempStringArr = new String[40];
			}
			
			for (short j = 0; j < tempStringArr.length; j++) {
				tempStringArr[j] = f.readTupleFromFile(FILE_NAME);
				
				//Converting values of age attributes and block sequence into 5 bytes offset.
				String age =  tempStringArr[j].substring(39, 41);
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
		}
		
		writer.close();
		//Calculate the end time
		long endTime = System.currentTimeMillis();
		
		tempStringArr = null;
		
		//Calculate the end memory.
		long endmem = Runtime.getRuntime().totalMemory();
		
		//Print performance.
		System.out.println("Time Taken : " + (endTime - startTime) + "ms");
		System.out.println("Memory Taken (in bytes): " + (endmem - startmem) + " bytes");
		System.out.println("Memory Taken (in MB): " + (double) (endmem - startmem) /(1024*1024) + " MB");
		
		}	
}
