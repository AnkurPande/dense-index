/**
 * @author Ankurp 
 * 
 */

package indexing;

import input_output.IOFile;

import java.io.IOException;

public class CreateIndex {
	
	static String path = "C:\\Users\\Ankurp\\DENSE-INDEX\\";
	static String FILE_NAME = path+"person.txt";
	static int  NO_OF_TOUPLES = 10000;
	
	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		new CreateIndex().runCases();
	}

	private void runCases() throws IOException{
		String[] tempStringArr = new String[40];
		IOFile f = new IOFile(FILE_NAME);
		//Calculate available memory at start.
		float startmem = Runtime.getRuntime().freeMemory();
		
		//start time
		long startTime = System.currentTimeMillis();	
		
		//Calculate number of runs required
		int runs = (int) Math.ceil((double)NO_OF_TOUPLES/(40));
		
		for (int i = 1; i <= runs; i++) {
			if (i == runs) {
				if (NO_OF_TOUPLES % (40) != 0)
					tempStringArr = new String[NO_OF_TOUPLES % 40];
				else
					tempStringArr = new String[40];
			} else {
				tempStringArr = new String[40];
			}
			
			for (int j = 0; j < tempStringArr.length; j++) {
				tempStringArr[j] = f.readTupleFromFile(FILE_NAME);
			}
			f.writeToFile(path+"\\data" + i + ".txt", tempStringArr);
			
		}
		
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
