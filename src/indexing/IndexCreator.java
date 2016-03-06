/**
 * @author Ankurp
 * 
 */

package indexing;

import input_output.IOFile;
import input_output.IndexWriter;
import model.Performance;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;

public class IndexCreator {

	// Constants for relation
	static final int BLOCK_SIZE = IOFile.BLOCK_SIZE;
	static final int RECORD_SIZE = IOFile.RECORD_SIZE;
	static final int AGE_OFFSET = IOFile.AGE_OFFSET;

	// static String path = "C:\\Users\\Ankurp\\DENSE-INDEX\\";
	static String path = "./resources/relation/";
	public static final String FILE_NAME = path + "person_fnl.txt";
	IndexWriter writer;
	IOFile f;

	public static void main(String[] args) throws FileNotFoundException, IOException {
		IndexCreator c = new IndexCreator();
		Performance perf = new Performance();

		perf.startTimer();
		c.runCases();
		perf.stopTimer();
		perf.calculateMemUsed();
		System.out.println("\nPerformance data for index creation.");
		System.out.println("Time Taken :          " + (double) perf.getTimeElapsed() / 1000 + " s");
		System.out.println("Memory Taken (in MB): " + (double) perf.getUsedMemory() / (1024 * 1024) + " MB");
	}

	public IndexCreator() throws FileNotFoundException {
		f = new IOFile(FILE_NAME);
		writer = new IndexWriter();
	}

	public void runCases() throws IOException {

		// Calculate number of runs required
		int recordsPerBlock = BLOCK_SIZE / RECORD_SIZE;
		long fileSize = f.length();
		long recordsInRelation = fileSize / RECORD_SIZE;
		int runs = (int) Math.ceil((double) recordsInRelation / recordsPerBlock);

		byte[] tuple = new byte[2];
		ByteBuffer block = ByteBuffer.allocate(BLOCK_SIZE + RECORD_SIZE);

		int recordOffset = 0;
		for (int i = 0; i <= runs; i++) {
			block.put(f.readSequentialBlock());
			block.flip();
			while (block.remaining() >= RECORD_SIZE) {
				block.position(block.position() + AGE_OFFSET);
				// Read a tuple from block.
				block.get(tuple, 0, 2);

				// Converting values of age attributes and block sequence into 5
				// bytes offset.
				String age = new String(tuple);
				short ageVal = Short.parseShort(age);

				// Add entry to index file.
				writer.addEntry(ageVal, recordOffset);
				++recordOffset;
				block.position((block.position() / RECORD_SIZE) * RECORD_SIZE + RECORD_SIZE);
			}
			// Move remaining bytes to beginning of buffer
			block.compact();
		}

		writer.close();
		System.out.println("Number of I/Os (creating index): " + f.getReads() + " reads, " + writer.getWrites() + " writes");
	}
}
