/** IndexWriter initializes a collection of BufferedIndexFileWriter instances,
 * and provides a single interface to adding entries to the index.
 * @author rkallos
 */

package input_output;

import java.io.FileNotFoundException;
import java.io.IOException;

public class IndexWriter {
	
	/** Represents the number of index files to create */
	private static final int BUCKETS = 81;
	/** Represents the starting number for the index file names */
	private static final int min = 18;
	
	/**
	 * Structure for holding BufferedIndexFileWriters.
	 * The current implementation uses a simple array,
	 * but it could be easily adapted to use a different structure.
	 */
	private BufferedIndexFileWriter[] writers;
	
	/** 
	 * Default constructor. Initializes writers. 
	 * @throws FileNotFoundException */
	public IndexWriter() throws FileNotFoundException {
		// Initialize file writers
		writers = new BufferedIndexFileWriter[BUCKETS + 1];
		for (int i = 0; i < writers.length; ++i) {
			writers[i] = new BufferedIndexFileWriter("./resources/index/" + Integer.toString(i + min));
		}
	}
	
	/**
	 * Look for specific bucket
	 */
	private BufferedIndexFileWriter lookup(short age) {
		return writers[age-min];
	}
	
	public void addEntry(short age, long offset) {
		lookup(age).addEntry(offset);
	}
	
	/**
	 * Close IndexWriter, flushing all BufferedIndexFileWriters.
	 * 
	 * @throws IOException 
	 */
	public void close() throws IOException {
		for(int i = 0; i < writers.length; ++i) {
			writers[i].close();
		}
	}
	
	/**
	 * Return number of I/O operations done while writing the indexes
	 */
	public int getWrites() {
		int sum = 0;
		for(int i = 0; i < writers.length; ++i) {
			sum += writers[i].getWrites();
		}
		return sum;
	}
}
