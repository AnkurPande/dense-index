/** IndexWriter initializes a collection of BufferedIndexFileWriter instances,
 * and provides a single interface to adding entries to the index.
 * @author rkallos
 */

package input_output;

import java.io.FileNotFoundException;
import java.io.IOException;

public class IndexWriter {
	
	/** Represents the number of index files to create */
	private static final int buckets = 81;
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
		writers = new BufferedIndexFileWriter[buckets];
		for (int i = 0; i < writers.length; ++i) {
			writers[i] = new BufferedIndexFileWriter("./index/" + Integer.toString(i + min));
		}
	}
	
	/**
	 * Look for specific bucket
	 */
	private BufferedIndexFileWriter lookup(short age) {
		return writers[age-min];
	}
	
	/** 
	 * Add an index entry to the index for age.
	 * 
	 * @param age
	 * @param entry
	 * @throws WrongEntrySizeException
	 */
	public void addEntry(short age, byte[] entry) throws WrongEntrySizeException {
		lookup(age).addEntry(entry);
	}
	
	public void addEntry(short age, int blockOffset, short recordOffset) throws WrongRecordOffsetSizeException {
		lookup(age).addEntry(blockOffset, recordOffset);
	}
	
	/** 
	 * Add multiple index entries to the index for age.
	 * 
	 * @param age
	 * @param entries
	 * @throws WrongEntrySizeException
	 */
	public void addEntries(short age, byte[] entries) throws WrongEntrySizeException {
		lookup(age).addEntries(entries);
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
