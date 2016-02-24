/** Handles the adding of index entries to a particular index file.
 * It minimizes writes to disk by buffering output to the file, and only
 * writing to a file when the buffer becomes full.
 * 
 * @author rkallos
 */

// TODO: Implement counting number of I/Os.

package input_output;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

// See if it's possible to use AspectJ for this:
// - Use aspect to check argument lengths?
// - Use aspect to call write()?
// - Use aspect to check for writing to end-of-block padding?

public class BufferedIndexFileWriter {
	
	/** The size of a hard disk block. Measured in bytes. */
	private static final short blockSize = 4096;
	/** The size of an index entry. Measured in bytes. */
	private static final short entrySize = 5;
	/** The number of full entries that fit inside one block. */
	private static final short threshold = blockSize / entrySize;
	/** The number of bytes to pad at the end of a block. */
	private static final short padding = blockSize - (threshold * entrySize);
	
	/** The number of index entries currently in the buffer. */
	private short size = 0;
	/** The buffer, represented as an array of bytes. */
	private byte[] buffer;
	/** The output stream for the index file. */
	private FileOutputStream fout;
	
	/** 
	 * Constructor: Tries to open/create an index file, and initialize the buffer.
	 * 
	 * @param filename
	 * @throws FileNotFoundException 
	 */
	public BufferedIndexFileWriter(String filename) throws FileNotFoundException {
		fout = new FileOutputStream(filename);
		buffer = new byte[blockSize];
	}
	
	/** 
	 * Add a single entry to the index file, flushing the buffer to fout when it becomes full.
	 * 
	 * @param entry
	 * @throws WrongEntrySizeException
	 */
	public void addEntry(byte[] entry) throws WrongEntrySizeException {
		if (entry.length > entrySize) {
			throw(new WrongEntrySizeException());
		}
		for(int i = 0; i < entrySize; ++i) {
			buffer[(size*entrySize)+i] = entry[i];
		}
		++size;
		if (size == threshold) {
			write();
		}
	}
	
	/** 
	 * Add multiple entries to the index file, flushing the buffer to fout when it becomes full.
	 * 
	 * @param entries
	 * @throws WrongEntrySizeException
	 */
	public void addEntries(byte[] entries) throws WrongEntrySizeException {
		if (entries.length % entrySize != 0) {
			throw(new WrongEntrySizeException());
		}
		for (int i = 0; i < entries.length; i = i + 5) {
			for (int j = 0; j < entrySize; ++j) {
				buffer[(size*entrySize)+j] = entries[i + j];
			}
			++size;
			if (size == threshold) {
				write();
			}
		}
	}
	
	/**
	 * Pad the end of buffer with null bytes, then flush the buffer to file.
	 */
	private void write() {
		try {
			for (int i = buffer.length - padding - 1; i < blockSize; ++i) {
				// Pad end of block with null byte
				buffer[i] = 0;
			}
			fout.write(buffer);
			size = 0;
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
