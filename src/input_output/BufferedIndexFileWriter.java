/** Handles the adding of index entries to a particular index file.
 * It minimizes writes to disk by buffering output to the file, and only
 * writing to a file when the buffer becomes full.
 * 
 * @author rkallos
 */

package input_output;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class BufferedIndexFileWriter {
	
	/** The size of a hard disk block. Measured in bytes. */
	private static final short BLOCK_SIZE = 4096;
	/** The size of an index entry. Measured in bytes. */
	private static final short ENTRY_SIZE = 5;
	/** The number of full entries that fit inside one block. */
	private static final short threshold = BLOCK_SIZE / ENTRY_SIZE;
	
	/** The number of index entries currently in the buffer. */
	private short size = 0;
	/** The buffer, represented as an array of bytes. */
	private ByteBuffer buffer;
	/** ByteBuffer for taking only 5/8 bytes for a long value */
	private ByteBuffer longConverter;
	/** The output stream for the index file. */
	private FileOutputStream fout;
	private FileChannel fc;
	/** The number of I/O writes performed */
	private int writes;
	
	/** 
	 * Constructor: Tries to open/create an index file, and initialize the buffer.
	 * 
	 * @param filename
	 * @throws FileNotFoundException 
	 */
	public BufferedIndexFileWriter(String filename) throws FileNotFoundException {
		fout = new FileOutputStream(filename);
		fc = fout.getChannel();
		buffer = ByteBuffer.allocateDirect(BLOCK_SIZE);
		longConverter = ByteBuffer.allocateDirect(8);
	}
	
	public void addEntry(long offset) {
		longConverter.putLong(offset);
		longConverter.position(3);
		buffer.put(longConverter);
		longConverter.clear();
		++size;
		if (size == threshold) {
			write();
		}
	}
	
	/**
	 * Flush the buffer to file.
	 */
	private void write() {
		try {
			buffer.flip();
			fc.write(buffer);
			buffer.clear();
			size = 0;
			++writes;
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public void close() throws IOException {
		buffer.flip();
		fc.write(buffer);
		fout.close();
		++writes;
	}
	
	/**
	 * Return number of times written to index file
	 */
	public int getWrites() {
		return writes;
	}
}
