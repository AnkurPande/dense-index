/**
 * @author Ankurp, Julian
 * 
 */

package input_output;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class IOFile {
	public static final int BLOCK_SIZE = 4096;
	public static final int RECORD_SIZE = 100;
	public static final int AGE_OFFSET = 39;

	private int reads = 0;
	private int writes = 0;

	// FileInputStream fInStream;
	RandomAccessFile raf;
	FileChannel fc;
	ByteBuffer block;

	public IOFile(String filename) throws FileNotFoundException {
		raf = new RandomAccessFile(filename, "r");
		fc = raf.getChannel();
		 block = ByteBuffer.allocate(BLOCK_SIZE);
	}

	public ByteBuffer readSequentialBlock() throws IOException {
		block.clear();
		fc.read(block);
		block.flip();
		++reads;
		return block;
	}

	public ByteBuffer readRandomBlock(long position) throws IOException {
		fc.position(position);
		block.clear();
		fc.read(block);
		block.flip();
		++reads;
		return block;
	}

	/**
	 * @author Richard
	 */
	public int getReads() {
		return reads;
	}

	public int getWrites() {
		return writes;
	}
	
	public long length() throws IOException {
		return fc.size();
	}

	@Override
	protected void finalize() throws Throwable {
		fc.close();
		raf.close();
	}
}
