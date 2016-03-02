package lookup;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import input_output.IOFile;

/**
 * @author Julian
 *
 */
public class LookupManager {
	public static final String INDEX_PATH = "./resources/index/";
	public static final String RELATION_PATH = "./resources/relation/person.txt";
	public static final String HITS_PATH = "./resources/output/hits.txt";
	private static int outputWrites = 0;

	// private static HashMap<Short, IndexEntry> index = new HashMap<>();
	RandomAccessFile raf;
	FileOutputStream fos;
	FileChannel fc;
	ByteBuffer hitsBuffer;

	public void openOutput() throws FileNotFoundException {
		fos = new FileOutputStream(HITS_PATH);
		// raf = new RandomAccessFile(HITS_PATH, "rw");
		fc = fos.getChannel();

		hitsBuffer = ByteBuffer.allocate(IOFile.BLOCK_SIZE);
	}

	public static void main(String[] args) throws IOException {
		// index.put((short) 18, new IndexEntry("18", 2));
		(new LookupManager()).lookupHits((short) 99);
	}

	public void lookupHits(Short age) throws IOException {
		openOutput();

		// IndexEntry entry = index.get(age);
		//
		// long count = entry.getCount();
		// System.out.println("Number of hits: " + count);
		//
		// String bucketName = entry.getBucketName();
		String bucketName = age.toString();

		long bucketSize = 0;
		long indexReads = 0;
		long relationReads = 0;
		try {
			IOFile bucketFile = new IOFile(INDEX_PATH + bucketName);
			IOFile relationFile = new IOFile(RELATION_PATH);

			bucketSize = bucketFile.length();

			int indexOffset = 0;
			ByteBuffer indexBlock;
			ByteBuffer dataBlock = null;
			ByteBuffer indexEntry;
			byte[] indexBytes = new byte[] { 0, 0, 0, 0, 0, 0, 0, 0 };
			byte[] record = new byte[100];
			long currentBlockStart;
			int blockOffset;
			long offset;
			long byteOffset;
			while (indexOffset < bucketSize) {
				indexBlock = bucketFile.readSequentialBlock();
				// System.out.println("Index block position: " +
				// indexBlock.position());
				indexOffset += IOFile.BLOCK_SIZE;

				currentBlockStart = 0;
				blockOffset = 0;
				while (indexBlock.hasRemaining()) {
					indexBlock.get(indexBytes, 3, 5);
					indexEntry = ByteBuffer.wrap(indexBytes);
					offset = indexEntry.getLong();

					byteOffset = offset * 100;

					// System.out.println("Index block position: " +
					// indexBlock.position());

					blockOffset = (int) (byteOffset - currentBlockStart);

					if (currentBlockStart == 0 || blockOffset >= 4000) {
						dataBlock = relationFile.readRandomBlock(byteOffset);
						currentBlockStart = byteOffset;
						blockOffset = 0;
					}

					// System.out.println("Data block position: " +
					// dataBlock.position());

					dataBlock.position(blockOffset);
					// System.out.println("Data block position: " +
					// dataBlock.position());
					dataBlock.get(record);
					// System.out.println("Data block position: " +
					// dataBlock.position());

					hitsBuffer.put(record);

					if (hitsBuffer.capacity() - hitsBuffer.position() < 100 || !indexBlock.hasRemaining()) {
						hitsBuffer.flip();
						while (hitsBuffer.hasRemaining()) {
							// System.out.println("Hit file position: " +
							// fc.position());
							int write = fc.write(hitsBuffer);
							outputWrites++;
							// System.out.println("Hit file position: " +
							// fc.position());
						}
						hitsBuffer.clear();
					}
				}
			}
			closeOutput();
			indexReads = bucketFile.getReads();
			relationReads = relationFile.getReads();
		} catch (FileNotFoundException e) {
		} finally {
			System.out.println("Number of hits:       " + bucketSize / 5);
			System.out.println("Index block reads:    " + indexReads);
			System.out.println("Relation block reads: " + relationReads);
			System.out.println("Output block writes:  " + outputWrites);
		}
	}

	// public int computeRelationOffset(long offset) {
	// int relationOffset = (int) ((offset * 100) / IOFile.BLOCK_SIZE);
	// short blockOffset = (short) ((offset * 100) % IOFile.BLOCK_SIZE);
	// if (blockOffset == 40){
	// relationOffset++;
	// }
	// return relationOffset;
	// }
	//
	// public short computeBlockOffset(long offset) {
	// short blockOffset = (short) ((offset * 100) % IOFile.BLOCK_SIZE);
	// return blockOffset;
	// }

	public void closeOutput() throws IOException {
		// raf.close();
		fos.close();
		fc.close();
	}
}
