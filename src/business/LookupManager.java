package business;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.HashMap;

import input_output.IOFile;
import model.IndexEntry;

/**
 * @author Julian
 *
 */
public class LookupManager {
	public static final String INDEX_PATH = "C:\\Users\\Julian\\dev\\git\\dense-index\\resources\\index\\";
	public static final String RELATION_PATH = "C:\\Users\\Julian\\dev\\git\\dense-index\\resources\\relation\\person.txt";
	public static final String HITS_PATH = "C:\\Users\\Julian\\dev\\git\\dense-index\\resources\\output\\hits.txt";

	private static HashMap<Short, IndexEntry> index = new HashMap<>();
	RandomAccessFile raf;
	FileInputStream fis;
	FileChannel fc;
	ByteBuffer hitsBuffer;

	public LookupManager() throws FileNotFoundException {
		// fis = new FileInputStream(HITS_PATH);
		raf = new RandomAccessFile(HITS_PATH, "rw");
		fc = raf.getChannel();
		hitsBuffer = ByteBuffer.allocate(IOFile.BLOCK_SIZE);
	}

	public static void main(String[] args) throws IOException {
		index.put((short) 25, new IndexEntry("bucket25", 2));
		(new LookupManager()).lookupHits((short) 25);
	}

	public void lookupHits(Short age) throws IOException {
		IndexEntry entry = index.get(age);

		long count = entry.getCount();
		long bucketSize = count * 5;
		System.out.println("Number of hits: " + count);

		String bucketName = entry.getBucketName();

		IOFile bucketFile = new IOFile(INDEX_PATH + bucketName);
		IOFile relationFile = new IOFile(RELATION_PATH);

		int indexOffset = 0;
		ByteBuffer indexBlock;
		ByteBuffer dataBlock = null;
		while (indexOffset < bucketSize) {
			indexBlock = bucketFile.readSequentialBlock();
			System.out.println("Index block position: " + indexBlock.position());
			indexOffset += IOFile.BLOCK_SIZE;
			// System.out.println("Index block: " + new String(indexBlock));

			// int dataStart;
			byte[] record = new byte[100];
			long currentBlockStart = -1;
			while (indexBlock.hasRemaining()) {
				// int blockOffset = indexBlock.getInt();
				char[] ca = new char[5];
				ca[0] = (char) indexBlock.get();
				ca[1] = (char) indexBlock.get();
				ca[2] = (char) indexBlock.get();
				ca[3] = (char) indexBlock.get();
				ca[4] = (char) indexBlock.get();
				long offset = new Long(new String(ca));
				
				
				
				
				long byteOffset = offset * 100;
				short blockOffset = (short) (byteOffset - currentBlockStart);
				

				

//				int blockNumber = computeRelationOffset(offset);
//				short blockOffset = computeBlockOffset(offset);

				System.out.println("Index block position: " + indexBlock.position());
//				if (blockNumber != currentBlockStart) {
				if (blockOffset >= 4000) {
					dataBlock = relationFile.readRandomBlock(byteOffset);
					System.out.println("Data block position: " + dataBlock.position());
					currentBlockStart = byteOffset;
					blockOffset = 0;
				}

				// int recordOffset = Character.getNumericValue((char)
				// indexBlock.get());
//				System.out.println("Index block position: " + indexBlock.position());
				// recordOffset *= 100;
				// dataStart += recordOffset;
				dataBlock.position(blockOffset);
				System.out.println("Data block position: " + dataBlock.position());
				dataBlock.get(record);
				System.out.println("Data block position: " + dataBlock.position());
//				System.out.println("Hit: " + record);

				hitsBuffer.put(record);

				if (hitsBuffer.capacity() - hitsBuffer.position() < 100 || !indexBlock.hasRemaining()) {
					hitsBuffer.flip();
					while (hitsBuffer.hasRemaining()) {
						fc.write(hitsBuffer);
					}
					hitsBuffer.flip();
					hitsBuffer.clear();
				}
			}
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

	@Override
	protected void finalize() throws Throwable {
		raf.close();
		fis.close();
		fc.close();
	}
}
