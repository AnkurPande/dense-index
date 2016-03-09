package lookup;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;

import indexing.IndexCreator;
import input_output.IOFile;

/**
 * @author Julian
 *
 */
public class LookupManager {
	public static final String INDEX_PATH = "./resources/index/";
	public static final String HITS_PATH = "./resources/output/hits.txt";

	private int outputWrites;
	private FileOutputStream fos;
	private FileChannel fc;
	private ByteBuffer hitsBuffer;

	public void openOutput() throws FileNotFoundException {
		fos = new FileOutputStream(HITS_PATH);
		fc = fos.getChannel();
		hitsBuffer = ByteBuffer.allocate(IOFile.BLOCK_SIZE);
		outputWrites = 0;
	}

	public void lookupHits(Short age) throws IOException {
		openOutput();

		String bucketName = age.toString();

		long indexReads = 0;
		long relationReads = 0;
		long hitCount = 0;
		long salarySum = 0;
		try {
			IOFile bucketFile = new IOFile(INDEX_PATH + bucketName);
			IOFile relationFile = new IOFile(IndexCreator.FILE_NAME);

			long bucketSize = bucketFile.length();
			hitCount = bucketSize / 4;
			System.out.println("Number of hits:       " + hitCount);

			int indexOffset = 0;
			ByteBuffer indexBlock;
			ByteBuffer dataBlock = null;
			byte[] record = new byte[100];
			long currentBlockStart;
			int blockOffset;
			long offset;
			long byteOffset;
			byte[] salaryBytes;
			long salary;
			while (indexOffset < bucketSize) {
				indexBlock = bucketFile.readSequentialBlock();
				indexOffset += IOFile.BLOCK_SIZE;

				currentBlockStart = 0;
				blockOffset = 0;
				while (indexBlock.hasRemaining()) {
					offset = indexBlock.getInt();
					byteOffset = offset * 100;
					blockOffset = (int) (byteOffset - currentBlockStart);

					if (currentBlockStart == 0 || blockOffset >= (IOFile.BLOCK_SIZE / IOFile.RECORD_SIZE) * IOFile.RECORD_SIZE) {
						dataBlock = relationFile.readRandomBlock(byteOffset);
						currentBlockStart = byteOffset;
						blockOffset = 0;
					}

					dataBlock.position(blockOffset);
					dataBlock.get(record);

					salaryBytes = Arrays.copyOfRange(record, 42, 52);
					salary = Long.parseUnsignedLong(new String(salaryBytes));
					salarySum += salary;

					hitsBuffer.put(record);
					if (hitsBuffer.capacity() - hitsBuffer.position() < 100 || !indexBlock.hasRemaining()) {
						hitsBuffer.flip();
						while (hitsBuffer.hasRemaining()) {
							fc.write(hitsBuffer);
						}
						outputWrites++;
						hitsBuffer.clear();
					}
				}
			}
			closeOutput();
			indexReads = bucketFile.getReads();
			relationReads = relationFile.getReads();
		} catch (FileNotFoundException e) {
			// Index bucket file doesn't exist. Do nothing
			System.out.println("Number of hits:       " + 0);
		} finally {
			System.out.println("Average salary:       " + salarySum / hitCount);
			System.out.println("\nDisk IO statistics");
			System.out.println("Index block reads:    " + indexReads);
			System.out.println("Relation block reads: " + relationReads);
			System.out.println("Output block writes:  " + outputWrites);
		}
	}

	public void closeOutput() throws IOException {
		fos.close();
		fc.close();
	}
}
