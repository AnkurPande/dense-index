package lookup;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
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
		hitsBuffer = ByteBuffer.allocateDirect(IOFile.BLOCK_SIZE);
		outputWrites = 0;
	}

	public void lookupHits(Short age) throws IOException {
		openOutput();

		String bucketName = age.toString();

		long indexReads = 0;
		long relationReads = 0;
		long hitCount = 0;
		long salarySum = 0;
		byte[] salaryBytes;
		long salary;
		
		long indexOffset = 0;
		ByteBuffer indexBlock = null;
		ByteBuffer dataBlock = null;
		
		ArrayList<Integer> recordOffsets = new ArrayList<Integer>();
		long recordBlock = 0;
		long recordByteOffset = 0;
		int position = 0;
		
		long currentDataBlockStart = 0;
		
		byte[] record = new byte[IOFile.RECORD_SIZE];
		
		try {
			IOFile bucketFile = new IOFile(INDEX_PATH + bucketName);
			IOFile relationFile = new IOFile(IndexCreator.FILE_NAME);

			long bucketSize = bucketFile.length();
			hitCount = bucketSize / 4;
			System.out.println("Number of hits:       " + hitCount);
			
			// Read entire index block sequentially
			while (indexOffset < bucketSize) {
				indexBlock = bucketFile.readSequentialBlock();
				indexOffset += IOFile.BLOCK_SIZE;
				
				// Add record offset to list
				while (indexBlock.hasRemaining()) {
					recordOffsets.add(indexBlock.getInt());
				}
			}
			
			// Read relation file blocks in order
			for (int i = 0; i < recordOffsets.size(); ++i) {
				// Calculate position of record in data file
				recordByteOffset = (long)recordOffsets.get(i) * IOFile.RECORD_SIZE;
				recordBlock = recordByteOffset / IOFile.BLOCK_SIZE;
				
				// Read data file block (if necessary)
				if (currentDataBlockStart == 0 || recordByteOffset >= currentDataBlockStart + IOFile.BLOCK_SIZE) {
					dataBlock = relationFile.readRandomBlock(recordBlock * IOFile.BLOCK_SIZE);
					currentDataBlockStart = (recordByteOffset / IOFile.BLOCK_SIZE) * IOFile.BLOCK_SIZE;
				}
				
				position = (int) (recordByteOffset - currentDataBlockStart);
				
				// Process records within data file block
				dataBlock.position(position);
				if (dataBlock.remaining() < IOFile.RECORD_SIZE) {
					int rem = dataBlock.remaining();
					// Read first half of record
					dataBlock.get(record, 0, rem);
					// Read next block
					dataBlock = relationFile.readSequentialBlock();
					// Read second half of record
					dataBlock.get(record, rem, IOFile.RECORD_SIZE - rem);
				} else {
					dataBlock.get(record);
				}
				
				salaryBytes = Arrays.copyOfRange(record, 41, 51);
				salary = Long.parseUnsignedLong(new String(salaryBytes));
				salarySum += salary;
				
				// Add matching record to hitsBuffer
				if (hitsBuffer.remaining() < IOFile.RECORD_SIZE) {
					// Fill up the rest of hitsbuffer
					int rem = hitsBuffer.remaining();
					hitsBuffer.put(record, 0, rem);
					// Write hitsbuffer
					hitsBuffer.flip();
					while (hitsBuffer.hasRemaining()) {
						fc.write(hitsBuffer);
					}
					outputWrites++;
					hitsBuffer.clear();
					// Add the rest of the record to hitsbuffer
					hitsBuffer.put(record, rem, IOFile.RECORD_SIZE - rem);
				} else {
					hitsBuffer.put(record);
				}
			}
			// Flush hitsBuffer to file.
			hitsBuffer.flip();
			while (hitsBuffer.hasRemaining()) {
				fc.write(hitsBuffer);
			}
			outputWrites++;
			hitsBuffer.clear();
			// Close file channels
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
