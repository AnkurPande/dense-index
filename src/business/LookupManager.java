package business;

import java.io.IOException;
import java.util.HashMap;

import input_output.IOFile;
import model.IndexEntry;

public class LookupManager {

	public static final String INDEX_PATH = "C:\\Users\\Julian\\dev\\git\\dense-index\\resources\\index\\";

	private static HashMap<Short, IndexEntry> index = new HashMap<>();

	public static void main(String[] args) throws IOException {
		index.put((short) 25, new IndexEntry("bucket25", 2));
		LookupManager.lookupHits((short) 25);
	}

	public static void lookupHits(Short age) throws IOException {
		IndexEntry entry = index.get(age);

		long count = entry.getCount();
		long bucketSize = count * 5;
		System.out.println("Number of hits: " + count);

		String bucketName = entry.getBucketName();

		IOFile bucketFile = new IOFile(INDEX_PATH + bucketName);
		int offset = 0;
		char[] block;
		while (offset < bucketSize) {
			block = bucketFile.readBucketBlock(offset);
			offset += IOFile.BLOCK_SIZE;
			System.out.println("block: " + new String(block));
			int hitStart = 0;
			while (hitStart < bucketSize) {
				String hitBlock = new String(block, hitStart, 4);
				hitStart += 4;
				String hitRecord = new String(block, hitStart++, 1);

				long blockOffset = new Long(hitBlock);
				long recordOffset = new Long(hitRecord);

				long relationOffset = blockOffset * IOFile.BLOCK_SIZE + recordOffset * 100;

				System.out.println("relationOffset: " + relationOffset);
			}
		}
	}

}
