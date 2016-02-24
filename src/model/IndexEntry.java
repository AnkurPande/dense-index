/**
 * 
 */
package model;

/**
 * @author Julian
 *
 */
public class IndexEntry {
	public IndexEntry(String bucketName, long count) {
		super();
		this.bucketName = bucketName;
		this.count = count;
	}
	private String bucketName;
	private long count;

	public String getBucketName() {
		return bucketName;
	}
	public void setBucketName(String bucketName) {
		this.bucketName = bucketName;
	}
	public long getCount() {
		return count;
	}
	public void setCount(long count) {
		this.count = count;
	}
}
