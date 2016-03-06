package model;

public class Performance {
	
	long startTime, endTime;
	long usedMemory;
	
	public long getStartTime() {
		return startTime;
	}

	public long getEndTime() {
		return endTime;
	}

	public void setStartTime(long startTime) {
		this.startTime = startTime;
	}

	public void setEndTime(long endTime) {
		this.endTime = endTime;
	}

	public void setUsedMemory(long usedMemory) {
		this.usedMemory = usedMemory;
	}

	public long getUsedMemory() {
		return usedMemory;
	}

	public void startTimer(){
		//start time
		this.startTime = System.currentTimeMillis();
	}
	
	public void stopTimer(){
		//end time
		this.endTime = System.currentTimeMillis();
	}
		
	public long getTimeElapsed() {
		//Return the elapsed time.
		return endTime -startTime;
	}
	

	public void calculateMemUsed() {
		//return the memory used.
		Runtime.getRuntime().gc();
		this.usedMemory = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
	}
}
