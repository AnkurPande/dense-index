package model;

public class Performance {
	
	long startTime, endTime;
	float startMem, endMem;
	
	public void startTimer(){
		//start time
		startTime = System.currentTimeMillis();
	}
	
	public void stopTimer(){
		//end time
		endTime = System.currentTimeMillis();
	}
	
	public void calculateStartMemory(){
		//available memory at start.
		startMem = Runtime.getRuntime().freeMemory();
	}
	
	public void calculateEndMemory(){
		//available memory at end.
		endMem = Runtime.getRuntime().totalMemory();
	}
		
	public long getTimeElapsed() {
		//Return the elapsed time.
		return endTime -startTime;
	}

	public float getMemUsed() {
		//return the memory used.
		return endMem - startMem;
	}
}
