/**
 * @author Ankurp
 * 
 * */

package model;

public class Performance {
	
	long startTime, endTime;
	
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
	
	/*Getter and setters*/
	
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

}
