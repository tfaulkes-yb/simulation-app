package com.yugabyte.simulation.workload;

import java.util.ArrayList;
import java.util.List;

import com.yugabyte.simulation.dao.TimerResult;
import com.yugabyte.simulation.dao.WorkloadResult;
import com.yugabyte.simulation.services.TimerService;

public abstract class WorkloadTypeInstance {
	private volatile WorkloadStatusType status;
	private Exception terminatingException = null;
	
	private final String workloadId;
	private final long startTime;
	private long endTime = -1;

	public abstract WorkloadType getType();
	public abstract boolean isComplete();
	private final int workloadOrdinal;
	private final TimerService timerService;
	
	private final List<TimerResult> timingResults;
	
	public WorkloadTypeInstance(TimerService timerService) {
		this.startTime = System.currentTimeMillis();
		this.workloadId = createWorkloadId(); 
		this.status = WorkloadStatusType.SUBMITTED;
		this.doInitialize();
		this.status = WorkloadStatusType.EXECUTING;
		this.timingResults = new ArrayList<TimerResult>();
		this.workloadOrdinal = timerService.startTimingWorkload(this);
		this.timerService = timerService;
	}

	protected TimerResult doAugmentTimingResult(TimerResult result) {
		return result;
	}
	
	protected String createWorkloadId() {
		return getType().getTypeName() + "_" + this.startTime;
	}
	protected void doTerminate() {}
	
	protected void doInitialize() {}

	public final void terminate() {
		this.status = WorkloadStatusType.TERMINATING;
		this.doTerminate();
		this.status = WorkloadStatusType.TERMINATED;
		this.endTime = System.currentTimeMillis();
	}
	
	
	public boolean isTerminated() {
		return WorkloadStatusType.TERMINATED.equals(status);
	}
	
	public void setTerminatedByException(Exception e) {
		this.terminatingException = e;
		this.terminate();
	}
	
	public Exception getTerminatingException() {
		return terminatingException;
	}
	
	public String getWorkloadId() {
		return workloadId;
	}
	
	public long getStartTime() {
		return startTime;
	}
	
	public long getEndTime() {
		return endTime;
	}
	
	public WorkloadStatusType getStatus() {
		return status;
	}
	
	public List<TimerResult> getTimingResults() {
		return timingResults;
	}
	public void submitTimingResult(TimerResult result, int maxLength) {
		synchronized (timingResults) {
			TimerResult newResult = doAugmentTimingResult(result);
			timingResults.add(newResult);
			if (timingResults.size() > maxLength) {
				timingResults.remove(0);
			}
		}
	}
	protected TimerService getTimerService() {
		return timerService;
	}
	protected int getWorkloadOrdinal() {
		return this.workloadOrdinal;
	}
	
	public List<TimerResult> getResults(long fromTime) {
		synchronized (timingResults) {
			if (fromTime <= 0) {
				return this.timingResults;
			}
			else {
				// Return a sub-array containing the correct elements which
				// are greater than fromTime. Do a binary search for this.
				// Binary search for the right element
				int length = timingResults.size();
				int start = 0;
				int end = length-1;
				int index = -1;
				while (start <= end) {
					int mid = (start + end)/2;
					// Move to the right side if the target is greater
					if (timingResults.get(mid).getStartTimeMs() <= fromTime) {
						start = mid + 1;
					}
					else {
						// Move left side
						index = mid;
						end = mid - 1;
					}
				}
				if (index >= 0) {
					return timingResults.subList(index, length);
				}
				else {
					return new ArrayList<TimerResult>();
				}
			}
		}
	}
	
	public WorkloadResult getWorkloadResult(long afterTime) {
		return new WorkloadResult(afterTime, this);
	}

}
