package com.yugabyte.simulation.workload;

public abstract class WorkloadTypeInstance {
	private boolean terminated = false;
	private Exception terminatingException = null;
	
	private final String workloadId;
	private final long startTime;
	private long endTime = -1;

	public abstract WorkloadType getType();
	public abstract boolean isComplete();
	
	public WorkloadTypeInstance() {
		this.startTime = System.currentTimeMillis();
		this.workloadId = getType().getTypeName() + "_" + this.startTime;
	}

	public void terminate() {
		this.terminated = true;
		this.endTime = System.currentTimeMillis();
	}
	
	public boolean isTerminated() {
		return terminated;
	}
	
	public void setTerminatedByException(Exception e) {
		this.terminatingException = e;
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
}
