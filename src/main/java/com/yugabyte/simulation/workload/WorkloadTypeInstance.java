package com.yugabyte.simulation.workload;

public abstract class WorkloadTypeInstance {
	private volatile WorkloadStatusType status;
	private Exception terminatingException = null;
	
	private final String workloadId;
	private final long startTime;
	private long endTime = -1;

	public abstract WorkloadType getType();
	public abstract boolean isComplete();
	
	public WorkloadTypeInstance() {
		this.startTime = System.currentTimeMillis();
		this.workloadId = getType().getTypeName() + "_" + this.startTime;
		this.status = WorkloadStatusType.SUBMITTED;
		this.doInitialize();
		this.status = WorkloadStatusType.EXECUTING;
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
}
