package com.yugabyte.simulation.dao;

public class WorkloadStatus {
	private String workloadId;
	private long startTime;
	private long endTime;
	private String status;
	
	
	public WorkloadStatus(String workloadId, long startTime, long endTime, String status) {
		super();
		this.workloadId = workloadId;
		this.startTime = startTime;
		this.endTime = endTime;
		this.status = status;
	}
	
	public WorkloadStatus() {
	}
	
	public String getWorkloadId() {
		return workloadId;
	}
	public void setWorkloadId(String workloadId) {
		this.workloadId = workloadId;
	}
	public long getStartTime() {
		return startTime;
	}
	public void setStartTime(long startTime) {
		this.startTime = startTime;
	}
	public long getEndTime() {
		return endTime;
	}
	public void setEndTime(long endTime) {
		this.endTime = endTime;
	}
	public String getStatus() {
		return status;
	}
	public void setStatus(String status) {
		this.status = status;
	}
	
	
}
