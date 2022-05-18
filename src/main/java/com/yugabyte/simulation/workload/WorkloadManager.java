package com.yugabyte.simulation.workload;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.springframework.stereotype.Service;

@Service
public class WorkloadManager {
	private List<WorkloadTypeInstance> activeWorkloads;
	private List<WorkloadTypeInstance> completedWorkloads;
	
	public WorkloadManager() {
		this.activeWorkloads = new ArrayList<WorkloadTypeInstance>();
		this.completedWorkloads = new ArrayList<WorkloadTypeInstance>();
	}
	
	private void addToWorkloadList(WorkloadTypeInstance instance, List<WorkloadTypeInstance> list) {
		long startTime = instance.getStartTime();
		for (int i = 0; i < list.size(); i++) {
			if (startTime < list.get(i).getStartTime()) {
				this.completedWorkloads.add(i, instance);
				return;
			}
		}
		// This item belongs at the end of the list
		list.add(instance);
	}
	
	private synchronized void updateStatus() {
		for (Iterator<WorkloadTypeInstance> iterator = this.activeWorkloads.iterator(); iterator.hasNext();) {
			WorkloadTypeInstance thisInstance = iterator.next();
			if (thisInstance.isComplete() || thisInstance.isTerminated()) {
				addToWorkloadList(thisInstance, this.completedWorkloads);
				iterator.remove();
			}
		}
	}
	
	public List<WorkloadTypeInstance> getActiveWorkloads() {
		this.updateStatus();
		return activeWorkloads;
	}
	
	public List<WorkloadTypeInstance> getCompletedWorkloads() {
		this.updateStatus();
		return completedWorkloads;
	}
	
	public synchronized void submitWorkload(WorkloadTypeInstance workloadTypeInstance) {
		this.addToWorkloadList(workloadTypeInstance, activeWorkloads);
	}
	
	public synchronized void registerWorkloadInstance(WorkloadTypeInstance instance) {
		this.addToWorkloadList(instance, activeWorkloads);
	}
	
	public synchronized WorkloadTypeInstance getWorkloadByName(String workloadId) {
		for (WorkloadTypeInstance instance : getActiveWorkloads()) {
			if (instance.getWorkloadId().equals(workloadId)) {
				return instance;
			}
		}
		return null;
	}
}
