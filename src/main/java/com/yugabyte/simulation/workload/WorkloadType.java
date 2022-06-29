package com.yugabyte.simulation.workload;

import com.yugabyte.simulation.services.TimerService;

public abstract class WorkloadType {
	public abstract String getTypeName();
	public abstract WorkloadTypeInstance createInstance(TimerService timerService, WorkloadManager workloadManager);
	public boolean canBeTerminated() {
		return true;
	}
}