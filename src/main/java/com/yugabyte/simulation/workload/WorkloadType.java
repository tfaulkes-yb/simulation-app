package com.yugabyte.simulation.workload;

import com.yugabyte.simulation.services.ServiceManager;

public abstract class WorkloadType {
	public abstract String getTypeName();
	public abstract WorkloadTypeInstance createInstance(ServiceManager serviceManager);
	public boolean canBeTerminated() {
		return true;
	}
}