package com.yugabyte.simulation.service;

import com.yugabyte.simulation.services.ServiceManager;
import com.yugabyte.simulation.workload.FixedStepsWorkloadType;
import com.yugabyte.simulation.workload.FixedTargetWorkloadType;
import com.yugabyte.simulation.workload.ThroughputWorkloadType;
import com.yugabyte.simulation.workload.FixedStepsWorkloadType.FixedStepWorkloadInstance;
import com.yugabyte.simulation.workload.FixedStepsWorkloadType.Step;
import com.yugabyte.simulation.workload.FixedTargetWorkloadType.FixedTargetWorkloadInstance;
import com.yugabyte.simulation.workload.ThroughputWorkloadType.ThroughputWorkloadInstance;

public class WorkloadInvoker {

	private ServiceManager serviceManager;
	public WorkloadInvoker(ServiceManager serviceManager) {
		this.serviceManager = serviceManager;
	}
	
	public FixedStepWorkloadInstance newFixedStepsInstance(Step ... steps) {
		return new FixedStepsWorkloadType(steps).createInstance(this.serviceManager);
	}
	
	public FixedTargetWorkloadInstance newFixedTargetInstance() {
		return new FixedTargetWorkloadType().createInstance(this.serviceManager);
	}
	
	public ThroughputWorkloadInstance newThroughputWorkloadInstance() {
		return new ThroughputWorkloadType().createInstance(serviceManager);
	}
}
