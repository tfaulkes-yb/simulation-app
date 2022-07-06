package com.yugabyte.simulation.service;

import java.util.List;

import com.yugabyte.simulation.dao.InvocationResult;
import com.yugabyte.simulation.dao.ParamValue;
import com.yugabyte.simulation.dao.WorkloadDesc;

public interface WorkloadSimulation {
	public List<WorkloadDesc> getWorkloads();
	public InvocationResult invokeWorkload(String workloadId, ParamValue[] value);
	public String getName();
}
