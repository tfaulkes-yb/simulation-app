package com.yugabyte.simulation.dao;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.yugabyte.simulation.services.TimerType;

public class WorkloadDesc {
	private final String workloadId;
	private final String name;
	private final String description;
	private final Map<String, String> workloadNames;
	private final List<WorkloadParamDesc> params;
	
	public WorkloadDesc(String workloadId, String name, String description, WorkloadParamDesc ... params) {
		super();
		this.workloadId = workloadId;
		this.name = name;
		this.description = description;
		this.params = Arrays.asList(params);
		this.workloadNames = new HashMap<>();
		this.workloadNames.put(TimerType.WORKLOAD1.toString(), "Workload 1");
		this.workloadNames.put(TimerType.WORKLOAD2.toString(), "Workload 2");
	}

	public WorkloadDesc nameWorkload(TimerType type, String name) {
		this.workloadNames.put(type.toString(), name);
		return this;
	}

	public String getWorkloadId() {
		return workloadId;
	}
	
	public String getName() {
		return name;
	}

	public String getDescription() {
		return description;
	}

	public List<WorkloadParamDesc> getParams() {
		return params;
	}
	
	public String getWorkloadName(TimerType type) {
		return this.workloadNames.get(type.toString());
	}
	
	public Map<String, String> getWorkloadNames() {
		return workloadNames;
	}
}
