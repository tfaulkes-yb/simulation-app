package com.yugabyte.simulation.dao;

import java.util.Arrays;
import java.util.List;

public class WorkloadDesc {
	private final String workloadId;
	private final String name;
	private final String description;
	private final List<WorkloadParamDesc> params;
	
	public WorkloadDesc(String workloadId, String name, String description, WorkloadParamDesc ... params) {
		super();
		this.workloadId = workloadId;
		this.name = name;
		this.description = description;
		this.params = Arrays.asList(params);
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
}
