package com.yugabyte.simulation.controller;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.yugabyte.simulation.dao.InvocationResult;
import com.yugabyte.simulation.dao.ParamValue;
import com.yugabyte.simulation.dao.SystemPreferences;
import com.yugabyte.simulation.dao.WorkloadDesc;
import com.yugabyte.simulation.dao.WorkloadResult;
import com.yugabyte.simulation.dao.WorkloadStatus;
import com.yugabyte.simulation.service.WorkloadSimulation;
import com.yugabyte.simulation.services.LoggingFileManager;
import com.yugabyte.simulation.services.SystemPreferencesService;
import com.yugabyte.simulation.services.TimerService;
import com.yugabyte.simulation.workload.AggregationWorkloadType;
import com.yugabyte.simulation.workload.WorkloadManager;
import com.yugabyte.simulation.workload.WorkloadTypeInstance;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;

@RestController
@RequestMapping("/api")
public class WorkloadSimulationController {
    @Autowired
    private WorkloadManager workloadManager;

    @Autowired
    private SystemPreferencesService systemPreferencesService;
    
    // Generic interface, to be populated with class loaded dynamically?
    @Autowired
    @Resource(name="${spring.workload:sonosWorkload}")
    // we can use @Qualifier as well
    private WorkloadSimulation workloadSimulation;

	@PostConstruct
	private void setWorkloadName() {
    	this.systemPreferencesService.setName(workloadSimulation.getName());
	}

    @GetMapping("get-workloads")
    public List<WorkloadDesc> getWorkloads() {
    	return workloadSimulation.getWorkloads();
    }

    @PostMapping("/invoke-workload/{workload}") 
    @ResponseBody
    public InvocationResult invokeWorkload(@PathVariable String workload, @RequestBody ParamValue[] params) {
    	return workloadSimulation.invokeWorkload(workload, params);
    }
    
    @GetMapping("get-active-workloads")
    public List<WorkloadResult> getActiveWorkloads() {
    	List<WorkloadTypeInstance> activeWorkloads = workloadManager.getActiveWorkloads();
    	List<WorkloadResult> statuses = new ArrayList<WorkloadResult>();
    	for (WorkloadTypeInstance instance : activeWorkloads) {
    		if (instance.getType().canBeTerminated()) {
    			statuses.add(instance.getWorkloadResult(Long.MAX_VALUE));
    		}
    	}
    	return statuses;
    }

    @PostMapping("save-system-preferences")
    @ResponseBody
    public InvocationResult saveSystemPreferences(@RequestBody SystemPreferences preferences) {
    	this.systemPreferencesService.saveSystemPreferences(preferences);
    	return new InvocationResult("Ok");
    }
    
    @GetMapping("get-system-preferences")
    public SystemPreferences getSystemPreferences() {
    	return this.systemPreferencesService.getSystemPreferences();
    }
    

    @GetMapping("terminate-workload/{workloadId}")
    public InvocationResult terminateWorkload(@PathVariable String workloadId) {
    	workloadManager.terminateWorkload(workloadId);
    	return new InvocationResult("Ok");
    }
    
    @GetMapping("/getResults/{afterTime}")
    @ResponseBody
    public Map<String, WorkloadResult> getResults(
    		@PathVariable(name = "afterTime") long afterTime) {
    	
    	return workloadManager.getResults(afterTime);
    }
}

