package com.yugabyte.simulation.service;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowCallbackHandler;
import org.springframework.stereotype.Repository;

import com.yugabyte.simulation.dao.InvocationResult;
import com.yugabyte.simulation.dao.ParamValue;
import com.yugabyte.simulation.dao.WorkloadDesc;
import com.yugabyte.simulation.dao.WorkloadParamDesc;
import com.yugabyte.simulation.services.TimerService;
import com.yugabyte.simulation.workload.FixedStepsWorkloadType;
import com.yugabyte.simulation.workload.FixedStepsWorkloadType.FixedStepWorkloadInstance;
import com.yugabyte.simulation.workload.ThroughputWorkloadType;
import com.yugabyte.simulation.workload.ThroughputWorkloadType.ThroughputWorkloadInstance;
import com.yugabyte.simulation.workload.WorkloadManager;
import com.yugabyte.simulation.workload.WorkloadSimulationBase;

@Repository
public class SimpleSelectWorkload extends WorkloadSimulationBase implements WorkloadSimulation {

	@Autowired
	private JdbcTemplate jdbcTemplate;
	
	@Autowired
	private TimerService timerService;
	
	@Autowired 
	private WorkloadManager workloadManager;
	
	private static final String CREATE_TABLE =
			"create table if not exists vulgar_words ("
			+ "id integer not null, "
			+ "created timestamp not null default now(), "
			+ "word_name varchar(30) not null, "
			+ "active_ind boolean not null default true,"
			+ "constraint vulgar_words_pk primary key (id)"
			+ ") split into 1 tablets;";
			
	private final String DROP_TABLE = "drop table if exists vulgar_words;";

	private final String QUERY = "select * from vulgar_words where active_ind = true;";
	
	private enum WorkloadType {
		CREATE_TABLES, 
		RUN_SIMULATION,
	}		
	
	private static final String DROP_TABLE_STEP = "Drop Table";
	private static final String CREATE_TABLE_STEP = "Create Table";

	private final FixedStepsWorkloadType createTablesWorkloadType;
	
	private final ThroughputWorkloadType runInstanceType;
	
	public SimpleSelectWorkload() {
		this.createTablesWorkloadType = new FixedStepsWorkloadType(
				DROP_TABLE_STEP,
				CREATE_TABLE_STEP);
		
		this.runInstanceType = new ThroughputWorkloadType();
	}
	
	private WorkloadDesc createTablesWorkload = new WorkloadDesc(
			WorkloadType.CREATE_TABLES.toString(),
			"Create Tables", 
			"Create the table. If the table already exists it will be dropped"
		);
	
	private WorkloadDesc runningWorkload = new WorkloadDesc(
			WorkloadType.RUN_SIMULATION.toString(),
			"Simulation",
			"Run a simulation of a simple table",
			new WorkloadParamDesc("Throughput (tps)", true, 1, 1000000, 500),
			new WorkloadParamDesc("Max Threads", true, 1, 500, 64)
		);
	
	@Override
	public List<WorkloadDesc> getWorkloads() {
		return Arrays.asList(
			createTablesWorkload, runningWorkload
		);
	}

	
	@Override
	public InvocationResult invokeWorkload(String workloadId, ParamValue[] values) {
		WorkloadType type = WorkloadType.valueOf(workloadId);
		try {
			switch (type) {
			case CREATE_TABLES:
				timerService.setCurrentWorkload(createTablesWorkload);
				this.createTables();
				timerService.removeCurrentWorkload(createTablesWorkload);
				return new InvocationResult("Ok");
			
			case RUN_SIMULATION:
				this.runSimulation(values[0].getIntValue(), values[1].getIntValue());
				return new InvocationResult("Ok");

			}
			throw new IllegalArgumentException("Unknown workload "+ workloadId);
		}
		catch (Exception e) {
			return new InvocationResult(e);
		}
	}

	private void createTables() {
		FixedStepsWorkloadType jobType = createTablesWorkloadType;
		FixedStepWorkloadInstance workload = jobType.createInstance(timerService);
		workloadManager.registerWorkloadInstance(workload);
		workload.execute((stepNum, stepName) -> {
			switch (stepName) {
			case DROP_TABLE_STEP:
				jdbcTemplate.execute(DROP_TABLE);
				break;
			case CREATE_TABLE_STEP:
				jdbcTemplate.execute(CREATE_TABLE);
				break;
			}
		});
	}
	

	private void runSimulation(int tps, int maxThreads) {
		jdbcTemplate.setFetchSize(1000);

		ThroughputWorkloadInstance instance = runInstanceType.createInstance(timerService).setMaxThreads(maxThreads);
		workloadManager.registerWorkloadInstance(instance);
		instance
			.execute(tps, (customData, threadData) -> {
				String query = QUERY;
				jdbcTemplate.query(query,
					new RowCallbackHandler() {
						@Override
						public void processRow(ResultSet rs) throws SQLException {
							System.out.printf("id=%d, word='%s', active=%b\n", 
									rs.getString("id"),
									rs.getString("word_name"),
									rs.getInt("active_ind"));
						}
					});
			});
	}
}
