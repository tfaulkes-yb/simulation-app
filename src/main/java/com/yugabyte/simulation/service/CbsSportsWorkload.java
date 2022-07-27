package com.yugabyte.simulation.service;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowCallbackHandler;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.yugabyte.simulation.dao.InvocationResult;
import com.yugabyte.simulation.dao.ParamValue;
import com.yugabyte.simulation.dao.WorkloadDesc;
import com.yugabyte.simulation.dao.WorkloadParamDesc;
import com.yugabyte.simulation.services.ServiceManager;
import com.yugabyte.simulation.workload.FixedStepsWorkloadType;
import com.yugabyte.simulation.workload.FixedTargetWorkloadType;
import com.yugabyte.simulation.workload.Step;
import com.yugabyte.simulation.workload.ThroughputWorkloadType;
import com.yugabyte.simulation.workload.WorkloadSimulationBase;

@Repository
public class CbsSportsWorkload extends WorkloadSimulationBase implements WorkloadSimulation {

	@Autowired
	private JdbcTemplate jdbcTemplate;
	
	@Autowired
	private ServiceManager serviceManager;
	
	@Override
	public String getName() {
		return "CBS Sports";
	}

	private static final String CREATE_SCHEMA =
			"create schema if not exists psuser;";
	
	private static final String CREATE_TABLE = 
			"create table if not exists psuser.newsletter_subscriptions ("
			+ "subscription_id bigint not null, "
			+ "cust_id bigint not null,"
			+ "mpid bigint not null,"
			+ "mcode varchar(11) not null,"
			+ "subscribed_ind smallint not null,"
			+ "opt_in_date timestamp,"
			+ "opt_out_date timestamp,"
			+ "opt_in_source varchar(256),"
			+ "create_dt timestamp not null default now(), "
			+ "mod_dt timestamp not null default now(), "
			+ "constraint newsletter_subscriptions_pk primary key (cust_id, mcode)"
			+ ") split into 1 tablets;";
			
	private final String DROP_TABLE = "drop table if exists psuser.newsletter_subscriptions;";
	
	private final String CREATE_INDEX = "create index newsletter_subscriptions_id on psuser.newsletter_subscriptions ( subscription_id );";

	private final String QUERY = "select SUBSCRIPTION_ID, CUST_ID, MCODE, MPID, SUBSCRIBED_IND, OPT_IN_DATE, OPT_OUT_DATE, OPT_IN_SOURCE from PSUSER.NEWSLETTER_SUBSCRIPTIONS where CUST_ID = ? and SUBSCRIBED_IND = 1 /** SportyApi **/";
	
	private final String INSERT = 
			"insert into psuser.newsletter_subscriptions ("
			+ "subscription_id, cust_id, mpid, mcode, subscribed_ind,"
			+ "opt_in_date, opt_out_date, opt_in_source)"
			+ " values "
			+ "(?, ?, ?, ?, ?, ?, ?, ?);";
	
	private enum WorkloadType {
		CREATE_TABLES, 
		SEED_DATA,
		UNBOUNDED_SIMULATION,
		RUN_SIMULATION,
		RUN_SIMULATION_RO_RR,
		RUN_SIMULATION_RO_RC,
		RUN_SIMULATION_RW_RR,
		RUN_SIMULATION_RW_RC,
	}		
	
	private final FixedStepsWorkloadType createTablesWorkloadType;
	private final FixedTargetWorkloadType runInstanceType;
	private final ThroughputWorkloadType throughtputType;
	
	public CbsSportsWorkload() {
		this.createTablesWorkloadType = new FixedStepsWorkloadType(
				new Step("Drop Table", (a,b) -> {
					jdbcTemplate.execute(DROP_TABLE);	
				}),
				new Step("Create Schema", (a,b) -> {
					jdbcTemplate.execute(CREATE_SCHEMA);
				}),
				new Step("Create Table", (a,b) -> {
					jdbcTemplate.execute(CREATE_TABLE);	
				}),
				new Step("Create Index", (a,b) -> {
					jdbcTemplate.execute(CREATE_INDEX);	
				})
		);
				
		this.throughtputType = new ThroughputWorkloadType();
		this.runInstanceType = new FixedTargetWorkloadType();
	}
	
	private WorkloadDesc createTablesWorkload = new WorkloadDesc(
			WorkloadType.CREATE_TABLES.toString(),
			"Create Tables", 
			"Create the table. If the table already exists it will be dropped"
		);
	
	private WorkloadDesc unboundedWorkload = new WorkloadDesc(
			WorkloadType.UNBOUNDED_SIMULATION.toString(),
			"Unbounded Simulation",
			"Run a simulation of a simple table",
			new WorkloadParamDesc("Invocations", 1, Integer.MAX_VALUE, 1000),
			new WorkloadParamDesc("Delay", 0, 1000000, 0),
			new WorkloadParamDesc("Threads", 1, 500, 32)
		);
	
	private WorkloadDesc runningWorkload = new WorkloadDesc(
			WorkloadType.RUN_SIMULATION.toString(),
			"Simulation",
			"Run a simulation of a simple table",
			new WorkloadParamDesc("TPS", 1, Integer.MAX_VALUE, 1000),
			new WorkloadParamDesc("MaxThreads", 1, 500, 32)
		);
	
	private WorkloadDesc runningWorkload_RO_RR = new WorkloadDesc(
			WorkloadType.RUN_SIMULATION_RO_RR.toString(),
			"Simulation_RO_RR",
			"Run a simulation of a simple table",
			new WorkloadParamDesc("Invocations", 1, Integer.MAX_VALUE, 1000),
			new WorkloadParamDesc("Delay", 0, 1000000, 0),
			new WorkloadParamDesc("Threads", 1, 500, 32)
		);
	
	private WorkloadDesc runningWorkload_RO_RC = new WorkloadDesc(
			WorkloadType.RUN_SIMULATION_RO_RC.toString(),
			"Simulation_RO_RC",
			"Run a simulation of a simple table",
			new WorkloadParamDesc("Invocations", 1, Integer.MAX_VALUE, 1000),
			new WorkloadParamDesc("Delay", 0, 1000000, 0),
			new WorkloadParamDesc("Threads", 1, 500, 32)
		);
	
	private WorkloadDesc runningWorkload_RW_RR = new WorkloadDesc(
			WorkloadType.RUN_SIMULATION_RW_RR.toString(),
			"Simulation_RW_RR",
			"Run a simulation of a simple table",
			new WorkloadParamDesc("Invocations", 1, Integer.MAX_VALUE, 1000),
			new WorkloadParamDesc("Delay", 0, 1000000, 0),
			new WorkloadParamDesc("Threads", 1, 500, 32)
		);
	
	private WorkloadDesc runningWorkload_RW_RC = new WorkloadDesc(
			WorkloadType.RUN_SIMULATION_RW_RC.toString(),
			"Simulation_RW_RC",
			"Run a simulation of a simple table",
			new WorkloadParamDesc("Invocations", 1, Integer.MAX_VALUE, 1000),
			new WorkloadParamDesc("Delay", 0, 1000000, 0),
			new WorkloadParamDesc("Threads", 1, 500, 32)
		);
	
	private WorkloadDesc seedDataWorkload = new WorkloadDesc(
			WorkloadType.SEED_DATA.toString(),
			"Seed the data",
			"Create sample data",
			new WorkloadParamDesc("Number of records", 1, Integer.MAX_VALUE, 1000),
			new WorkloadParamDesc("Threads", 1, 500, 32)
		);
	
	@Override
	public List<WorkloadDesc> getWorkloads() {
		return Arrays.asList(
			createTablesWorkload, seedDataWorkload, unboundedWorkload, runningWorkload, 
			runningWorkload_RO_RR, runningWorkload_RO_RC, runningWorkload_RW_RC, runningWorkload_RW_RR
		);
	}
	
	@Override
	public InvocationResult invokeWorkload(String workloadId, ParamValue[] values) {
		WorkloadType type = WorkloadType.valueOf(workloadId);
		try {
			switch (type) {
			case CREATE_TABLES:
				this.createTables();
				return new InvocationResult("Ok");
			
			case UNBOUNDED_SIMULATION:
				this.unboundedSimulation(values[0].getIntValue(), values[1].getIntValue());
				return new InvocationResult("Ok");
				
			case SEED_DATA:
				this.seedData(values[0].getIntValue(), values[1].getIntValue());
				return new InvocationResult("Ok");
				
			case RUN_SIMULATION:
				this.runSimulation(values[0].getIntValue(), values[1].getIntValue(), values[2].getIntValue());
				return new InvocationResult("Ok");

			case RUN_SIMULATION_RO_RR:
				this.runSimulation_RO_RR(values[0].getIntValue(), values[1].getIntValue(), values[2].getIntValue());
				return new InvocationResult("Ok");

			case RUN_SIMULATION_RO_RC:
				this.runSimulation_RO_RC(values[0].getIntValue(), values[1].getIntValue(), values[2].getIntValue());
				return new InvocationResult("Ok");

			case RUN_SIMULATION_RW_RR:
				this.runSimulation_RW_RR(values[0].getIntValue(), values[1].getIntValue(), values[2].getIntValue());
				return new InvocationResult("Ok");

			case RUN_SIMULATION_RW_RC:
				this.runSimulation_RW_RC(values[0].getIntValue(), values[1].getIntValue(), values[2].getIntValue());
				return new InvocationResult("Ok");

			}
			throw new IllegalArgumentException("Unknown workload "+ workloadId);
		}
		catch (Exception e) {
			return new InvocationResult(e);
		}
	}

	private void createTables() {
		createTablesWorkloadType.createInstance(serviceManager).execute();
	}
	
	private void insertRecord(AtomicLong currentCounter) {
		jdbcTemplate.update(INSERT, new Object[] {
			currentCounter.getAndIncrement(),
			LoadGeneratorUtils.getLong(1000, 30_000_00),
			LoadGeneratorUtils.getLong(10, 500),
			LoadGeneratorUtils.getHexString(7),
			LoadGeneratorUtils.getInt(0, 2),
			new Date(),
			new Date(),
			null
		}, new int[] {
			Types.BIGINT, Types.BIGINT, Types.BIGINT, Types.VARCHAR, 
			Types.SMALLINT, Types.TIMESTAMP, Types.TIMESTAMP, Types.VARCHAR
		});
	}
	private void runQueryNoTxn() {
		int custNum = ThreadLocalRandom.current().nextInt(1000, 20000000);
		jdbcTemplate.query(QUERY, new Object[] {custNum}, new int[] {Types.INTEGER},
			new RowCallbackHandler() {
				@Override
				public void processRow(ResultSet rs) throws SQLException {
				}
			});
	}
	
	@Transactional(readOnly = true, propagation = Propagation.NOT_SUPPORTED, isolation = Isolation.REPEATABLE_READ)
	private void runQuery_RO_RR_Txn() {
		int custNum = ThreadLocalRandom.current().nextInt(1000, 20000000);
		jdbcTemplate.query(QUERY, new Object[] {custNum}, new int[] {Types.INTEGER},
			new RowCallbackHandler() {
				@Override
				public void processRow(ResultSet rs) throws SQLException {
				}
			});
	}
	
	@Transactional(readOnly = true, propagation = Propagation.NOT_SUPPORTED, isolation = Isolation.READ_COMMITTED)
	private void runQuery_RO_RC_Txn() {
		int custNum = ThreadLocalRandom.current().nextInt(1000, 20000000);
		jdbcTemplate.query(QUERY, new Object[] {custNum}, new int[] {Types.INTEGER},
			new RowCallbackHandler() {
				@Override
				public void processRow(ResultSet rs) throws SQLException {
				}
			});
	}
	
	@Transactional(readOnly = false, propagation = Propagation.NOT_SUPPORTED, isolation = Isolation.REPEATABLE_READ)
	private void runQuery_RW_RR_Txn() {
		int custNum = ThreadLocalRandom.current().nextInt(1000, 20000000);
		jdbcTemplate.query(QUERY, new Object[] {custNum}, new int[] {Types.INTEGER},
			new RowCallbackHandler() {
				@Override
				public void processRow(ResultSet rs) throws SQLException {
				}
			});
	}
	
	@Transactional(readOnly = false, propagation = Propagation.NOT_SUPPORTED, isolation = Isolation.READ_COMMITTED)
	private void runQuery_RW_RC_Txn() {
		int custNum = ThreadLocalRandom.current().nextInt(1000, 20000000);
		jdbcTemplate.query(QUERY, new Object[] {custNum}, new int[] {Types.INTEGER},
			new RowCallbackHandler() {
				@Override
				public void processRow(ResultSet rs) throws SQLException {
				}
			});
	}

	private void seedData(int target, int threads) {
		jdbcTemplate.setFetchSize(1000);

		final AtomicLong currentValue = new AtomicLong();
		jdbcTemplate.query("select max(subscription_id) from psuser.newsletter_subscriptions",
				(rs) -> { currentValue.set(rs.getLong(1)+1); } );
		runInstanceType
			.createInstance(serviceManager)
			.setCustomData(currentValue)
			.execute(threads, target, (customData, threadData) -> {
				insertRecord((AtomicLong)customData);
				return null;
			});
	}

	private void unboundedSimulation(int targetTps, int maxThreads) {
		jdbcTemplate.setFetchSize(1000);

		throughtputType
			.createInstance(serviceManager)
			.setMaxThreads(maxThreads)
			.execute(targetTps, (customData, threadData) -> {
				runQueryNoTxn();
			});
	}
	
	private void runSimulation(int target, int delay, int threads) {
		jdbcTemplate.setFetchSize(1000);

		runInstanceType
			.createInstance(serviceManager)
			.setDelayBetweenInvocations(delay)
			.execute(threads, target, (customData, threadData) -> {
				runQueryNoTxn();
				return null;
			});
	}
	
	private void runSimulation_RW_RC(int target, int delay, int threads) {
		jdbcTemplate.setFetchSize(1000);

		runInstanceType
			.createInstance(serviceManager)
			.setDelayBetweenInvocations(delay)
			.execute(threads, target, (customData, threadData) -> {
				runQuery_RW_RC_Txn();
				return null;
			});
	}
	
	private void runSimulation_RO_RR(int target, int delay, int threads) {
		jdbcTemplate.setFetchSize(1000);

		runInstanceType
			.createInstance(serviceManager)
			.setDelayBetweenInvocations(delay)
			.execute(threads, target, (customData, threadData) -> {
				runQuery_RO_RR_Txn();
				return null;
			});
	}
	private void runSimulation_RO_RC(int target, int delay, int threads) {
		jdbcTemplate.setFetchSize(1000);

		runInstanceType
			.createInstance(serviceManager)
			.setDelayBetweenInvocations(delay)
			.execute(threads, target, (customData, threadData) -> {
				runQuery_RO_RC_Txn();
				return null;
			});
	}
	private void runSimulation_RW_RR(int target, int delay, int threads) {
		jdbcTemplate.setFetchSize(1000);

		runInstanceType
			.createInstance(serviceManager)
			.setDelayBetweenInvocations(delay)
			.execute(threads, target, (customData, threadData) -> {
				runQuery_RW_RR_Txn();
				return null;
			});
	}

}
