package com.yugabyte.simulation.service;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowCallbackHandler;
import org.springframework.stereotype.Repository;

import com.yugabyte.simulation.dao.InvocationResult;
import com.yugabyte.simulation.dao.ParamValue;
import com.yugabyte.simulation.dao.WorkloadDesc;
import com.yugabyte.simulation.dao.WorkloadParamDesc;
import com.yugabyte.simulation.services.TimerService;
import com.yugabyte.simulation.workload.FixedStepsWorkloadType;
import com.yugabyte.simulation.workload.FixedTargetWorkloadType;
import com.yugabyte.simulation.workload.ThroughputWorkloadType;
import com.yugabyte.simulation.workload.WorkloadManager;
import com.yugabyte.simulation.workload.WorkloadSimulationBase;

@Repository
public class CapitalGroupWorkload extends WorkloadSimulationBase implements WorkloadSimulation {

	@Autowired
	private JdbcTemplate jdbcTemplate;
	
	@Autowired
	private TimerService timerService;
	
	@Autowired 
	private WorkloadManager workloadManager;
	
	@Override
	public String getName() {
		return "Capital Group";
	}

	private static final String CREATE_TABLE_CACHED_DATA =
			"create table if not exists cached_data ("
			+ "id uuid not null,"
			+ "created timestamp not null default now(),"
			+ "updated timestamp not null default now(),"
			+ "int1 int,"
			+ "int2 int,"
			+ "int3 int,"
			+ "int4 int,"
			+ "int5 int,"
			+ "bigint1 bigint,"
			+ "bigint2 bigint,"
			+ "bigint3 bigint,"
			+ "bigint4 bigint,"
			+ "bigint5 bigint,"
			+ "decimal1 decimal,"
			+ "decimal2 decimal,"
			+ "decimal3 decimal,"
			+ "decimal4 decimal,"
			+ "decimal5 decimal,"
			+ "text1 varchar(500),"
			+ "text2 varchar(500),"
			+ "text3 varchar(500),"
			+ "text4 varchar(500),"
			+ "text5 varchar(500),"
			+ "text6 varchar(500),"
			+ "constraint cached_data_pk primary key (id)"
			+ ")";
			
	private static final String DROP_TABLE_CACHED_DATA = "drop table if exists cached_data;";

	private static final String INSERT_RECORD_CACHED_DATA = "insert into cached_data("
			+ "id, int1, int2, int3, int4, int5,"
			+ "bigint1, bigint2, bigint3, bigint4, bigint5,"
			+ "decimal1, decimal2, decimal3, decimal4, decimal5,"
			+ "text1, text2, text3, text4, text5, text6)"
			+ " values (?, ?, ?, ?, ?, ?,"
			+ "?,?,?,?,?,"
			+ "?,?,?,?,?,"
			+ "?,?,?,?,?,?"
			+ ");";
	
	private static final String POINT_QUERY_CACHED_DATA = "select * from cached_data where id = ?::uuid;";

	
	private static final String CREATE_TABLE_BULK_DATA =
			"create table if not exists bulk_data ("
			+ "id uuid not null,"
			+ "created timestamp not null default now(),"
			+ "updated timestamp not null default now(),"
			+ "int1 int,"
			+ "int2 int,"
			+ "int3 int,"
			+ "int4 int,"
			+ "int5 int,"
			+ "int6 int,"
			+ "int7 int,"
			+ "int8 int,"
			+ "int9 int,"
			+ "int10 int,"
			+ "bigint1 bigint,"
			+ "bigint2 bigint,"
			+ "bigint3 bigint,"
			+ "bigint4 bigint,"
			+ "bigint5 bigint,"
			+ "bigint6 bigint,"
			+ "bigint7 bigint,"
			+ "bigint8 bigint,"
			+ "bigint9 bigint,"
			+ "bigint10 bigint,"
			+ "decimal1 decimal,"
			+ "decimal2 decimal,"
			+ "decimal3 decimal,"
			+ "decimal4 decimal,"
			+ "decimal5 decimal,"
			+ "decimal6 decimal,"
			+ "decimal7 decimal,"
			+ "decimal8 decimal,"
			+ "decimal9 decimal,"
			+ "decimal10 decimal,"
			+ "text1 varchar(500),"
			+ "text2 varchar(500),"
			+ "text3 varchar(500),"
			+ "text4 varchar(500),"
			+ "text5 varchar(500),"
			+ "text6 varchar(500),"
			+ "text7 varchar(500),"
			+ "text8 varchar(500),"
			+ "text9 varchar(500),"
			+ "text10 varchar(500),"
			+ "constraint bulk_data_pk primary key (id)"
			+ ")";
			
	private static final String DROP_TABLE_BULK_DATA = "drop table if exists bulk_data;";

	private static final String INSERT_RECORD_BULK_DATA = "insert into bulk_data("
			+ "id, int1, int2, int3, int4, int5, int6, int7, int8, int9, int10,"
			+ "bigint1, bigint2, bigint3, bigint4, bigint5, bigint6, bigint7, bigint8, bigint9, bigint10,"
			+ "decimal1, decimal2, decimal3, decimal4, decimal5, decimal6, decimal7, decimal8, decimal9, decimal10,"
			+ "text1, text2, text3, text4, text5, text6, text7, text8, text9, text10)"
			+ " values (?,?,?,?,?,?,?,?,?,?,?,"
			+ "?,?,?,?,?,?,?,?,?,?,"
			+ "?,?,?,?,?,?,?,?,?,?,"
			+ "?,?,?,?,?,?,?,?,?,?"
			+ ");";
	

	private static final int ROWS_TO_PRELOAD = 10000;
	
	private enum WorkloadType {
		CREATE_TABLES, 
		SEED_DATA,
		BULK_DATA_LOAD,
		RUN_SIMULATION,
	}		
	
	private final FixedStepsWorkloadType createTablesWorkloadType;
	private final FixedTargetWorkloadType seedingWorkloadType;
	private final ThroughputWorkloadType runInstanceType;
	private final ThroughputWorkloadType bulkWorkloadType;
	
	public CapitalGroupWorkload() {
		this.createTablesWorkloadType = new FixedStepsWorkloadType(
				new FixedStepsWorkloadType.Step("Drop Main Table", (a,b) -> jdbcTemplate.execute(DROP_TABLE_CACHED_DATA)),
				new FixedStepsWorkloadType.Step("Drop Bulk Table", (a,b) -> jdbcTemplate.execute(DROP_TABLE_BULK_DATA)),
				new FixedStepsWorkloadType.Step("Create Main Table", (a,b) -> jdbcTemplate.execute(CREATE_TABLE_CACHED_DATA)),
				new FixedStepsWorkloadType.Step("Create Bulk Table", (a,b) -> jdbcTemplate.execute(CREATE_TABLE_BULK_DATA))
		);
				
		this.seedingWorkloadType = new FixedTargetWorkloadType();
		this.runInstanceType = new ThroughputWorkloadType();
		this.bulkWorkloadType = new ThroughputWorkloadType();
	}
	
	private WorkloadDesc createTablesWorkload = new WorkloadDesc(
			WorkloadType.CREATE_TABLES.toString(),
			"Create Tables", 
			"Create the table. If the table already exists it will be dropped"
		);
	
	private WorkloadDesc seedingWorkload = new WorkloadDesc(
			WorkloadType.SEED_DATA.toString(),
			"Seed Data",
			"Load data into the table",
			new WorkloadParamDesc("Items to generate:", 1, Integer.MAX_VALUE, 1000),
			new WorkloadParamDesc("Threads", 1, 500, 32)
		);

	private WorkloadDesc bulkSeedingWorkload = new WorkloadDesc(
			WorkloadType.BULK_DATA_LOAD.toString(),
			"Bulk Data Loading",
			"Load bulk data through batch inserts",
			new WorkloadParamDesc("Throughput (tps)", 1, 1000000, 500),
			new WorkloadParamDesc("Batch Size", 1, 2000, 100),
			new WorkloadParamDesc("Max Threads", 1, 500, 64)
		);
	

	private WorkloadDesc runningWorkload = new WorkloadDesc(
			WorkloadType.RUN_SIMULATION.toString(),
			"Simulation",
			"Run a simulation of a simple table",
			new WorkloadParamDesc("Throughput (tps)", 1, 1000000, 500),
			new WorkloadParamDesc("Max Threads", 1, 500, 64)
		);
	
	@Override
	public List<WorkloadDesc> getWorkloads() {
		return Arrays.asList(
			createTablesWorkload, seedingWorkload, runningWorkload, bulkSeedingWorkload
		);
	}

	private int asInt(ParamValue[] values, int index) {
		return values[index].getIntValue();
	}
	
	@Override
	public InvocationResult invokeWorkload(String workloadId, ParamValue[] values) {
		WorkloadType type = WorkloadType.valueOf(workloadId);
		try {
			switch (type) {
			case CREATE_TABLES:
				this.createTables();
				return new InvocationResult("Ok");
			
			case SEED_DATA:
				this.seedData(asInt(values, 0), asInt(values, 1));
				return new InvocationResult("Ok");
				
			case RUN_SIMULATION:
				this.runSimulation(asInt(values, 0), asInt(values, 1));
				return new InvocationResult("Ok");

			case BULK_DATA_LOAD:
				this.bulkDataLoad(asInt(values, 0), asInt(values, 1), asInt(values, 2));
				return new InvocationResult("Ok");
				
			}
			throw new IllegalArgumentException("Unknown workload "+ workloadId);
		}
		catch (Exception e) {
			return new InvocationResult(e);
		}
	}

	private void createTables() {
		createTablesWorkloadType.createInstance(timerService, workloadManager).execute();
	}
	
	private void seedData(int numberToGenerate, int threads) {
		seedingWorkloadType
			.createInstance(timerService, workloadManager)
			.execute(threads, numberToGenerate, (customData, threadData) -> {
				jdbcTemplate.update(INSERT_RECORD_CACHED_DATA,
						LoadGeneratorUtils.getUUID(),

						LoadGeneratorUtils.getInt(0, 100),
						LoadGeneratorUtils.getInt(20, 300),
						LoadGeneratorUtils.getInt(100, 1000),
						LoadGeneratorUtils.getInt(0, 1000),
						LoadGeneratorUtils.getInt(500, 10000000),
				
						LoadGeneratorUtils.getLong(1000000000L, 100000000000L),
						LoadGeneratorUtils.getLong(1000000000L, 100000000000L),
						LoadGeneratorUtils.getLong(1000000000L, 100000000000L),
						LoadGeneratorUtils.getLong(1000000000L, 100000000000L),
						LoadGeneratorUtils.getLong(1000000000L, 100000000000L),
						
						LoadGeneratorUtils.getDouble(),
						LoadGeneratorUtils.getDouble(),
						LoadGeneratorUtils.getDouble(),
						LoadGeneratorUtils.getDouble(1000.0, 10000000.0),
						LoadGeneratorUtils.getDouble(-5, 5),

						LoadGeneratorUtils.getText(200, 490),
						LoadGeneratorUtils.getText(200, 490),
						LoadGeneratorUtils.getText(200, 490),
						LoadGeneratorUtils.getText(200, 490),
						LoadGeneratorUtils.getText(200, 490),
						LoadGeneratorUtils.getText(200, 490)
					);
				
				return threadData;
			});
	}
	
	private void bulkDataLoad(int throughput, int batchSize, int maxThreads) {
		Random random = ThreadLocalRandom.current();
		jdbcTemplate.setFetchSize(1000);

		bulkWorkloadType
			.createInstance(timerService, workloadManager)
			.setMaxThreads(maxThreads)
			.execute(throughput, (customData, threadData) -> {
				jdbcTemplate.batchUpdate(INSERT_RECORD_BULK_DATA, new BatchPreparedStatementSetter() {
					
					@Override
					public void setValues(PreparedStatement ps, int i) throws SQLException {
						ps.setObject(1, LoadGeneratorUtils.getUUID());
						
						ps.setObject(2, LoadGeneratorUtils.getInt(0, 100));
						ps.setObject(3, LoadGeneratorUtils.getInt(0, 100));
						ps.setObject(4, LoadGeneratorUtils.getInt(0, 100));
						ps.setObject(5, LoadGeneratorUtils.getInt(0, 100));
						ps.setObject(6, LoadGeneratorUtils.getInt(0, 100));
						ps.setObject(7, LoadGeneratorUtils.getInt(0, 100));
						ps.setObject(8, LoadGeneratorUtils.getInt(0, 100));
						ps.setObject(9, LoadGeneratorUtils.getInt(0, 100));
						ps.setObject(10, LoadGeneratorUtils.getInt(0, 100));
						ps.setObject(11, LoadGeneratorUtils.getInt(0, 100));
						
						ps.setObject(12, LoadGeneratorUtils.getLong(1000000000L, 100000000000L));
						ps.setObject(13, LoadGeneratorUtils.getLong(1000000000L, 100000000000L));
						ps.setObject(14, LoadGeneratorUtils.getLong(1000000000L, 100000000000L));
						ps.setObject(15, LoadGeneratorUtils.getLong(1000000000L, 100000000000L));
						ps.setObject(16, LoadGeneratorUtils.getLong(1000000000L, 100000000000L));
						ps.setObject(17, LoadGeneratorUtils.getLong(1000000000L, 100000000000L));
						ps.setObject(18, LoadGeneratorUtils.getLong(1000000000L, 100000000000L));
						ps.setObject(19, LoadGeneratorUtils.getLong(1000000000L, 100000000000L));
						ps.setObject(20, LoadGeneratorUtils.getLong(1000000000L, 100000000000L));
						ps.setObject(21, LoadGeneratorUtils.getLong(1000000000L, 100000000000L));
						
						ps.setObject(22, LoadGeneratorUtils.getDouble(1000.0, 10000000.0));
						ps.setObject(23, LoadGeneratorUtils.getDouble(1000.0, 10000000.0));
						ps.setObject(24, LoadGeneratorUtils.getDouble(1000.0, 10000000.0));
						ps.setObject(25, LoadGeneratorUtils.getDouble(1000.0, 10000000.0));
						ps.setObject(26, LoadGeneratorUtils.getDouble(1000.0, 10000000.0));
						ps.setObject(27, LoadGeneratorUtils.getDouble(1000.0, 10000000.0));
						ps.setObject(28, LoadGeneratorUtils.getDouble(1000.0, 10000000.0));
						ps.setObject(29, LoadGeneratorUtils.getDouble(1000.0, 10000000.0));
						ps.setObject(30, LoadGeneratorUtils.getDouble(1000.0, 10000000.0));
						ps.setObject(31, LoadGeneratorUtils.getDouble(1000.0, 10000000.0));

						ps.setObject(32, LoadGeneratorUtils.getText(200, 490));
						ps.setObject(33, LoadGeneratorUtils.getText(200, 490));
						ps.setObject(34, LoadGeneratorUtils.getText(200, 490));
						ps.setObject(35, LoadGeneratorUtils.getText(200, 490));
						ps.setObject(36, LoadGeneratorUtils.getText(200, 490));
						ps.setObject(37, LoadGeneratorUtils.getText(200, 490));
						ps.setObject(38, LoadGeneratorUtils.getText(200, 490));
						ps.setObject(39, LoadGeneratorUtils.getText(200, 490));
						ps.setObject(40, LoadGeneratorUtils.getText(200, 490));
						ps.setObject(41, LoadGeneratorUtils.getText(200, 490));

					}
					
					@Override
					public int getBatchSize() {
						return batchSize;
					}
				});
			});
	}

	private List<UUID> getQueryList() {
		List<UUID> results = new ArrayList<UUID>(ROWS_TO_PRELOAD);
		jdbcTemplate.setMaxRows(ROWS_TO_PRELOAD);
		jdbcTemplate.setFetchSize(ROWS_TO_PRELOAD);
		jdbcTemplate.query("select id from cached_data limit " + ROWS_TO_PRELOAD,
			new RowCallbackHandler() {
			
				@Override
				public void processRow(ResultSet rs) throws SQLException {
					UUID value = (UUID)rs.getObject(1);
					results.add(value);
				}
		});
		return results;
	}


	private void runSimulation(int tps, int maxThreads) {
		System.out.println("**** Preloading data...");
		final List<UUID> uuids = getQueryList();
		System.out.println("**** Preloading complete...");

		Random random = ThreadLocalRandom.current();
		jdbcTemplate.setFetchSize(1000);

		runInstanceType
			.createInstance(timerService, workloadManager)
			.setMaxThreads(maxThreads)
			.execute(tps, (customData, threadData) -> {
				String query = POINT_QUERY_CACHED_DATA;
				UUID id = uuids.get(random.nextInt(uuids.size()));
				jdbcTemplate.query(query, new Object[] {id}, new int[] {java.sql.Types.VARCHAR},
					new RowCallbackHandler() {
						@Override
						public void processRow(ResultSet rs) throws SQLException {
							/*
							System.out.printf("id=%s, word='%s', active=%b\n", 
									rs.getString("id"),
									rs.getString("word_name"),
									rs.getInt("active_ind"));
							*/
						}
					});
			});
	}
}
