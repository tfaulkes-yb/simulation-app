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

import com.yugabyte.simulation.dao.WorkloadDesc;
import com.yugabyte.simulation.dao.WorkloadParamDesc;
import com.yugabyte.simulation.workload.Step;
import com.yugabyte.simulation.workload.WorkloadSimulationBase;

@Repository
public class AmexWorkload extends WorkloadSimulationBase implements WorkloadSimulation {

	@Autowired
	private JdbcTemplate jdbcTemplate;
	
	@Override
	public String getName() {
		return "American Express Workload";
	}

	private static final String CREATE_TABLE = 
			"create table if not exists key_value ("
			+ "key_bigint bigint not null, "
			+ "value_int int not null, "
			+ "value_bigint bigint not null,"
			+ "value_varchar varchar(200),"
			+ "created_date timestamp not null default now(),"
			+ "constraint key primary key (key_bigint)"
			+ ");";
			
	private final String DROP_TABLE = "drop table if exists key_value;";
	
	private final String DROP_INDEX = "drop index if exists key_value_idx;";
	
	private final String CREATE_INDEX = "create index if not exists key_value_idx on key_value ( value_bigint );";

	private final String QUERY = "select key_bigint, value_int, value_bigint, value_varchar, created_date from key_value where key_bigint = ?";
	
	private final String INSERT = 
			"insert into key_value ("
			+ "key_bigint, "
			+ "value_int,"
			+ "value_bigint, "
			+ "value_varchar)"
			+ " values "
			+ "(?, ?, ?, ?);";
	
	private final String UPDATE = 
			"update key_value set "
			+ "value_int = ?, "
			+ "value_bigint = ?, "
			+ "value_varchar = ? "
			+ " where key_bigint = ?;";
	
	private enum WorkloadType {
		CREATE_TABLES, 
		SEED_DATA,
		RUN_SIMULATION
	}		
	
	public List<WorkloadDesc> getWorkloads() {
		return Arrays.asList(
				new WorkloadDesc(
						WorkloadType.CREATE_TABLES.toString(),
						"Create Tables"
					)
					.setDescription("Create the table and index. If the table or index already exists they will be dropped.")
					.onInvoke((runner, params) -> {
						runner.newFixedStepsInstance(
							new Step("Drop Table", (a,b) -> jdbcTemplate.execute(DROP_TABLE)),	
							new Step("Create Table", (a,b) -> jdbcTemplate.execute(CREATE_TABLE)),
							new Step("Drop Index", (a,b) -> jdbcTemplate.execute(DROP_INDEX)),
							new Step("Create Index", (a,b) -> jdbcTemplate.execute(CREATE_INDEX))	
						)
						.execute();
					}),

				new WorkloadDesc(
						WorkloadType.SEED_DATA.toString(),
						"Seed the data",
						"Create sample data",
						new WorkloadParamDesc("Number of records", 1, Integer.MAX_VALUE, 1000),
						new WorkloadParamDesc("Threads", 1, 500, 32)
					)
					.onInvoke((runner, params) -> {
						jdbcTemplate.setFetchSize(1000);
	
						final AtomicLong currentValue = new AtomicLong(0);
						jdbcTemplate.query("select max(subscription_id) from newsletter_subscriptions",
								(rs) -> { currentValue.set(rs.getLong(1)+1); } );

						runner.newFixedTargetInstance()
							.setCustomData(currentValue)
							.execute(params.asInt(1), params.asInt(0),
									(customData, threadData) -> {
								insertRecord((AtomicLong)customData);
								return null;
							});
					}),

				new WorkloadDesc(
						WorkloadType.RUN_SIMULATION.toString(),
						"Unbounded Simulation",
						"Run a workload which performs a mix of reads and writes",
						new WorkloadParamDesc("% Reads (vs Writes)", 0, 100, 50, "reads"),
						new WorkloadParamDesc("TPS", 1, Integer.MAX_VALUE, 1000),
						new WorkloadParamDesc("MaxThreads", 1, 500, 32)
					)
					.onInvoke((runner, params) -> {
						jdbcTemplate.setFetchSize(1000);
						final AtomicLong maxRecords = new AtomicLong();
						System.out.printf("Querying number of records in the database...");
						jdbcTemplate.query("select max(key_bigint) from key_value",
								(rs) -> { maxRecords.set(rs.getLong(1)+1); } );
						System.out.printf("Done (%,d records exist)\n", maxRecords.get());

						final int percentReads = params.asInt(0);
						runner.newThroughputWorkloadInstance()
							.setCustomData(maxRecords)
							.setMaxThreads(params.asInt(2))
							.execute(params.asInt(1), (customData, threadData) -> {
								runQueryNoTxn(percentReads, ((AtomicLong)customData).get());
							});

					})

			);
	}
	
	private void insertRecord(AtomicLong currentCounter) {
		jdbcTemplate.update(INSERT, new Object[] {
			currentCounter.getAndIncrement(),
			LoadGeneratorUtils.getInt(0, 5_000_000),
			LoadGeneratorUtils.getLong(1000, 30_000_00),
			LoadGeneratorUtils.getText(20, 190)
		}, new int[] {
			Types.BIGINT, Types.INTEGER, Types.BIGINT, Types.VARCHAR
		});
	}
	
	private void updateRecord(long recordNumber) {
		jdbcTemplate.update(UPDATE, new Object[] {
			LoadGeneratorUtils.getInt(0, 5_000_000),
			LoadGeneratorUtils.getLong(1000, 30_000_00),
			LoadGeneratorUtils.getText(20, 190),
			recordNumber
		}, new int[] {
			Types.INTEGER, Types.BIGINT, Types.VARCHAR, Types.BIGINT
		});
	}
	
	private void queryRecord(long recordNumber) {
		jdbcTemplate.query(QUERY, new Object[] {recordNumber}, new int[] {Types.INTEGER},
			new RowCallbackHandler() {
				@Override
				public void processRow(ResultSet rs) throws SQLException {
				}
			});
	}
	
	private void runQueryNoTxn(int percentReaads, long numRecords) {
		long keyId = ThreadLocalRandom.current().nextLong(numRecords);
		if (ThreadLocalRandom.current().nextInt(100) < percentReaads) {
			queryRecord(keyId);
		}
		else {
			updateRecord(keyId);
		}
	}
}
