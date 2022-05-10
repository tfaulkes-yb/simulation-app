package com.yugabyte.simulation.service;

import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;
import org.w3c.dom.css.Counter;

import com.yugabyte.simulation.dao.InvocationResult;
import com.yugabyte.simulation.dao.ParamType;
import com.yugabyte.simulation.dao.ParamValue;
import com.yugabyte.simulation.dao.WorkloadDesc;
import com.yugabyte.simulation.dao.WorkloadParamDesc;
import com.yugabyte.simulation.dao.WorkloadSimulationDAOImpl;
import com.yugabyte.simulation.services.ExecutionStatus;
import com.yugabyte.simulation.services.Timer;
import com.yugabyte.simulation.services.TimerService;
import com.yugabyte.simulation.services.TimerType;

@Repository
public class SonosWorkload implements WorkloadSimulation {

	@Autowired
	private JdbcTemplate jdbcTemplate;
	
	@Autowired
	private TimerService timerService;
	
	private static final String CREATE_TOPOLOGY_TABLE = 
			"create table if not exists topology (\n"
			+ "	parentid varchar,\n"
			+ "	Id varchar,\n"
			+ "	IdType varchar not null,\n"
			+ "	IdName varchar,\n"
			+ "	created timestamp without time zone default now(),\n"
			+ "	updated timestamp without time zone default now(),\n"
			+ "	constraint topology_pkey PRIMARY KEY(id,parentid)\n"
			+ ")\n"
			+ ";";

	private static final String DROP_TOPOLOGY_TABLE = 
			"drop table if exists topology;";
	
	private static final String TRUNCATE_TOPOLOGY_TABLE = "truncate topology";
			
	private static final String CREATE_MHHMAP_TABLE = 
			"create table if not exists mhhmap (\n"
			+ "	sonosId varchar,\n"
			+ "	mHHId varchar,\n"
			+ "	locId varchar,\n"
			+ "	status varchar,\n"
			+ "	splitReason varchar,\n"
			+ "	created timestamp without time zone default now(),\n"
			+ "        updated timestamp without time zone default now(),\n"
			+ "	constraint mhhmap_pkey PRIMARY KEY(sonosId,mHHId,locId)\n"
			+ ")\n"
			+ ";";
	private static final String DROP_MHHMAP_TABLE = 
			"drop table if exists mhhmap;";

	private static final String TRUNCATE_MHHMAP_TABLE = "truncate mhhmap";

	private static final String INSERT_TOPOLOGY_TABLE = 
			"insert into topology("
			+ "parentid,"
			+ "Id,"
			+ "IdType,"
			+ "IdName)"
			+ "values ("
			+ "?,"
			+ "?,"
			+ "?,"
			+ "?"
			+ ");";
	
	private static final String INSERT_PLAYER_TABLE = 
			"insert into mhhmap("
			+ "sonosId,"
			+ "mHHId,"
			+ "locId,"
			+ "status,"
			+ "splitReason)"
			+ "values ("
			+ "?,"
			+ "?,"
			+ "?,"
			+ "?,"
			+ "?"
			+ ");";
	
	private enum WorkloadType {
		CREATE_TABLES, 
		LOAD_DATA
	}		
	
	private enum IdType {
		USER,
		LOCATION_GROUP,
		LOCATION
	}
	
    private static final Logger LOGGER = LoggerFactory.getLogger(SonosWorkload.class);

	
	// NOTE: This class is NOT designed to be thread safe
	private static class MutableInteger {
		private int value;
		public MutableInteger() {
			this(0);
		}
		public MutableInteger(int value) {
			this.value = value;
		}
		public int addAndGet(int value) {
			this.value += value;
			return this.value;
		}
		public int subAndGet(int value) {
			this.value -= value;
			return this.value;
		}
		public int get() {
			return this.value;
		}
		@Override
		public String toString() {
			return Integer.toString(this.value);
		}
	}
	
	@Override
	public List<WorkloadDesc> getWorkloads() {
		return Arrays.asList(
			new WorkloadDesc(
					WorkloadType.CREATE_TABLES.toString(),
					"Create Tables", 
					"Create the simulation tables. If the tables are already created they will not be re-created unless 'force' is set to true",
					new WorkloadParamDesc("force", ParamType.BOOLEAN, false, new ParamValue(false))
			),
			new WorkloadDesc(
					WorkloadType.LOAD_DATA.toString(),
					"Load Data",
					"Generate test data into the database",
					new WorkloadParamDesc("Number of players", true, 1, Integer.MAX_VALUE, 1000),
					new WorkloadParamDesc("Truncate tables", false, false),
					new WorkloadParamDesc("Number of threads", true, 1, 1024, 32)
			)
		);
	}

	@Override
	public InvocationResult invokeWorkload(String workloadId, ParamValue[] values) {
		WorkloadType type = WorkloadType.valueOf(workloadId);
		try {
			switch (type) {
			case CREATE_TABLES:
				this.createTables(values[0].getBoolValue());
				return new InvocationResult("Ok");
			
			case LOAD_DATA:
				this.loadData(values[0].getIntValue(), values[1].getBoolValue(), values[2].getIntValue());
				return new InvocationResult("Ok");
			}
			throw new IllegalArgumentException("Unknown workload "+ workloadId);
		}
		catch (Exception e) {
			return new InvocationResult(e);
		}
	}
	
	private void createTables(boolean force) {
		if (force) {
			jdbcTemplate.execute(DROP_MHHMAP_TABLE);
			jdbcTemplate.execute(DROP_TOPOLOGY_TABLE);
		}
		jdbcTemplate.execute(CREATE_MHHMAP_TABLE);
		jdbcTemplate.execute(CREATE_TOPOLOGY_TABLE);
	}
	
	private void logCall(String name, Object[] data) {
		if (LOGGER.isDebugEnabled()) {
			StringBuffer sb = new StringBuffer();
			sb.append(name).append("(");
			for (int i = 0; i < data.length; i++) {
				if (data[i] == null) {
					sb.append("null");
				}
				else {
					sb.append(data[i].toString());
				}
				if (i < data.length-1) {
					sb.append(',');
				}
			}
			sb.append(')');
			LOGGER.debug(sb.toString());
		}
	}
	
	private int id = 1;
	private String generateUUID() {
		return LoadGeneratorUtils.getUUID().toString();
//		return Integer.toString(id++);
	}
	
	private String generatePlayer(String locId) {
		String sonosId = generateUUID();
		String mhhid = "Sonos_" + LoadGeneratorUtils.getUUID().toString() + "." 
				+ LoadGeneratorUtils.getFixedLengthNumber(10);
		String status = "ACTIVE";
		String splitReason = "UNSPLIT";

		Object[] data = new Object[] {sonosId, mhhid, locId, status, splitReason};
		logCall("generatePlayer", data);
		
		Timer timer = timerService.getTimer(TimerType.STATUS).start();
		jdbcTemplate.update(INSERT_PLAYER_TABLE, data);
		timer.end(ExecutionStatus.SUCCESS);
		return sonosId;

	}
	/**
	 * Generate a new topology with the given parameters.
	 * @param parentId. If this is null, the type will be forced to USER
	 * @param type 
	 * @return the id for this generated topology
	 */
	private String generateTopology(String parentId, IdType type) {
		// If there is no parent, it has to be the user level
		if (parentId == null) {
			type = IdType.USER;
		}
		String thisId = generateUUID();
		String idName = LoadGeneratorUtils.getName() + " " + LoadGeneratorUtils.getName();
		
		Object[] data = new Object[] {parentId == null? "null" : parentId, thisId, type.toString(), idName};
		logCall("generateTopology", data);

		Timer timer = timerService.getTimer(TimerType.SUBMISSION).start();
		jdbcTemplate.update(INSERT_TOPOLOGY_TABLE, data);
		timer.end(ExecutionStatus.SUCCESS);
		return thisId;
	}
	
	
	private static final double MAX_PLAYERS_PER_LOCATION = 10.0;
	private static final double MAX_LOCATION_GROUPS_PER_GROUP = 5.0;
	private static final double AVG_PLAYERS_PER_LOCATION = 1.95;
	private int getPlayersInThisLocation(int maxPlayers) {
		return Math.min(maxPlayers,
				1+(int)(Math.random()*Math.random()*Math.random()*MAX_PLAYERS_PER_LOCATION));
	}
	
	private String generateHierarchyLevel(String parentId, int currentDepth, int maxPlayers, double maxItemsPerLayerInHeirarchy, MutableInteger generatedPlayers) {
		if (currentDepth == 0) {
			// Generate the location
			String locationId = generateTopology(parentId, IdType.LOCATION);
			int playersInThisLocation = getPlayersInThisLocation(maxPlayers);
			for (int i = 0; i < playersInThisLocation; i++) {
				generatePlayer(locationId);
				generatedPlayers.addAndGet(1);
			}
			return locationId;
		}
		else {
			Random random = ThreadLocalRandom.current();
			int childrenLayers = 1+random.nextInt((int)Math.ceil(maxItemsPerLayerInHeirarchy));
			for (int i = 0; i < childrenLayers; i++) {
				String thisId = generateTopology(parentId, IdType.LOCATION_GROUP);
				this.generateHierarchyLevel(thisId, currentDepth-1, maxPlayers, maxItemsPerLayerInHeirarchy, generatedPlayers);
			}
			return null;
		}
	}
	private String generateHierarchy(int depth, int maxPlayers, MutableInteger generatedPlayers) {
		if (depth < 2) {
			throw new IllegalArgumentException("Heirarchy depth must be at least 2 (USER and LOCATION)");
		}
		if (maxPlayers < 1) {
			throw new IllegalArgumentException("No players specified");
		}
		int remainingPlayers = maxPlayers - generatedPlayers.get();
		if (remainingPlayers <= 0) {
			return "";
		}
		int intermediateLayers = depth - 2;

		// Generate the user
		String userId = generateTopology(null, IdType.USER);

		// Average number of players per location
		double approxMaxLocations = remainingPlayers / AVG_PLAYERS_PER_LOCATION;

		// Work out the number of locations
		double itemsPerLayerInHeirarchy = intermediateLayers > 0 ? Math.pow(approxMaxLocations, (1.0/intermediateLayers)) : 1;
		
		// Set maximum items per layer in the hierarchy
		itemsPerLayerInHeirarchy = Math.min(itemsPerLayerInHeirarchy, MAX_LOCATION_GROUPS_PER_GROUP);
		
		generateHierarchyLevel(userId, intermediateLayers, remainingPlayers, itemsPerLayerInHeirarchy, generatedPlayers);

		return userId;
	}
	
	private int loadData(int numberOfPlayers, boolean truncateTables, int numThreads) {
		if (truncateTables) {
			jdbcTemplate.execute(TRUNCATE_MHHMAP_TABLE);
			jdbcTemplate.execute(TRUNCATE_TOPOLOGY_TABLE);

		}
		ExecutorService executor = Executors.newFixedThreadPool(numThreads);
		AtomicInteger counter = new AtomicInteger();
		int numCreated = 0;
		for (int i = 0; i < numThreads; i++) {
			final int thisThreadsPlayerCount = (numberOfPlayers - numCreated) / (numThreads - i);
			numCreated += thisThreadsPlayerCount;
			executor.submit(new Runnable() {
				@Override
				public void run() {
					SonosWorkload.this.loadDataThreaded(thisThreadsPlayerCount, counter);
				}
			});
		}
		try {
			executor.awaitTermination(1, TimeUnit.DAYS);
		} catch (InterruptedException e) {
		}
		return counter.get();
	}
	
	private int loadDataThreaded(int numberOfPlayers, final AtomicInteger counter) {
		MutableInteger generatedPlayers = new MutableInteger();
		int lastCount = 0;
		while (generatedPlayers.get() < numberOfPlayers) {
			int level = 2;
			int value = ThreadLocalRandom.current().nextInt(100);
			if (0 <= value && value <= 42) {
				level = 2;
			}
			else if (value <= 68) {
				level = 3;
			}
			else if (value <= 83) {
				level = 4;
			}
			else if (value <= 90) {
				level = 5;
			}
			else if (value <= 95) {
				level = 6;
			}
			else if (value <= 98) {
				level = 7;
			}
			else {
				level = 8;
			}
			if (LOGGER.isDebugEnabled()) {
				LOGGER.debug(String.format("\nLevel:%d, total players:%d, generated players:%d\n", level, numberOfPlayers, generatedPlayers.get()));
			}
			this.generateHierarchy(level, numberOfPlayers, generatedPlayers);
			counter.addAndGet(generatedPlayers.value - lastCount);
			lastCount = generatedPlayers.value;
		}
		return generatedPlayers.get();
	}
}
