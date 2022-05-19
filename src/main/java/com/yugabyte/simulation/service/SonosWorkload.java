package com.yugabyte.simulation.service;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowCallbackHandler;
import org.springframework.stereotype.Repository;

import com.yugabyte.simulation.dao.InvocationResult;
import com.yugabyte.simulation.dao.ParamType;
import com.yugabyte.simulation.dao.ParamValue;
import com.yugabyte.simulation.dao.WorkloadDesc;
import com.yugabyte.simulation.dao.WorkloadParamDesc;
import com.yugabyte.simulation.services.ExecutionStatus;
import com.yugabyte.simulation.services.Timer;
import com.yugabyte.simulation.services.TimerService;
import com.yugabyte.simulation.services.TimerType;
import com.yugabyte.simulation.workload.FixedStepsWorkloadType;
import com.yugabyte.simulation.workload.FixedStepsWorkloadType.FixedStepWorkloadInstance;
import com.yugabyte.simulation.workload.ThroughputWorkloadType;
import com.yugabyte.simulation.workload.WorkloadManager;
import com.yugabyte.simulation.workload.ThroughputWorkloadType.ThroughputWorkloadInstance;
import com.yugabyte.simulation.workload.WorkloadSimulationBase;

@Repository
public class SonosWorkload extends WorkloadSimulationBase implements WorkloadSimulation {

	@Autowired
	private JdbcTemplate jdbcTemplate;
	
	@Autowired
	private TimerService timerService;
	
	@Autowired 
	private WorkloadManager workloadManager;
	
	private static final String CREATE_TOPOLOGY_TABLE = 
			"create table if not exists topology (\n"
			+ "	parentid varchar(100),\n"
			+ "	Id varchar(100),\n"
			+ "	IdType varchar(100) not null,\n"
			+ "	IdName varchar(100),\n"
			+ " children int,\n"
			+ " depth int,\n"
			+ "	created timestamp default now(),\n"
			+ "	updated timestamp default now(),\n"
			+ "	constraint topology_prikey PRIMARY KEY(id,parentid)\n"
			+ ") split into 48 tablets;";
	
	private static final String CREATE_TOPOLOGY_INDEX = 
			"Create unique index if not exists topology_idx2 "
			+ "on topology(parentid, idtype desc, id asc);";

	private static final String DROP_TOPOLOGY_TABLE = 
			"drop table if exists topology;";
	
	private static final String TRUNCATE_TOPOLOGY_TABLE = "truncate topology";
			
	private static final String CREATE_MHHMAP_TABLE = 
			"create table if not exists mhhmap (\n"
			+ "	sonosId varchar(100),\n"
			+ "	mHHId varchar(100),\n"
			+ "	locId varchar(100),\n"
			+ "	status varchar(100),\n"
			+ "	splitReason varchar(100),\n"
			+ "	created timestamp without time zone default now(),\n"
			+ "        updated timestamp without time zone default now(),\n"
			+ "	constraint mhhmap_prikey PRIMARY KEY(locId,mHHId,sonosId)\n"
			+ ") split into 48 tablets;";
	private static final String DROP_MHHMAP_TABLE = 
			"drop table if exists mhhmap;";

	private static final String TRUNCATE_MHHMAP_TABLE = "truncate mhhmap";

	private static final String INSERT_TOPOLOGY_TABLE = 
			"insert into topology("
			+ "parentid,"
			+ "Id,"
			+ "IdType,"
			+ "IdName,"
			+ "children,"
			+ "depth)"
			+ "values ("
			+ "?,"
			+ "?,"
			+ "?,"
			+ "?,"
			+ "?,"
			+ "?"
			+ ");";
	
	private static final String INSERT_MHHMAP_TABLE = 
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
	
	private static final String TOP_DOWN_QUERY = 
			"/*+ Set(enable_hashjoin off) Set(enable_mergejoin off) Set(enable_seqscan off) IndexScan(t topology_idx2) */ WITH RECURSIVE locs AS ("
			+ "	SELECT"
			+ "		id,idtype,idname,parentid,children,depth"
			+ "	FROM"
			+ "		topology"
			+ "	WHERE"
			+ "		id = ?"
			+ "	UNION"
			+ "		SELECT"
			+ "			t.id,"
			+ "			t.idtype,"
			+ "			t.idname,"
			+ "			t.parentid,"
			+ "			t.children,"
			+ "			t.depth"
			+ "		FROM"
			+ "			 topology t"
			+ "		INNER JOIN locs l ON l.id = t.parentid"
			+ ") SELECT"
			+ "	id,idtype,idname,parentid,children,depth "
			+ " FROM "
			+ "	locs;";
	
	private static final String BOTTOM_UP_QUERY = 
			"/*+ Set(enable_hashjoin off) Set(enable_mergejoin off) Set(enable_seqscan off) IndexScan(t topology_idx2) */ WITH RECURSIVE locs AS ("
			+ " SELECT"
			+ "     id,idtype,idname,parentid,children,depth"
			+ " FROM"
			+ "     topology"
			+ " WHERE"
			+ "     id = ?"
			+ " UNION ALL"
			+ "     SELECT"
			+ "         t.id,"
			+ "         t.idtype,"
			+ "         t.idname,"
			+ "		    t.parentid,"
			+ "			t.children,"
			+ "			t.depth"
			+ "     FROM"
			+ "         topology t,locs l"
			+ "     where l.parentid=t.id"
			+ ") SELECT"
			+ "        id,idtype,idname,parentid,children,depth"
			+ " FROM"
			+ "        locs;";
	
	private static final String READ_TOPOLOGY = 
			"   SELECT"
			+ "    id,idtype,idname,parentid,children,depth"
			+ " FROM"
			+ "    topology"
			+ " WHERE"
			+ "    id = ?";
		
	private static final String READ_ONLY_TRANSACTION = "SET TRANSACTION READ ONLY;";
	private static final String FOLLOWER_READS = "SET yb_read_from_followers = TRUE;";
	
	private static final String BOTTOM_UP_FOLLOWER_READ = 
			FOLLOWER_READS + READ_ONLY_TRANSACTION + BOTTOM_UP_QUERY;
	
	private static final String TOP_DOWN_FOLLOWER_READ = 
			FOLLOWER_READS + READ_ONLY_TRANSACTION + TOP_DOWN_QUERY;
	
	private static final String READ_TOPOLOGY_FOLLOWER_READ = 
			FOLLOWER_READS + READ_ONLY_TRANSACTION + READ_TOPOLOGY;

	private static final String BACKFILL_TRANSACTION =
			"BEGIN TRANSACTION;" +
					INSERT_TOPOLOGY_TABLE + 
					INSERT_TOPOLOGY_TABLE + 
					INSERT_MHHMAP_TABLE +
			"END TRANSACTION;";
	
	private enum WorkloadType {
		CREATE_TABLES, 
		LOAD_DATA,
		RUN_SIMULATION,
		RUN_TOP_DOWN_QUERY,
		RUN_BOTTOM_UP_QUERY
	}		
	
	private enum IdType {
		USER,
		LOCATION_GROUP,
		LOCATION
	}
	
	private static final String DROP_MHHMAP_STEP = "Drop MhhMap";
	private static final String DROP_TOPOLOGY_STEP = "Drop Topology";
	private static final String CREATE_MHHMAP_STEP = "Create MhhMap";
	private static final String CREATE_TOPOLOGY_STEP = "Create Topology";
	
    private static final Logger LOGGER = LoggerFactory.getLogger(SonosWorkload.class);

//	private static final double MAX_MHHMAP_PER_LOCATION = 10.0;
	private static final double MAX_LOCATION_GROUPS_PER_GROUP = 15.0;
	private static final double AVG_MHHMAP_PER_LOCATION = 6;
	
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
		public int get() {
			return this.value;
		}
		@Override
		public String toString() {
			return Integer.toString(this.value);
		}
	}

	private final FixedStepsWorkloadType createTablesWorkloadType;
	private final FixedStepsWorkloadType createTablesWithTruncateWorkloadType;
	private final ThroughputWorkloadType runInstanceType;
	
	public SonosWorkload() {
		this.createTablesWorkloadType = new FixedStepsWorkloadType(
				CREATE_MHHMAP_STEP,
				CREATE_TOPOLOGY_STEP);
		
		this.createTablesWithTruncateWorkloadType = new FixedStepsWorkloadType(
				DROP_MHHMAP_STEP,
				DROP_TOPOLOGY_STEP,
				CREATE_MHHMAP_STEP,
				CREATE_TOPOLOGY_STEP);
		
		this.runInstanceType = new ThroughputWorkloadType();
	}
	
	private WorkloadDesc createTablesWorkload = new WorkloadDesc(
			WorkloadType.CREATE_TABLES.toString(),
			"Create Tables", 
			"Create the simulation tables. If the tables are already created they will not be re-created unless 'force' is set to true",
			new WorkloadParamDesc("force", ParamType.BOOLEAN, false, new ParamValue(false))
		);
	
	private WorkloadDesc loadDataWorkload =  new WorkloadDesc(
					WorkloadType.LOAD_DATA.toString(),
					"Load Data",
					"Generate test data into the database",
					new WorkloadParamDesc("Number of locations", true, 1, Integer.MAX_VALUE, 1000),
					new WorkloadParamDesc("Truncate tables", false, false),
					new WorkloadParamDesc("Number of threads", true, 1, 1024, 32)
				)
				.nameWorkload(TimerType.WORKLOAD2, "MhhMap")
				.nameWorkload(TimerType.WORKLOAD1, "Topology");
	
	private WorkloadDesc runningWorkload = new WorkloadDesc(
			WorkloadType.RUN_SIMULATION.toString(),
			"Simulation",
			"Run a simulation of the day-to-day activities of Sonos. This includes adding locations and looking at component hierarchies",
			new WorkloadParamDesc("Throughput (tps)", true, 1, 1000000, 500),
			new WorkloadParamDesc("Backfill ratio", true, 0, 100, 10),
			new WorkloadParamDesc("Top down read ratio", true, 0, 100, 10),
			new WorkloadParamDesc("Bottom up read ratio", true, 0, 100, 10),
			new WorkloadParamDesc("Point read ratio", true, 0, 100, 10),
			new WorkloadParamDesc("Use local reads", true, false)
			)
			.nameWorkload(TimerType.WORKLOAD1, "Inserts")
			.nameWorkload(TimerType.WORKLOAD2, "Hierarchy");
	
	private WorkloadDesc runTopDownQuery = new WorkloadDesc(
			WorkloadType.RUN_TOP_DOWN_QUERY.toString(),
			"Top Down Query",
			"Run a top down query with the given UUID. The results will be displayed in the server console",
			new WorkloadParamDesc("UUID", ParamType.STRING, true));

	private WorkloadDesc runBottomUpQuery = new WorkloadDesc(
			WorkloadType.RUN_BOTTOM_UP_QUERY.toString(),
			"Bottom Up Query",
			"Run a bottom up query with the given UUID. The results will be displayed in the server console",
			new WorkloadParamDesc("UUID", ParamType.STRING, true));

	@Override
	public List<WorkloadDesc> getWorkloads() {
		return Arrays.asList(
			createTablesWorkload, loadDataWorkload, runningWorkload, runTopDownQuery, runBottomUpQuery
		);
	}

	@Override
	public InvocationResult invokeWorkload(String workloadId, ParamValue[] values) {
		WorkloadType type = WorkloadType.valueOf(workloadId);
		try {
			switch (type) {
			case CREATE_TABLES:
				timerService.setCurrentWorkload(createTablesWorkload);
				this.createTables(values[0].getBoolValue());
				timerService.removeCurrentWorkload(createTablesWorkload);
				return new InvocationResult("Ok");
			
			case LOAD_DATA:
				timerService.setCurrentWorkload(loadDataWorkload);
				this.loadData(values[0].getIntValue(), values[1].getBoolValue(), values[2].getIntValue());
				timerService.removeCurrentWorkload(loadDataWorkload);
				return new InvocationResult("Ok");
				
			case RUN_SIMULATION:
				this.runSimulation(
						values[0].getIntValue(), 
						values[1].getIntValue(), 
						values[2].getIntValue(),
						values[3].getIntValue(),
						values[4].getIntValue(),
						values[5].getBoolValue()
					);
				return new InvocationResult("Ok");
				
			case RUN_TOP_DOWN_QUERY:
				this.runTopDownQuery(values[0].getStringValue(), false);
				return new InvocationResult("Ok");
				
			case RUN_BOTTOM_UP_QUERY:
				this.runBottomUpQuery(values[0].getStringValue(), false);
				return new InvocationResult("Ok");
				
			}
			throw new IllegalArgumentException("Unknown workload "+ workloadId);
		}
		catch (Exception e) {
			return new InvocationResult(e);
		}
	}
	
	private void runTopDownQuery(String uuid, boolean followerReads) {
		String query = followerReads ? TOP_DOWN_FOLLOWER_READ : TOP_DOWN_QUERY;	
		jdbcTemplate.query(TOP_DOWN_QUERY, new Object[] {uuid}, new int[] {Types.VARCHAR},
				new RowCallbackHandler() {
					
					@Override
					public void processRow(ResultSet rs) throws SQLException {
						if (LOGGER.isDebugEnabled()) {
							LOGGER.debug(String.format(
								"id='%s', idtype='%s', idname='%s', parentid='%s', children=%d, depth=%d\n", 
								rs.getString("id"),
								rs.getString("idtype"),
								rs.getString("idname"),
								rs.getString("parentid"),
								rs.getInt("children"),
								rs.getInt("depth")));
						}
					}
				});
	 }
	
	private void runBottomUpQuery(String uuid, boolean followerReads) {
		String query = followerReads ? BOTTOM_UP_FOLLOWER_READ : BOTTOM_UP_QUERY;
		
		jdbcTemplate.query(query, new Object[] {uuid}, new int[] {Types.VARCHAR},
				new RowCallbackHandler() {
					
					@Override
					public void processRow(ResultSet rs) throws SQLException {
						if (LOGGER.isDebugEnabled()) {
							LOGGER.debug(String.format(
									"id='%s', idtype='%s', idname='%s', parentid='%s', children=%d, depth=%d\n", 
								rs.getString("id"),
								rs.getString("idtype"),
								rs.getString("idname"),
								rs.getString("parentid"),
								rs.getInt("children"),
								rs.getInt("depth")));
						}
					}
				});
	 }
	
	private void runPointRead(String uuid, boolean followerReads) {
		String query = followerReads ? READ_TOPOLOGY_FOLLOWER_READ : READ_TOPOLOGY;
		
		long now = System.currentTimeMillis();
		jdbcTemplate.query(query, new Object[] {uuid}, new int[] {Types.VARCHAR},
				new RowCallbackHandler() {
					
					@Override
					public void processRow(ResultSet rs) throws SQLException {
						if (LOGGER.isDebugEnabled()) {
							LOGGER.debug(String.format(
									"id='%s', idtype='%s', idname='%s', parentid='%s', children=%d, depth=%d\n", 
								rs.getString("id"),
								rs.getString("idtype"),
								rs.getString("idname"),
								rs.getString("parentid"),
								rs.getInt("children"),
								rs.getInt("depth")));
						}
					}
				});
		
		System.out.printf("TOP DOWN query (%s) ran in %dms\n", uuid, System.currentTimeMillis() - now);
	}
	
	private void createTables(boolean force) {
		FixedStepsWorkloadType jobType = force ? createTablesWithTruncateWorkloadType : createTablesWorkloadType;
		FixedStepWorkloadInstance workload = jobType.createInstance(timerService);
		workloadManager.registerWorkloadInstance(workload);
		workload.execute((stepNum, stepName) -> {
			switch (stepName) {
			case DROP_MHHMAP_STEP:
				jdbcTemplate.execute(DROP_MHHMAP_TABLE);
				break;
			case DROP_TOPOLOGY_STEP:
				jdbcTemplate.execute(DROP_TOPOLOGY_TABLE);
				break;
			case CREATE_MHHMAP_STEP:
				jdbcTemplate.execute(CREATE_MHHMAP_TABLE);
				break;
			case CREATE_TOPOLOGY_STEP:
				jdbcTemplate.execute(CREATE_TOPOLOGY_TABLE + CREATE_TOPOLOGY_INDEX);
				break;
			}
		});
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
	
	private String generateUUID() {
		return LoadGeneratorUtils.getUUID().toString();
	}
	
	private String generateMhhMap(String userId, String locId) {
		String sonosId = userId;
		String mhhid = "Sonos_" + LoadGeneratorUtils.getUUID().toString() + "." 
				+ LoadGeneratorUtils.getFixedLengthNumber(10);
		String status = "ACTIVE";
		String splitReason = "UNSPLIT";

		Object[] data = new Object[] {sonosId, mhhid, locId, status, splitReason};
		logCall("generateMhhmap", data);
		
		Timer timer = timerService.getTimer(TimerType.WORKLOAD2).start();
		jdbcTemplate.update(INSERT_MHHMAP_TABLE, data);
		timer.end(ExecutionStatus.SUCCESS);
		return sonosId;

	}
	/**
	 * Generate a new topology with the given parameters.
	 * @param parentId. If this is null, the type will be forced to USER
	 * @param type 
	 * @return the id for this generated topology
	 */
	private String generateTopology(String thisId, String parentId, IdType type, int children, int depth) {
		// If there is no parent, it has to be the user level
		if (parentId == null) {
			type = IdType.USER;
		}
		String idName = LoadGeneratorUtils.getName() + " " + LoadGeneratorUtils.getName();
		
		Object[] data = new Object[] {parentId == null? "null" : parentId, thisId, type.toString(), idName, children, depth};
		logCall("generateTopology", data);

		Timer timer = timerService.getTimer(TimerType.WORKLOAD1).start();
		jdbcTemplate.update(INSERT_TOPOLOGY_TABLE, data);
		timer.end(ExecutionStatus.SUCCESS);
		return thisId;
	}
	
	
//	private int getPlayersInThisLocation(int maxPlayers) {
//		return Math.min(maxPlayers,
//				1+(int)(Math.random()*Math.random()*Math.random()*MAX_MHHMAP_PER_LOCATION));
//	}
	
	private int generateChildrenForHierarchyLevel(String userId, String parentId, 
			int currentDepth, int maxPlayers, double 
			maxItemsPerLayerInHeirarchy, MutableInteger generatedMhhMaps) {
		
		Random random = ThreadLocalRandom.current();
		int childrenAtThisLayer = 1+random.nextInt((int)Math.ceil(maxItemsPerLayerInHeirarchy));
		int childrenCount = 0;
		for (int i = 0; i < childrenAtThisLayer; i++) {
			String thisChildId = generateUUID();
			if (currentDepth == 0) {
				// Generate the location
				String locationId = generateTopology(generateUUID(), parentId, IdType.LOCATION, 0, 1);
				
				// After discussions with Sonos, there is typically 1 MhhMap per location.
				generateMhhMap(userId, locationId);
				generatedMhhMaps.addAndGet(1);
				childrenCount++;
			}
			else {
				int thisChildCount = this.generateChildrenForHierarchyLevel(userId, thisChildId, currentDepth-1, maxPlayers, maxItemsPerLayerInHeirarchy, generatedMhhMaps);
				generateTopology(thisChildId, parentId, IdType.LOCATION_GROUP, thisChildCount, currentDepth-1);
				childrenCount += thisChildCount;
			}
		}
		return childrenCount;
	}
	
	private String generateHierarchy(int depth, int maxMhhMaps, MutableInteger generatedMhhMaps) {
		if (depth < 2) {
			throw new IllegalArgumentException("Heirarchy depth must be at least 2 (USER and LOCATION)");
		}
		if (maxMhhMaps < 1) {
			throw new IllegalArgumentException("No mhhmaps specified");
		}
		int remainingMhhMaps = maxMhhMaps - generatedMhhMaps.get();
		if (remainingMhhMaps <= 0) {
			return "";
		}
		int intermediateLayers = depth - 2;

		// Generate the user
		String userId = generateUUID();

		// Average number of players per location
		double approxMaxLocations = remainingMhhMaps / AVG_MHHMAP_PER_LOCATION;

		// Work out the number of locations
		double itemsPerLayerInHeirarchy = Math.pow(approxMaxLocations, (1.0/(1+intermediateLayers)));
		
		// Set maximum items per layer in the hierarchy
		itemsPerLayerInHeirarchy = Math.min(itemsPerLayerInHeirarchy, MAX_LOCATION_GROUPS_PER_GROUP);
		
		int childrenAtDepth = generateChildrenForHierarchyLevel(userId, userId, intermediateLayers, remainingMhhMaps, itemsPerLayerInHeirarchy, generatedMhhMaps);

		generateTopology(userId, null, IdType.USER, childrenAtDepth, depth);
		return userId;
	}
	
	private int loadData(int numberOfMhhMaps, boolean truncateTables, int numThreads) {
		if (truncateTables) {
			jdbcTemplate.execute(TRUNCATE_MHHMAP_TABLE);
			jdbcTemplate.execute(TRUNCATE_TOPOLOGY_TABLE);

		}
		ExecutorService executor = Executors.newFixedThreadPool(numThreads);
		AtomicInteger counter = new AtomicInteger();
		int numCreated = 0;
		for (int i = 0; i < numThreads; i++) {
			final int thisThreadsMhhMapCount = (numberOfMhhMaps - numCreated) / (numThreads - i);
			numCreated += thisThreadsMhhMapCount;
			executor.submit(new Runnable() {
				@Override
				public void run() {
					SonosWorkload.this.loadDataThreaded(thisThreadsMhhMapCount, counter);
				}
			});
		}
		executor.shutdown();
		try {
			executor.awaitTermination(1, TimeUnit.DAYS);
		} catch (InterruptedException e) {
		}
		return counter.get();
	}
	
	private int loadDataThreaded(int numberOfMhhMaps, final AtomicInteger counter) {
		MutableInteger generatedMhhMaps = new MutableInteger();
		int lastCount = 0;
		while (generatedMhhMaps.get() < numberOfMhhMaps) {
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
				LOGGER.debug(String.format("\nLevel:%d, total MhhMaps:%d, generated MhhMaps:%d\n", level, numberOfMhhMaps, generatedMhhMaps.get()));
			}
			this.generateHierarchy(level, numberOfMhhMaps, generatedMhhMaps);
			counter.addAndGet(generatedMhhMaps.value - lastCount);
			lastCount = generatedMhhMaps.value;
		}
		return generatedMhhMaps.get();
	}
	
	private void performBackfillWrite() {
		String userUuid = generateUUID();
		String locationUuid = generateUUID();

		String mhhid = "Sonos_" + LoadGeneratorUtils.getUUID().toString() + "." 
				+ LoadGeneratorUtils.getFixedLengthNumber(10);

		jdbcTemplate.update(BACKFILL_TRANSACTION, new Object[] {
				"null",
				userUuid,
				IdType.USER.toString(),
				LoadGeneratorUtils.getName() + " " + LoadGeneratorUtils.getName(),
				1,
				2,
				
				userUuid,
				locationUuid,
				IdType.LOCATION.toString(),
				LoadGeneratorUtils.getName() + " " + LoadGeneratorUtils.getName(),
				0,
				1,
				
				userUuid,
				mhhid,
				locationUuid,
				"ACTIVE",
				"UNSPLIT"
		});
	}
	
	
	private List<String> getRandomTopologyList() {
		List<String> results = new ArrayList<String>(2000);
		jdbcTemplate.query("select id from topology limit 2000",
			new RowCallbackHandler() {
			
				@Override
				public void processRow(ResultSet rs) throws SQLException {
					String value = rs.getString(1);
					results.add(value);
				}
		});
		return results;
	}
	
	private List<String> getUserList() {
		List<String> results = new ArrayList<String>(2000);
		jdbcTemplate.query("select id from topology where parentid = 'null' limit 2000",
			new RowCallbackHandler() {
			
				@Override
				public void processRow(ResultSet rs) throws SQLException {
					String value = rs.getString(1);
					results.add(value);
				}
		});
		return results;
	}
	
	private List<String> getLocationList() {
		List<String> results = new ArrayList<String>(2000);
		jdbcTemplate.query("select id from topology where idtype = 'LOCATION' limit 2000",
			new RowCallbackHandler() {
			
				@Override
				public void processRow(ResultSet rs) throws SQLException {
					String value = rs.getString(1);
					results.add(value);
				}
		});
		return results;
	}
	
	
	private void runSimulation(
			final int tps, 
			final int percentageBackfills, 
			final int percentageTopDownReads,
			final int percentageBottomUpReads, 
			final int percentagePointReads, 
			final boolean localReads) {
		
		System.out.println("**** Preloading data...");
		final List<String> users = getUserList();
		final List<String> locations = getLocationList();
		final List<String> topology = getRandomTopologyList();
		System.out.println("**** Preload of data done");
		
		final int totalCount = percentageBackfills + percentageTopDownReads + percentageBottomUpReads + percentagePointReads; 
		ThroughputWorkloadInstance instance = runInstanceType.createInstance(timerService);
		workloadManager.registerWorkloadInstance(instance);
		instance.execute(tps, (customData, threadData) -> {
			Random random = ThreadLocalRandom.current();
			int value = ThreadLocalRandom.current().nextInt(totalCount);
			if (value < percentageBackfills) {
				performBackfillWrite();
			}
			else if (value < percentageBackfills + percentageTopDownReads) {
				runTopDownQuery(users.get(random.nextInt(users.size())), localReads);
			}
			else if (value < percentageBackfills + percentageTopDownReads + percentageBottomUpReads) {
				runBottomUpQuery(locations.get(random.nextInt(locations.size())), localReads);
			}
			else {
				runPointRead(topology.get(random.nextInt(topology.size())), localReads);
			}
			
			return null;
		});
		
		
//		System.out.println("**** Setting desired rate to 1200");
//		instance.setDesiredRate(1200);
//
//		System.out.println("**** Setting Desired Rate to 200");
//		instance.setDesiredRate(200);
//
//		System.out.println("**** Terminating Instance");
//		instance.terminate();
	}
}
