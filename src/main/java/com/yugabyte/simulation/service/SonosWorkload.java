package com.yugabyte.simulation.service;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
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
import org.springframework.jdbc.core.RowCallbackHandler;
import org.springframework.jdbc.datasource.DataSourceUtils;

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
import com.yugabyte.simulation.workload.ThroughputWorkloadType.ThroughputWorkloadInstance;
import com.yugabyte.simulation.workload.WorkloadManager;
import com.yugabyte.simulation.workload.WorkloadSimulationBase;

//@Repository
public class SonosWorkload extends WorkloadSimulationBase implements WorkloadSimulation {

	@Autowired
	private JdbcTemplate jdbcTemplate;
	
	@Autowired
	private TimerService timerService;
	
	@Autowired 
	private WorkloadManager workloadManager;
	
	private static final UUID NULL_UUID = UUID.fromString("00000000-0000-0000-0000-000000000000");
	private static final String CREATE_TOPOLOGY_TABLE = 
			"create table if not exists topology (\n"
			+ "	parentid uuid,\n"
			+ "	Id uuid,\n"
			+ " UserId uuid,\n"
			+ "	IdType varchar(20) not null,\n"
			+ "	IdName varchar(100),\n"
			+ " children int,\n"
			+ " depth int,\n"
			+ "	created timestamp default now(),\n"
			+ "	updated timestamp default now(),\n"
			+ "	constraint topology_prikey PRIMARY KEY(id,idtype,parentid)\n"
			+ ") split into 48 tablets;";
	
	private static final String CREATE_TOPOLOGY_INDEX = 
			"Create unique index if not exists topology_idx2 "
			+ "on topology(userid, parentid asc, idtype asc, id asc) include "
			+ "(idname, children, depth) WHERE parentid <> '00000000-0000-0000-0000-000000000000'::uuid "
			+ "AND idtype::text = 'LOCATION_GROUP'::text;";
	private static final String DROP_TOPOLOGY_INDEX = "drop index if exists topology_idx2";

    private static final String CREATE_TOPOLOGY_INDEX2 =
            "Create unique index if not exists topology_idx3 "
            + "on topology(userid, parentid asc, idtype asc, id asc) include "
            + "(idname, children, depth) WHERE parentid <> '00000000-0000-0000-0000-000000000000'::uuid "
            + "AND idtype::text = 'LOCATION'::text;";
	private static final String DROP_TOPOLOGY_INDEX2 = "drop index if exists topology_idx3";
    
	private static final String CREATE_TOPOLOGY_INDEX_NO_USERID = 
			"Create unique index if not exists topology_idx4 "
			+ "on topology(parentid, idtype asc, id asc) include "
			+ "(idname, children, depth) WHERE parentid <> '00000000-0000-0000-0000-000000000000'::uuid "
			+ "AND idtype::text = 'LOCATION_GROUP'::text;";
	private static final String DROP_TOPOLOGY_INDEX_NO_USERID = "drop index if exists topology_idx4";

    private static final String CREATE_TOPOLOGY_INDEX2_NO_USERID =
            "Create unique index if not exists topology_idx5 "
            + "on topology(parentid, idtype asc, id asc) include "
            + "(idname, children, depth) WHERE parentid <> '00000000-0000-0000-0000-000000000000'::uuid "
            + "AND idtype::text = 'LOCATION'::text;";
	private static final String DROP_TOPOLOGY_INDEX2_NO_USERID = "drop index if exists topology_idx5";
    
    private static final String CREATE_TOPOLOGY_USER_INDEX = 
			"Create unique index if not exists topology_user_idx "
			+ "on topology(userid, id asc) include "
			+ "(idname, parentid, id, idtype, children, depth);";
	private static final String DROP_TOPOLOGY_USER_INDEX = "drop index if exists topology_user_idx";
    
    
    private static final String DROP_TOPOLOGY_TABLE = 
			"drop table if exists topology cascade;";
	
	private static final String TRUNCATE_TOPOLOGY_TABLE = "truncate topology";
			
	private static final String CREATE_MHHMAP_TABLE = 
			"create table if not exists mhhmap (\n"
			+ "	sonosId uuid,\n"
			+ "	mHHId varchar(100),\n"
			+ "	locId uuid,\n"
			+ "	status varchar(20),\n"
			+ "	splitReason varchar(20),\n"
			+ "	created timestamp without time zone default now(),\n"
			+ "        updated timestamp without time zone default now(),\n"
			+ "	constraint mhhmap_prikey PRIMARY KEY(locId,mHHId,sonosId)\n"
			+ ") split into 48 tablets;";
	private static final String DROP_MHHMAP_TABLE = 
			"drop table if exists mhhmap cascade;";

	private static final String TRUNCATE_MHHMAP_TABLE = "truncate mhhmap";

	private static final String INSERT_TOPOLOGY_TABLE = 
			"insert into topology("
			+ "userid,"
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
	
	private static final String TOP_DOWN_QUERY_RECURSIVE_NO_USERID = 
			"/*+ Set(enable_hashjoin off) Set(enable_mergejoin off) Set(enable_seqscan off) Set(transaction_read_only on) IndexOnlyScan(t topology_idx4) IndexOnlyScan(t1 topology_idx5) */\n"
			+ "	WITH RECURSIVE locs AS (\n"
			+ "		    SELECT\n"
			+ "		        id,idtype,idname,parentid,children,depth"
			+ "		    FROM\n"
			+ "		        topology\n"
			+ "		    WHERE\n"
			+ "		        id = ?::uuid"
			+ "		    UNION ALL\n"
			+ "		        SELECT\n"
			+ "		            t.id,\n"
			+ "		            t.idtype,\n"
			+ "		            t.idname,\n"
			+ "		            t.parentid,\n"
			+ "		            t.children,\n"
			+ "		            t.depth\n"
			+ "		        FROM\n"
			+ "		                topology t\n"
			+ "		        INNER JOIN locs l ON t.parentid = l.id\n"
			+ "		        WHERE t.idtype = 'LOCATION_GROUP' AND t.parentid <> '00000000-0000-0000-0000-000000000000'::uuid\n"
			+ "		) SELECT\n"
			+ "		    id,idtype,idname,parentid,children,depth\n"
			+ "		    FROM\n"
			+ "		    locs l\n"
			+ "		  UNION ALL\n"
			+ "		  SELECT t1.id, t1.idtype, t1.idname, t1.parentid, t1.children, t1.depth\n"
			+ "		  FROM topology t1 INNER JOIN locs l ON t1.parentid = l.id WHERE t1.idtype = 'LOCATION' AND t1.parentid <> '00000000-0000-0000-0000-000000000000'::uuid;\n";

	private static final String TOP_DOWN_QUERY_RECURSIVE = 
			"/*+ Set(enable_hashjoin off) Set(enable_mergejoin off) Set(enable_seqscan off) Set(transaction_read_only on) IndexOnlyScan(t topology_idx2) IndexOnlyScan(t1 topology_idx3) */\n"
			+ "	WITH RECURSIVE locs AS (\n"
			+ "		    SELECT\n"
			+ "		        userid,id,idtype,idname,parentid,children,depth"
			+ "		    FROM\n"
			+ "		        topology\n"
			+ "		    WHERE\n"
			+ "		        id = ?::uuid"
			+ "		    UNION ALL\n"
			+ "		        SELECT\n"
			+ "                 t.userId,\n"
			+ "		            t.id,\n"
			+ "		            t.idtype,\n"
			+ "		            t.idname,\n"
			+ "		            t.parentid,\n"
			+ "		            t.children,\n"
			+ "		            t.depth\n"
			+ "		        FROM topology t\n"
			+ "		        INNER JOIN locs l ON t.userid = l.userid and t.parentid = l.id\n"
			+ "		        WHERE t.idtype = 'LOCATION_GROUP' AND t.parentid <> '00000000-0000-0000-0000-000000000000'::uuid\n"
			+ "		) SELECT\n"
			+ "		    userid,id,idtype,idname,parentid,children,depth\n"
			+ "		    FROM\n"
			+ "		    locs l\n"
			+ "		  UNION ALL\n"
			+ "		  SELECT t1.userid, t1.id, t1.idtype, t1.idname, t1.parentid, t1.children, t1.depth\n"
			+ "		  FROM topology t1 INNER JOIN locs l ON t1.userId = l.userId and t1.parentid = l.id WHERE t1.idtype = 'LOCATION' AND t1.parentid <> '00000000-0000-0000-0000-000000000000'::uuid;\n";

	private static final String TOP_DOWN_QUERY = 
			"/*+ Set(transaction_read_only on) IndexScan(t topology_user_idx) */\n"
			+ "select id,idtype,idname,parentid,children,depth from topology t "
			+ "where userid = ?::uuid;";
			
	private static final String BOTTOM_UP_QUERY = 
			"/*+ Set(enable_hashjoin off) Set(enable_mergejoin off) Set(enable_seqscan off) IndexScan(t topology_prikey) Set(transaction_read_only on)*/ WITH RECURSIVE locs AS ("
			+ " SELECT"
			+ "     id,idtype,idname,parentid,children,depth"
			+ " FROM"
			+ "     topology"
			+ " WHERE"
			+ "     id = ?::uuid"
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
			"/*+ Set(transaction_read_only on) */"
			+ "   SELECT"
			+ "    id,idtype,idname,parentid,children,depth"
			+ " FROM"
			+ "    topology"
			+ " WHERE"
			+ "    id = ?::uuid;";
		
	private static final String READ_ONLY_TRANSACTION = "START TRANSACTION READ ONLY;";
	private static final String FOLLOWER_READS = "SET yb_read_from_followers TO true;";
	private static final String COMMIT = "commit;";
	
	private static final String BOTTOM_UP_FOLLOWER_READ = 
			READ_ONLY_TRANSACTION + FOLLOWER_READS + BOTTOM_UP_QUERY + COMMIT;
	
	private static final String TOP_DOWN_FOLLOWER_READ = 
			FOLLOWER_READS + TOP_DOWN_QUERY;
	
	private static final String READ_TOPOLOGY_FOLLOWER_READ = 
			READ_ONLY_TRANSACTION + FOLLOWER_READS + READ_TOPOLOGY + COMMIT;

	private static final String BACKFILL_TRANSACTION =
			"BEGIN TRANSACTION;" +
					INSERT_TOPOLOGY_TABLE + // USER
					INSERT_TOPOLOGY_TABLE + // LOCATION
					INSERT_MHHMAP_TABLE +
			"END TRANSACTION;";
	
	private enum WorkloadType {
		CREATE_TABLES, 
		CREATE_INDEXES,
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
	
	private static final String DROP_INDEX1_STEP = "Drop Index 1";
	private static final String DROP_INDEX2_STEP = "Drop Index 2";
	private static final String DROP_INDEX1_NO_USERID_STEP = "Drop Index 1 (no user ids)";
	private static final String DROP_INDEX2_NO_USERID_STEP = "Drop Index 2 (no user ids)";
	private static final String DROP_TOPOLOGY_USER_INDEX_STEP = "Drop Topology User Index";
	
	private static final String CREATE_INDEX1_STEP = "Create Index 1";
	private static final String CREATE_INDEX2_STEP = "Create Index 2";
	
	private static final String CREATE_INDEX1_NO_USERID_STEP = "Create Index 1 (no user ids)";
	private static final String CREATE_INDEX2_NO_USERID_STEP = "Create Index 2 (no user ids)";
	private static final String CREATE_TOPOLOGY_USER_INDEX_STEP = "Create Topology User Index";
	
    private static final Logger LOGGER = LoggerFactory.getLogger(SonosWorkload.class);

	private static final double MAX_LOCATION_GROUPS_PER_GROUP = 15.0;
	private static final double AVG_MHHMAP_PER_LOCATION = 6;
	
	private static final int ROWS_TO_PRELOAD = 2000;
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
	public static class Statements {
		private Connection conn;
		private PreparedStatement topDownPS;
		public Statements() {
			super();
		}
		public Statements(Connection conn, PreparedStatement topDownPS) {
			this();
			this.conn = conn;
			this.topDownPS = topDownPS;
		}
		public Connection getConn() {
			return conn;
		}
		public PreparedStatement getTopDownPreparedStatement() {
			return topDownPS;
		}
		public void setConn(Connection conn) {
			this.conn = conn;
		}
		public void setTopDownPS(PreparedStatement topDownPS) {
			this.topDownPS = topDownPS;
		}
		@Override
		public String toString() {
			return String.format("{connection: %d, topDownStmt: %d}", System.identityHashCode(conn), System.identityHashCode(topDownPS));
		}
	}

	private final FixedStepsWorkloadType createTablesWorkloadType;
	private final FixedStepsWorkloadType createTablesWithTruncateWorkloadType;
	
	private final FixedStepsWorkloadType createIndexesNoDropNoOld;
	private final FixedStepsWorkloadType createIndexesDropNoOld;
	private final FixedStepsWorkloadType createIndexesNoDropOld;
	private final FixedStepsWorkloadType createIndexesDropOld;

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
		
		this.createIndexesNoDropNoOld = new FixedStepsWorkloadType(
				CREATE_TOPOLOGY_USER_INDEX_STEP,
				CREATE_INDEX1_STEP,
				CREATE_INDEX2_STEP);

		this.createIndexesNoDropOld = new FixedStepsWorkloadType(
				CREATE_TOPOLOGY_USER_INDEX_STEP,
				CREATE_INDEX1_STEP,
				CREATE_INDEX2_STEP,
				CREATE_INDEX1_NO_USERID_STEP,
				CREATE_INDEX2_NO_USERID_STEP);
		
		this.createIndexesDropNoOld = new FixedStepsWorkloadType(
				DROP_INDEX1_STEP,
				DROP_INDEX2_STEP,
				DROP_INDEX1_NO_USERID_STEP,
				DROP_INDEX2_NO_USERID_STEP,
				DROP_TOPOLOGY_USER_INDEX_STEP,
				CREATE_TOPOLOGY_USER_INDEX_STEP,
				CREATE_INDEX1_STEP,
				CREATE_INDEX2_STEP);

		this.createIndexesDropOld = new FixedStepsWorkloadType(
				DROP_INDEX1_STEP,
				DROP_INDEX2_STEP,
				DROP_INDEX1_NO_USERID_STEP,
				DROP_INDEX2_NO_USERID_STEP,
				DROP_TOPOLOGY_USER_INDEX_STEP,
				CREATE_TOPOLOGY_USER_INDEX_STEP,
				CREATE_INDEX1_STEP,
				CREATE_INDEX2_STEP,
				CREATE_INDEX1_NO_USERID_STEP,
				CREATE_INDEX2_NO_USERID_STEP);
		

		this.runInstanceType = new ThroughputWorkloadType();
	}
	
	private WorkloadDesc createTablesWorkload = new WorkloadDesc(
			WorkloadType.CREATE_TABLES.toString(),
			"Create Tables", 
			"Create the simulation tables. If the tables are already created they will not be re-created unless 'force' is set to true",
			new WorkloadParamDesc("force", false, false)
		);
	
	private WorkloadDesc createIndexesWorkload = new WorkloadDesc(
			WorkloadType.CREATE_INDEXES.toString(),
			"Recreate Indexes", 
			"Recreate the indexes. Normally not needed, unless the indexes have changed.",
			new WorkloadParamDesc("drop existing indexes", false, true),
			new WorkloadParamDesc("create older indexes", false, false)
		);
			
	private WorkloadDesc loadDataWorkload =  new WorkloadDesc(
					WorkloadType.LOAD_DATA.toString(),
					"Load Data",
					"Generate test data into the database",
					new WorkloadParamDesc("Number of locations", true, 1, Integer.MAX_VALUE, 1000),
					new WorkloadParamDesc("Truncate tables", false, false),
					new WorkloadParamDesc("Number of threads", true, 1, 1024, 32),
					new WorkloadParamDesc("Fixed size records", false, false)
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
			new WorkloadParamDesc("Use local reads", true, false),
			
			new WorkloadParamDesc("Hierarchy Depth 2", true, true),
			new WorkloadParamDesc("Hierarchy Depth 3", true, true),
			new WorkloadParamDesc("Hierarchy Depth 4", true, true),
			new WorkloadParamDesc("Hierarchy Depth 5", true, true),
			new WorkloadParamDesc("Hierarchy Depth 6", true, true),
			new WorkloadParamDesc("Hierarchy Depth 7", true, true),
			new WorkloadParamDesc("Hierarchy Depth 8", true, true),
			new WorkloadParamDesc("Max Threads", true, 1, 500, 64),
			new WorkloadParamDesc("Use hierarchial query", true, false),
			new WorkloadParamDesc("Use UserIds in hierarchy query", true, true)
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
			createTablesWorkload, createIndexesWorkload, loadDataWorkload, runningWorkload, runTopDownQuery, runBottomUpQuery
		);
	}

	public String formInClause(ParamValue[] values) {
		StringBuffer sb = new StringBuffer();
		boolean firstValue = true;
		for (int i = 6; i <= 12; i++) {
			if (values[i].getBoolValue()) {
				if (firstValue) {
					sb.append("AND depth in (");
				}
				else {
					sb.append(",");
				}
				sb.append(i-4);
				firstValue = false;
			}
		}
		if (firstValue) {
			return "";
		}
		else {
			sb.append(")");
			return sb.toString();
		}
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
			
			case CREATE_INDEXES:
				timerService.setCurrentWorkload(createIndexesWorkload);
				this.createIndexes(values[0].getBoolValue(), values[1].getBoolValue());
				timerService.removeCurrentWorkload(createIndexesWorkload);
				return new InvocationResult("Ok");
			
			case LOAD_DATA:
				timerService.setCurrentWorkload(loadDataWorkload);
				this.loadData(values[0].getIntValue(), values[1].getBoolValue(), values[2].getIntValue(), values[3].getBoolValue());
				timerService.removeCurrentWorkload(loadDataWorkload);
				return new InvocationResult("Ok");
				
			case RUN_SIMULATION:
				this.runSimulation(
						values[0].getIntValue(), 
						values[1].getIntValue(), 
						values[2].getIntValue(),
						values[3].getIntValue(),
						values[4].getIntValue(),
						values[5].getBoolValue(),
						this.formInClause(values),
						values[13].getIntValue(),
						values[14].getBoolValue(),
						values[15].getBoolValue()
					);
				return new InvocationResult("Ok");

			case RUN_TOP_DOWN_QUERY:
				this.runTopDownQuerySingle(UUID.fromString(values[0].getStringValue()), false);
				return new InvocationResult("Ok");
				
			case RUN_BOTTOM_UP_QUERY:
				this.runBottomUpQuerySingle(UUID.fromString(values[0].getStringValue()), false);
				return new InvocationResult("Ok");
				
			}
			throw new IllegalArgumentException("Unknown workload "+ workloadId);
		}
		catch (Exception e) {
			return new InvocationResult(e);
		}
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
				jdbcTemplate.execute(CREATE_TOPOLOGY_TABLE + CREATE_TOPOLOGY_INDEX + CREATE_TOPOLOGY_INDEX2 + CREATE_TOPOLOGY_USER_INDEX);
				break;
			}
		});
	}
	
	private void createIndexes(boolean drop, boolean createOld) {
		FixedStepsWorkloadType jobType = null;
		if (drop) {
			jobType = (createOld ? createIndexesDropOld : createIndexesDropNoOld);
		}
		else {
			jobType = (createOld? createIndexesNoDropOld : createIndexesNoDropNoOld);
		}
		FixedStepWorkloadInstance workload = jobType.createInstance(timerService);
		workloadManager.registerWorkloadInstance(workload);
		workload.execute((stepNum, stepName) -> {
			switch (stepName) {
			case DROP_INDEX1_STEP:
				jdbcTemplate.execute(DROP_TOPOLOGY_INDEX);
				break;
			case DROP_INDEX2_STEP:
				jdbcTemplate.execute(DROP_TOPOLOGY_INDEX2);
				break;
			case DROP_INDEX1_NO_USERID_STEP:
				jdbcTemplate.execute(DROP_TOPOLOGY_INDEX_NO_USERID);
				break;
			case DROP_INDEX2_NO_USERID_STEP:
				jdbcTemplate.execute(DROP_TOPOLOGY_INDEX2_NO_USERID);
				break;
			case DROP_TOPOLOGY_USER_INDEX_STEP:
				jdbcTemplate.execute(DROP_TOPOLOGY_USER_INDEX);
				break;
			case CREATE_INDEX1_STEP:
				jdbcTemplate.execute(CREATE_TOPOLOGY_INDEX);
				break;
			case CREATE_INDEX2_STEP:
				jdbcTemplate.execute(CREATE_TOPOLOGY_INDEX2);
				break;
			case CREATE_INDEX1_NO_USERID_STEP:
				jdbcTemplate.execute(CREATE_TOPOLOGY_INDEX_NO_USERID);
				break;
			case CREATE_INDEX2_NO_USERID_STEP:
				jdbcTemplate.execute(CREATE_TOPOLOGY_INDEX2_NO_USERID);
				break;
			case CREATE_TOPOLOGY_USER_INDEX_STEP:
				jdbcTemplate.execute(CREATE_TOPOLOGY_USER_INDEX);
				break;
			}
		});
	}
	

	private void runTopDownQuery_UserIdHierarchy(UUID uuid, boolean followerReads) {
		String query = TOP_DOWN_QUERY_RECURSIVE;
		final boolean debug = LOGGER.isDebugEnabled();
		RowCallbackHandler handler = 					
				new RowCallbackHandler() {
					@Override
					public void processRow(ResultSet rs) throws SQLException {
						if (debug) {
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
				};
 
		
		if (followerReads) {
			query = query.replace("?", "'" + uuid.toString() + "'");
			jdbcTemplate.query(query,handler);
		}
		else {
			jdbcTemplate.query(query, new Object[] {uuid}, new int[] {Types.VARCHAR}, handler);
		}
	 }

	private void runTopDownQuery_NoUserIdHierarchy(UUID uuid, boolean followerReads) {
		String query = TOP_DOWN_QUERY_RECURSIVE_NO_USERID;
		final boolean debug = LOGGER.isDebugEnabled();
		RowCallbackHandler handler = 					
				new RowCallbackHandler() {
					@Override
					public void processRow(ResultSet rs) throws SQLException {
						if (debug) {
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
				};
 
		
		if (followerReads) {
			query = query.replace("?", "'" + uuid.toString() + "'");
			jdbcTemplate.query(query,handler);
		}
		else {
			jdbcTemplate.query(query, new Object[] {uuid}, new int[] {Types.VARCHAR}, handler);
		}
	 }
	
	private void runTopDownQuery(UUID uuid, boolean followerReads) {
		String query = TOP_DOWN_QUERY;
		final boolean debug = LOGGER.isDebugEnabled();
		RowCallbackHandler handler = 					
				new RowCallbackHandler() {
					@Override
					public void processRow(ResultSet rs) throws SQLException {
						if (debug) {
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
				};
 
		
		if (followerReads) {
			query = query.replace("?", "'" + uuid.toString() + "'");
			jdbcTemplate.query(query,handler);
		}
		else {
			jdbcTemplate.query(query, new Object[] {uuid}, new int[] {Types.VARCHAR}, handler);
		}
	 }
	

	private void runTopDownQuerySingle(UUID uuid, boolean followerReads) {
		long now = System.nanoTime();
//		jdbcTemplate.query(TOP_DOWN_QUERY, new Object[] {uuid}, new int[] {Types.VARCHAR},
		jdbcTemplate.query(TOP_DOWN_QUERY.replace("?", "'" + uuid.toString() + "'"),
				new RowCallbackHandler() {
					
					@Override
					public void processRow(ResultSet rs) throws SQLException {
						System.out.printf(
							"id='%s', idtype='%s', idname='%s', parentid='%s', children=%d, depth=%d\n", 
							rs.getString("id"),
							rs.getString("idtype"),
							rs.getString("idname"),
							rs.getString("parentid"),
							rs.getInt("children"),
							rs.getInt("depth"));
					}
				});
		System.out.printf("Results for top down query from %s fetched in %fms\n", uuid.toString(), (System.nanoTime() - now) / 1_000_000.0);

		now = System.nanoTime();
		jdbcTemplate.query("Select id, idtype, idname, parentid, children, depth from topology t where "
				+ "parentid = '00000000-0000-0000-0000-000000000000' and idtype = 'LOCATION'",
				new RowCallbackHandler() {
					
					@Override
					public void processRow(ResultSet rs) throws SQLException {
//						System.out.printf(
//							"id='%s', idtype='%s', idname='%s', parentid='%s', children=%d, depth=%d\n", 
//							rs.getString("id"),
//							rs.getString("idtype"),
//							rs.getString("idname"),
//							rs.getString("parentid"),
//							rs.getInt("children"),
//							rs.getInt("depth"));
					}
				});
		System.out.printf("Results for top down FIXED query from %s fetched in %fms\n", uuid.toString(), (System.nanoTime() - now) / 1_000_000.0);
		

		Connection connection = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			connection = DataSourceUtils.getConnection(jdbcTemplate.getDataSource());
//			ps = connection.prepareStatement("SET pg_hint_plan.enable_hint=ON;SET pg_hint_plan.debug_print=detailed;SET pg_hint_plan.message_level=debug;EXPLAIN " + TOP_DOWN_QUERY.replace("?", "'"+uuid+"'"));
			ps = connection.prepareStatement("SET pg_hint_plan.message_level TO debug;EXPLAIN ANALYZE " + TOP_DOWN_QUERY.replace("?", "'" + uuid.toString() + "'"));

			boolean results = ps.execute();
			int rsCount = 0;
	        // Loop through the available result sets.
	        do {
	            if (results) {
	                rs = ps.getResultSet();
	                rsCount++;

	                // Show data from the result set.
	                System.out.println("RESULT SET #" + rsCount);
	                while (rs.next()) {
	                    System.out.println(rs.getString(1));
	                }
	            }
	            else {
	            	System.out.println("Update count = " + ps.getUpdateCount());
	            }
	            System.out.println();
	            results = ps.getMoreResults();
	        } while (results);
		}
		catch (Exception e) {
			System.out.println("Explain terminated by exception " + e.getClass() + ": " + e.getMessage());
			e.printStackTrace();
		}
		finally {
			try {
				rs.close();
				ps.close();
				DataSourceUtils.releaseConnection(connection, jdbcTemplate.getDataSource());
			}
			catch (Exception e) {};
		}
	 }


	private void runBottomUpQuerySingle(UUID uuid, boolean followerReads) {
		long now = System.nanoTime();
		jdbcTemplate.query(BOTTOM_UP_QUERY, new Object[] {uuid}, new int[] {Types.VARCHAR},
				new RowCallbackHandler() {
					
					@Override
					public void processRow(ResultSet rs) throws SQLException {
						System.out.printf(
									"id='%s', idtype='%s', idname='%s', parentid='%s', children=%d, depth=%d\n", 
								((UUID)rs.getObject("id")).toString(),
								rs.getString("idtype"),
								rs.getString("idname"),
								((UUID)rs.getObject("parentid")).toString(),
								rs.getInt("children"),
								rs.getInt("depth"));
					}
				});
		System.out.printf("Results for bottom up query from %s fetched in %fms\n", uuid.toString(), (System.nanoTime() - now) / 1_000_000.0);
	 }
	
	private void runBottomUpQuery(UUID uuid, boolean followerReads) {
		// String query = followerReads ? BOTTOM_UP_FOLLOWER_READ : BOTTOM_UP_QUERY;
//		String query = BOTTOM_UP_QUERY.replace("?", "'" + uuid + "'");

		final boolean debug = LOGGER.isDebugEnabled();
		RowCallbackHandler handler = 
			new RowCallbackHandler() {
				
				@Override
				public void processRow(ResultSet rs) throws SQLException {
					if (debug) {
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
			};
 
		if (followerReads) {
			String query = BOTTOM_UP_QUERY.replace("?", "'" + uuid.toString() + "'");
			jdbcTemplate.query(query,handler);
		}
		else {
			String query = BOTTOM_UP_QUERY;
			jdbcTemplate.query(query, new Object[] {UUID.fromString(uuid.toString())}, new int[] {Types.VARCHAR}, handler);
		}
	 }
	
	private void runPointRead(UUID uuid, boolean followerReads) {

		if (followerReads) {
			String query = READ_TOPOLOGY_FOLLOWER_READ.replace("?", "'" + uuid.toString() + "'");
			jdbcTemplate.execute(query);
		}
		else {
			String query = followerReads ? READ_TOPOLOGY_FOLLOWER_READ : READ_TOPOLOGY;
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
	}

	private void performBackfillWrite() {
		UUID userUuid = generateUUID();
		UUID locationUuid = generateUUID();

		String mhhid = "Sonos_" + LoadGeneratorUtils.getUUID().toString() + "." 
				+ LoadGeneratorUtils.getFixedLengthNumber(10);

		jdbcTemplate.update(BACKFILL_TRANSACTION, new Object[] {
				userUuid,
				NULL_UUID,
				userUuid,
				IdType.USER.toString(),
				LoadGeneratorUtils.getName() + " " + LoadGeneratorUtils.getName(),
				1,
				2,
				
				userUuid,
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
	
	private UUID generateUUID() {
		return LoadGeneratorUtils.getUUID();
	}
	
	private UUID generateMhhMap(UUID userId, UUID locId) {
		UUID sonosId = userId;
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
	private UUID generateTopology(UUID userId, UUID thisId, UUID parentId, IdType type, int children, int depth) {
		// If there is no parent, it has to be the user level
		if (parentId == null) {
			type = IdType.USER;
		}
		String idName = LoadGeneratorUtils.getName() + " " + LoadGeneratorUtils.getName();
		
		Object[] data = new Object[] {userId, parentId == null? NULL_UUID : parentId, thisId, type.toString(), idName, children, depth};
		logCall("generateTopology", data);

		Timer timer = timerService.getTimer(TimerType.WORKLOAD1).start();
		jdbcTemplate.update(INSERT_TOPOLOGY_TABLE, data);
		timer.end(ExecutionStatus.SUCCESS);
		return thisId;
	}
	
	private int generateChildrenForHierarchyLevel(UUID userId, UUID parentId, 
			int currentDepth, int maxPlayers, double 
			maxItemsPerLayerInHeirarchy, MutableInteger generatedMhhMaps, boolean fixedSizeData) {
		
		Random random = ThreadLocalRandom.current();
		int childrenAtThisLayer = fixedSizeData ? (currentDepth == 4 ? 6 : 4) : 1+random.nextInt((int)Math.ceil(maxItemsPerLayerInHeirarchy));
		int childrenCount = 0;
		for (int i = 0; i < childrenAtThisLayer; i++) {
			UUID thisChildId = generateUUID();
			if (currentDepth == 1) {
				// Generate the location
				UUID locationId = generateTopology(userId, generateUUID(), parentId, IdType.LOCATION, 0, 1);
				
				// After discussions with Sonos, there is typically 1 MhhMap per location.
				generateMhhMap(userId, locationId);
				generatedMhhMaps.addAndGet(1);
				childrenCount++;
			}
			else {
				int thisChildCount = this.generateChildrenForHierarchyLevel(userId, thisChildId, currentDepth-1, maxPlayers, maxItemsPerLayerInHeirarchy, generatedMhhMaps, fixedSizeData);
				generateTopology(userId, thisChildId, parentId, IdType.LOCATION_GROUP, thisChildCount, currentDepth);
				childrenCount += thisChildCount;
			}
		}
		return childrenCount;
	}
	
	private UUID generateHierarchy(int depth, int maxMhhMaps, MutableInteger generatedMhhMaps, boolean fixedSizeData) {
		if (depth < 2) {
			throw new IllegalArgumentException("Heirarchy depth must be at least 2 (USER and LOCATION)");
		}
		if (maxMhhMaps < 1) {
			throw new IllegalArgumentException("No mhhmaps specified");
		}
		int remainingMhhMaps = maxMhhMaps - generatedMhhMaps.get();
		if (remainingMhhMaps <= 0) {
			return null;
		}
		int intermediateLayers = depth - 2;

		// Generate the user
		UUID userId = generateUUID();

		// Average number of players per location
		double approxMaxLocations = remainingMhhMaps / AVG_MHHMAP_PER_LOCATION;

		// Work out the number of locations
		double itemsPerLayerInHeirarchy = Math.pow(approxMaxLocations, (1.0/(1+intermediateLayers)));
		
		// Set maximum items per layer in the hierarchy
		itemsPerLayerInHeirarchy = Math.min(itemsPerLayerInHeirarchy, MAX_LOCATION_GROUPS_PER_GROUP);
		
		int childrenAtDepth = generateChildrenForHierarchyLevel(userId, userId, depth-1, remainingMhhMaps, itemsPerLayerInHeirarchy, generatedMhhMaps, fixedSizeData);

		generateTopology(userId, userId, null, IdType.USER, childrenAtDepth, depth);
		return userId;
	}
	
	private int loadData(int numberOfMhhMaps, boolean truncateTables, int numThreads, boolean fixedSizeData) {
		if (truncateTables) {
			jdbcTemplate.execute(TRUNCATE_MHHMAP_TABLE);
			jdbcTemplate.execute(TRUNCATE_TOPOLOGY_TABLE);

		}
		// If we're generating fixed size data, generate a special parent with 1500 children
		if (fixedSizeData) {
			UUID userId = UUID.fromString("00000000-0000-0000-0000-111111111111");
			for (int i = 0; i < 1500; i++) {
				UUID locationId = generateTopology(userId, generateUUID(), userId, IdType.LOCATION, 0, 1);
				generateMhhMap(userId, locationId);

			}
			generateTopology(userId, userId, null, IdType.USER, 1500, 2);
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
					SonosWorkload.this.loadDataThreaded(thisThreadsMhhMapCount, counter, fixedSizeData);
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
	
	private int loadDataThreaded(int numberOfMhhMaps, final AtomicInteger counter, final boolean fixedSizeData) {
		MutableInteger generatedMhhMaps = new MutableInteger();
		int lastCount = 0;
		while (generatedMhhMaps.get() < numberOfMhhMaps) {
			int level = 2;
			int value = fixedSizeData ? 94 : ThreadLocalRandom.current().nextInt(100);
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
			this.generateHierarchy(level, numberOfMhhMaps, generatedMhhMaps, fixedSizeData);
			counter.addAndGet(generatedMhhMaps.value - lastCount);
			lastCount = generatedMhhMaps.value;
		}
		return generatedMhhMaps.get();
	}
	
	private List<UUID> getRandomTopologyList() {
		List<UUID> results = new ArrayList<UUID>(ROWS_TO_PRELOAD);
		jdbcTemplate.query("select id from topology limit " + ROWS_TO_PRELOAD,
			new RowCallbackHandler() {
			
				@Override
				public void processRow(ResultSet rs) throws SQLException {
					UUID value = (UUID)rs.getObject(1);
					results.add(value);
				}
		});
		return results;
	}
	
	private List<UUID> getUserList(String inClause) {
		List<UUID> results = new ArrayList<UUID>(ROWS_TO_PRELOAD);
		jdbcTemplate.query("select id from topology where idtype = 'USER' " + inClause + " limit " + ROWS_TO_PRELOAD,
			new RowCallbackHandler() {
			
				@Override
				public void processRow(ResultSet rs) throws SQLException {
					UUID value = (UUID)rs.getObject(1);
					results.add(value);
				}
		});
		return results;
	}
	
	private List<UUID> getLocationList() {
		List<UUID> results = new ArrayList<UUID>(ROWS_TO_PRELOAD);
		jdbcTemplate.query("select id from topology where idtype = 'LOCATION' limit " + ROWS_TO_PRELOAD,
			new RowCallbackHandler() {
			
				@Override
				public void processRow(ResultSet rs) throws SQLException {
					UUID value = (UUID)rs.getObject(1);
					results.add(value);
				}
		});
		return results;
	}

	List<UUID> locations = null;
	List<UUID> topology = null;

	private void runSimulation(
			final int tps, 
			final int percentageBackfills, 
			final int percentageTopDownReads,
			final int percentageBottomUpReads, 
			final int percentagePointReads, 
			final boolean localReads,
			final String inClause,
			final int maxThreads,
			final boolean useRecursiveQuery,
			final boolean useQueryWithUserIds) {
		
		System.out.println("**** Preloading data...");
		final List<UUID> users = getUserList(inClause);

		if(locations == null && percentageBottomUpReads > 0){
			locations = getLocationList();
		}

		if(topology == null && percentagePointReads > 0){
			topology = getRandomTopologyList();
		}

		jdbcTemplate.setFetchSize(100000);

		System.out.println("**** Preload of data done");
		
		final int totalCount = percentageBackfills + percentageTopDownReads + percentageBottomUpReads + percentagePointReads; 
		ThroughputWorkloadInstance instance = runInstanceType.createInstance(timerService).setMaxThreads(maxThreads);
		workloadManager.registerWorkloadInstance(instance);
		instance
			.execute(tps, (customData, threadData) -> {
				Random random = ThreadLocalRandom.current();
				int value = ThreadLocalRandom.current().nextInt(totalCount);
				if (value < percentageBackfills) {
					performBackfillWrite();
				}
				else if (value < percentageBackfills + percentageTopDownReads) {
					if (useRecursiveQuery) {
						if (useQueryWithUserIds) {
							runTopDownQuery_UserIdHierarchy(users.get(random.nextInt(users.size())), localReads);
						}
						else {
							runTopDownQuery_NoUserIdHierarchy(users.get(random.nextInt(users.size())), localReads);
						}
					}
					else {
						runTopDownQuery(users.get(random.nextInt(users.size())), localReads);
					}
				}
				else if (value < percentageBackfills + percentageTopDownReads + percentageBottomUpReads) {
					runBottomUpQuery(locations.get(random.nextInt(locations.size())), localReads);
				}
				else {
					runPointRead(topology.get(random.nextInt(topology.size())), localReads);
				}
			});
	}
}