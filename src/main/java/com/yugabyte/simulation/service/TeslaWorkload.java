package com.yugabyte.simulation.service;
import com.yugabyte.simulation.dao.InvocationResult;
import com.yugabyte.simulation.dao.ParamValue;
import com.yugabyte.simulation.dao.WorkloadDesc;
import com.yugabyte.simulation.dao.WorkloadParamDesc;
import com.yugabyte.simulation.services.TimerService;
import com.yugabyte.simulation.workload.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowCallbackHandler;
import org.springframework.stereotype.Repository;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

@Repository
public class TeslaWorkload  extends WorkloadSimulationBase implements WorkloadSimulation {

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Autowired
    private TimerService timerService;

    @Autowired
    private WorkloadManager workloadManager;

	@Override
	public String getName() {
		return "Sonos";
	}

    private static final String CREATE_TABLE1 = "create table if not exists table1(\n" +
            "   pkid uuid,\n" +
            "   col1 int,\n" +
            "   col2 int,\n" +
            "   col3 int,\n" +
            "   col4 int,\n" +
            "   col5 numeric,\n" +
            "   col6 numeric,\n" +
            "   col7 numeric,\n" +
            "   col8 timestamp default now(),\n" +
            "   col9 timestamp default now(),\n" +
            "   primary key (pkid)\n" +
            ");";

    private static final String CREATE_TABLE2 = "create table  if not exists table2(\n" +
            "   pkid uuid,\n" +
            "   col1 bytea,\n" +
            "   primary key (pkid)\n" +
            ");";

    private static final String CREATE_TABLE3 = "create table if not exists table3(\n" +
            "   pkid uuid,\n" +
            "   col1 varchar(255),\n" +
            "   col2 bytea,\n" +
            "   primary key (pkid)\n" +
            ");";



    private static final String DROP_TABLE1 = "drop table if exists table1 cascade;";
    private static final String TRUNCATE_TABLE1 = "truncate table1;";
    private static final String DROP_TABLE2 = "drop table if exists table2 cascade;";
    private static final String TRUNCATE_TABLE2 = "truncate table2;";
    private static final String DROP_TABLE3 = "drop table if exists table3 cascade;";
    private static final String TRUNCATE_TABLE3 = "truncate table3;";

    // column 8 and 9 in table 1 are timestamps. I will let db populate those.
    private static final String INSERT_RECORD_TABLE1 = "insert into table1(pkid, col1, col2, col3, col4, col5, col6, col7) values(?,?,?,?,?,?,?,?);";
    private static final String INSERT_RECORD_TABLE2 = "insert into table2(pkid, col1) values(?,?);";
    private static final String INSERT_RECORD_TABLE3 = "insert into table3(pkid, col1, col2) values(?,?,?);";

    private final String POINT_SELECT_QUERY_TABLE1 = "select pkid,col1,col2,col3,col4,col5,col6,col7,col8,col9 from table1 where pkid = ?::uuid;";
    private final String POINT_SELECT_QUERY_TABLE2 = "select pkid,col1 from table2 where pkid = ?::uuid;";
    private final String POINT_SELECT_QUERY_TABLE3 = "select pkid,col1,col2 from table3 where pkid = ?::uuid;";

    private static final int ROWS_TO_PRELOAD = 10000;

    private enum WorkloadType {
        CREATE_TABLES,
        SEED_DATA,
        RUN_SIMULATION
    }

    private final FixedStepsWorkloadType createTablesWorkloadType;
    private final FixedTargetWorkloadType seedingWorkloadType;
    private final ThroughputWorkloadType runInstanceType;

    public TeslaWorkload() {
        this.createTablesWorkloadType = new FixedStepsWorkloadType(
                new FixedStepsWorkloadType.Step("Drop Table1", (a,b) -> jdbcTemplate.execute(DROP_TABLE1)),
                new FixedStepsWorkloadType.Step("Create Table1", (a,b) -> jdbcTemplate.execute(CREATE_TABLE1)),
                new FixedStepsWorkloadType.Step("Drop Table2", (a,b) -> jdbcTemplate.execute(DROP_TABLE2)),
                new FixedStepsWorkloadType.Step("Create Table2", (a,b) -> jdbcTemplate.execute(CREATE_TABLE2)),
                new FixedStepsWorkloadType.Step("Drop Table3", (a,b) -> jdbcTemplate.execute(DROP_TABLE3)),
                new FixedStepsWorkloadType.Step("Create Table3", (a,b) -> jdbcTemplate.execute(CREATE_TABLE3))
        );

        this.seedingWorkloadType = new FixedTargetWorkloadType();
        this.runInstanceType = new ThroughputWorkloadType();
    }

    private WorkloadDesc createTablesWorkload = new WorkloadDesc(
            TeslaWorkload.WorkloadType.CREATE_TABLES.toString(),
            "Create Tables",
            "Create the table. If the table already exists it will be dropped"
    );

    private WorkloadDesc seedingWorkload = new WorkloadDesc(
            TeslaWorkload.WorkloadType.SEED_DATA.toString(),
            "Seed Data",
            "Load data into the 3 tables (Latency on charts will show cumulative value for 3 inserts)",
            new WorkloadParamDesc("Items to generate:", 1, Integer.MAX_VALUE, 1000),
            new WorkloadParamDesc("Threads", 1, 500, 32)
    );

    private WorkloadDesc runningWorkload = new WorkloadDesc(
            TeslaWorkload.WorkloadType.RUN_SIMULATION.toString(),
            "Simulation",
            "Run a simulation of a reads from 3 tables (Latency on charts will show cumulative value for 3 selects and 3 inserts)",
            new WorkloadParamDesc("Throughput (tps)", 1, 1000000, 500),
            new WorkloadParamDesc("Max Threads", 1, 500, 64),
            new WorkloadParamDesc("Include new Inserts (to 3 tables)", false)
    );

    @Override
    public List<WorkloadDesc> getWorkloads() {
        return Arrays.asList(
                createTablesWorkload
                , seedingWorkload
                , runningWorkload
        );
    }


    @Override
    public InvocationResult invokeWorkload(String workloadId, ParamValue[] values) {
        TeslaWorkload.WorkloadType type = TeslaWorkload.WorkloadType.valueOf(workloadId);
        try {
            switch (type) {
                case CREATE_TABLES:
                    this.createTables();
                    return new InvocationResult("Ok");

                case SEED_DATA:
                    this.seedData(values[0].getIntValue(), values[1].getIntValue());
                    return new InvocationResult("Ok");

                case RUN_SIMULATION:
                    this.runSimulation(values[0].getIntValue(), values[1].getIntValue(), values[2].getBoolValue());
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
                    UUID uuid = LoadGeneratorUtils.getUUID();
                    jdbcTemplate.update(INSERT_RECORD_TABLE1,
                            uuid,
                            LoadGeneratorUtils.getInt(0, 100),
                            LoadGeneratorUtils.getInt(20, 300),
                            LoadGeneratorUtils.getInt(100, 1000),
                            LoadGeneratorUtils.getInt(0, 1000),
                            LoadGeneratorUtils.getDouble(),
                            LoadGeneratorUtils.getDouble(),
                            LoadGeneratorUtils.getDouble()
                    );
                    jdbcTemplate.update(INSERT_RECORD_TABLE2,
                            uuid,
                            LoadGeneratorUtils.getBinaryDataOfFixedSize(LoadGeneratorUtils.getInt(0,1024))
                    );
                    jdbcTemplate.update(INSERT_RECORD_TABLE3,
                            uuid,
                            LoadGeneratorUtils.getAlphaString(LoadGeneratorUtils.getInt(1,255)),
                            LoadGeneratorUtils.getBinaryDataOfFixedSize(LoadGeneratorUtils.getInt(0,4096))
                    );
                    return threadData;
                });
    }

    private List<UUID> getQueryList() {
        List<UUID> results = new ArrayList<UUID>(ROWS_TO_PRELOAD);
        jdbcTemplate.setMaxRows(ROWS_TO_PRELOAD);
        jdbcTemplate.setFetchSize(ROWS_TO_PRELOAD);
        jdbcTemplate.query("select pkid from table1 limit " + ROWS_TO_PRELOAD,
                new RowCallbackHandler() {
                    @Override
                    public void processRow(ResultSet rs) throws SQLException {
                        UUID value = (UUID)rs.getObject(1);
                        results.add(value);
                    }
                });
        return results;
    }


    private void runSimulation(int tps, int maxThreads, boolean runInserts) {
        System.out.println("**** Preloading data...");
        final List<UUID> uuids = getQueryList();
        System.out.println("**** Preloading complete...");

        Random random = ThreadLocalRandom.current();
        jdbcTemplate.setFetchSize(1000);

        runInstanceType
                .createInstance(timerService, workloadManager)
                .setMaxThreads(maxThreads)
                .execute(tps, (customData, threadData) -> {
                    UUID id = uuids.get(random.nextInt(uuids.size()));
                    runPointReadTable1(id);
                    runPointReadTable2(id);
                    runPointReadTable3(id);

                    if(runInserts){
                        runInserts();
                    }
                });
    }

    private void runPointReadTable1(UUID id){
        String query = POINT_SELECT_QUERY_TABLE1;
        jdbcTemplate.query(query, new Object[] {id}, new int[] {java.sql.Types.VARCHAR},
                new RowCallbackHandler() {
                    @Override
                    public void processRow(ResultSet rs) throws SQLException {
/*                                    System.out.printf("pkid=%s, col1='%s', col2=%s, col3=%s, col4=%s, col5=%s, col6=%s, col7=%s, col8=%s, col9=%s \n",
                                            rs.getString("pkid"),
                                            rs.getInt("col1"),
                                            rs.getInt("col2"),
                                            rs.getInt("col3"),
                                            rs.getInt("col4"),
                                            rs.getDouble("col5"),
                                            rs.getDouble("col6"),
                                            rs.getDouble("col7"),
                                            rs.getTimestamp("col8"),
                                            rs.getTimestamp("col9")

                                    );*/
                    }
                });
    }

    private void runPointReadTable2(UUID id){
        String query = POINT_SELECT_QUERY_TABLE2;
        jdbcTemplate.query(query, new Object[] {id}, new int[] {java.sql.Types.VARCHAR},
                new RowCallbackHandler() {
                    @Override
                    public void processRow(ResultSet rs) throws SQLException {
/*                                    System.out.printf("pkid=%s, col1='%s' \n",
                                            rs.getString("pkid"),
                                            rs.getBytes("col1") != null?rs.getBytes("col1").length:null
                                    );*/
                    }
                });
    }

    private void runPointReadTable3(UUID id){
        String query = POINT_SELECT_QUERY_TABLE3;
        jdbcTemplate.query(query, new Object[] {id}, new int[] {java.sql.Types.VARCHAR},
                new RowCallbackHandler() {
                    @Override
                    public void processRow(ResultSet rs) throws SQLException {
/*                        System.out.printf("pkid=%s, col1='%s' , col2='%s' \n",
                                rs.getString("pkid"),
                                rs.getString("col1"),
                                rs.getBytes("col2") != null?rs.getBytes("col1").length:null
                        );*/
                    }
                });
    }

    private void runInserts(){
        UUID uuid = LoadGeneratorUtils.getUUID();
        jdbcTemplate.update(INSERT_RECORD_TABLE1,
                uuid,
                LoadGeneratorUtils.getInt(0, 100),
                LoadGeneratorUtils.getInt(20, 300),
                LoadGeneratorUtils.getInt(100, 1000),
                LoadGeneratorUtils.getInt(0, 1000),
                LoadGeneratorUtils.getDouble(),
                LoadGeneratorUtils.getDouble(),
                LoadGeneratorUtils.getDouble()
        );
        jdbcTemplate.update(INSERT_RECORD_TABLE2,
                uuid,
                LoadGeneratorUtils.getBinaryDataOfFixedSize(LoadGeneratorUtils.getInt(0,1024))
        );
        jdbcTemplate.update(INSERT_RECORD_TABLE3,
                uuid,
                LoadGeneratorUtils.getAlphaString(LoadGeneratorUtils.getInt(1,255)),
                LoadGeneratorUtils.getBinaryDataOfFixedSize(LoadGeneratorUtils.getInt(0,4096))
        );
    }



}

