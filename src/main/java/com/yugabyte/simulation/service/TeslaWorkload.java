package com.yugabyte.simulation.service;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowCallbackHandler;
import org.springframework.stereotype.Repository;

import com.yugabyte.simulation.dao.InvocationResult;
import com.yugabyte.simulation.dao.ParamType;
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
public class TeslaWorkload  extends WorkloadSimulationBase implements WorkloadSimulation {

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Autowired
    private ServiceManager serviceManager;

	@Override
	public String getName() {
		return "Tesla";
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
            "   rawdatacol bytea,\n" +
            "   primary key (pkid)\n" +
            ");";

    private static final String CREATE_TABLE3 = "create table if not exists table3(\n" +
            "   pkid uuid,\n" +
            "   col1 varchar(255),\n" +
            "   rawdatacol bytea,\n" +
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
    private static final String INSERT_RECORD_TABLE2 = "insert into table2(pkid, rawdatacol) values(?,?);";
    private static final String INSERT_RECORD_TABLE3 = "insert into table3(pkid, col1, rawdatacol) values(?,?,?);";

    private final String POINT_SELECT_QUERY_TABLE1 = "select pkid,col1,col2,col3,col4,col5,col6,col7,col8,col9 from table1 where pkid = ?::uuid;";
    private final String POINT_SELECT_QUERY_TABLE2 = "select pkid,rawdatacol from table2 where pkid = ?::uuid;";
    private final String POINT_SELECT_QUERY_TABLE3 = "select pkid,col1,rawdatacol from table3 where pkid = ?::uuid;";

    private final String SELECT_QUERY_ON_BINARYCOL_TABLE2 = "select pkid,rawdatacol from table2 where rawdatacol like ?::bytea limit 100;";
    private final String SELECT_QUERY_ON_BINARYCOL_TABLE3 = "select pkid,col1,rawdatacol from table3 where rawdatacol like ?::bytea limit 100;";

    private static final int ROWS_TO_PRELOAD = 10000;

    private enum WorkloadType {
        CREATE_TABLES,
        SEED_DATA,
        RUN_SIMULATION,
        RUN_LIKE_QUERY_ON_TABLE2,
        RUN_LIKE_QUERY_ON_TABLE3
    }

    private final FixedStepsWorkloadType createTablesWorkloadType;
    private final FixedTargetWorkloadType seedingWorkloadType;
    private final ThroughputWorkloadType runInstanceType;

    public TeslaWorkload() {
        this.createTablesWorkloadType = new FixedStepsWorkloadType(
                new Step("Drop Table1", (a,b) -> jdbcTemplate.execute(DROP_TABLE1)),
                new Step("Create Table1", (a,b) -> jdbcTemplate.execute(CREATE_TABLE1)),
                new Step("Drop Table2", (a,b) -> jdbcTemplate.execute(DROP_TABLE2)),
                new Step("Create Table2", (a,b) -> jdbcTemplate.execute(CREATE_TABLE2)),
                new Step("Drop Table3", (a,b) -> jdbcTemplate.execute(DROP_TABLE3)),
                new Step("Create Table3", (a,b) -> jdbcTemplate.execute(CREATE_TABLE3))
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

    private WorkloadDesc runningWorkloadWithLikeOnByteaColInTable2 = new WorkloadDesc(
            WorkloadType.RUN_LIKE_QUERY_ON_TABLE2.toString(),
            "Select Query on Binary Data (non indexed) [Table2]",
            "Run Query with like clause on binary column for Table2. rawdatacol is non indexed column containing blob data.",
            new WorkloadParamDesc("rawdatacol value", ParamType.STRING)
    );

    private WorkloadDesc runningWorkloadWithLikeOnByteaColInTable3 = new WorkloadDesc(
            WorkloadType.RUN_LIKE_QUERY_ON_TABLE3.toString(),
            "Select Query on Binary Data (non indexed) [Table3]",
            "Run Query with like clause on binary column for Table3. rawdatacol is non indexed column containing blob data.",
            new WorkloadParamDesc("rawdatacol value", ParamType.STRING)
    );



    @Override
    public List<WorkloadDesc> getWorkloads() {
        return Arrays.asList(
                createTablesWorkload
                , seedingWorkload
                , runningWorkload
                , runningWorkloadWithLikeOnByteaColInTable2
                , runningWorkloadWithLikeOnByteaColInTable3

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

                case RUN_LIKE_QUERY_ON_TABLE2:
                    this.runSimulationWithLikeOnByteaColForTable2(values[0].getStringValue());
                    return new InvocationResult("Ok");
                case RUN_LIKE_QUERY_ON_TABLE3:
                    this.runSimulationWithLikeOnByteaColForTable3(values[0].getStringValue());
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

    private void seedData(int numberToGenerate, int threads) {
        seedingWorkloadType
                .createInstance(serviceManager)
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
                .createInstance(serviceManager)
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
/*                                    System.out.printf("pkid=%s, rawdatacol='%s' \n",
                                            rs.getString("pkid"),
                                            rs.getBytes("rawdatacol") != null?rs.getBytes("rawdatacol").length:null
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
/*                        System.out.printf("pkid=%s, col1='%s' , rawdatacol='%s' \n",
                                rs.getString("pkid"),
                                rs.getString("col1"),
                                rs.getBytes("rawdatacol") != null?rs.getBytes("rawdatacol").length:null
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

    private void runSimulationWithLikeOnByteaColForTable2(String rawDataColVal){
        long now = System.nanoTime();
        System.out.println("**************** Single query on Table2 with like:["+SELECT_QUERY_ON_BINARYCOL_TABLE2+"] rawDataColVal:["+rawDataColVal+"]");
        jdbcTemplate.query(SELECT_QUERY_ON_BINARYCOL_TABLE2, new Object[] {"%"+rawDataColVal+"%"}, new int[] {Types.VARCHAR},
                new RowCallbackHandler() {
                    @Override
                    public void processRow(ResultSet rs) throws SQLException {
                        System.out.printf(
                                "pkid='%s', rawdatacol='%s'\n",
                                ((UUID)rs.getObject("pkid")).toString(),
                                rs.getString("rawdatacol"));
                    }
                });
        System.out.printf("**************** Results for raw data like query for Table2 from %s fetched in %fms\n", rawDataColVal, (System.nanoTime() - now) / 1_000_000.0);

    }

    private void runSimulationWithLikeOnByteaColForTable3(String rawDataColVal){
        long now = System.nanoTime();
        System.out.println("**************** Single query on Table3 with like:["+SELECT_QUERY_ON_BINARYCOL_TABLE3+"] rawDataColVal:["+rawDataColVal+"]");
        jdbcTemplate.query(SELECT_QUERY_ON_BINARYCOL_TABLE3, new Object[] {"%"+rawDataColVal+"%"}, new int[] {Types.VARCHAR},
                new RowCallbackHandler() {
                    @Override
                    public void processRow(ResultSet rs) throws SQLException {
                        System.out.printf(
                                "pkid='%s', col1='%s', rawdatacol='%s'\n",
                                ((UUID)rs.getObject("pkid")).toString(),
                                rs.getString("col1"),
                                rs.getString("rawdatacol"));
                    }
                });
        System.out.printf("**************** Results for raw data like query for Table3 from %s fetched in %fms\n", rawDataColVal, (System.nanoTime() - now) / 1_000_000.0);

    }



}

