package com.yugabyte.simulation.service;

import java.util.Arrays;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import com.yugabyte.simulation.dao.InvocationResult;
import com.yugabyte.simulation.dao.ParamType;
import com.yugabyte.simulation.dao.ParamValue;
import com.yugabyte.simulation.dao.WorkloadDesc;
import com.yugabyte.simulation.dao.WorkloadParamDesc;

@Repository
public class SonosWorkload implements WorkloadSimulation {

	@Autowired
	private JdbcTemplate jdbcTemplate;
	
	private static final String CREATE_TOPOLOGY_TABLE = 
			"create table if not exists topology (\n"
			+ "	parentid varchar,\n"
			+ "	Id varchar,\n"
			+ "	IdType varchar not null,\n"
			+ "	IdName varchar,\n"
			+ "	created timestamp without time zone default now(),\n"
			+ "	updated timestamp without time zone default now(),\n"
			+ "	constraint topology_pkey PRIMARY KEY(parentid,id)\n"
			+ ")\n"
			+ ";";

	private static final String DROP_TOPOLOGY_TABLE = 
			"drop table if exists topology;";
			
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
			"drop table mhhmap if exists;";

	
	private enum WorkloadType {
		CREATE_TABLES, 
		LOAD_DATA
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
					new WorkloadParamDesc("Truncate tables", false, false)
			)
		);
		
	}

	@Override
	public InvocationResult invokeWorkload(String workloadId, ParamValue[] values) {
		WorkloadType type = WorkloadType.valueOf(workloadId);
		switch (type) {
		case CREATE_TABLES:
			this.createTables(values[0].getBoolValue());
			return new InvocationResult("Ok");
		
		case LOAD_DATA:
			return new InvocationResult("Done");
		}
		throw new IllegalArgumentException("Unknown workload "+ workloadId);
	}
	
	private void createTables(boolean force) {
		if (force) {
			jdbcTemplate.execute(DROP_MHHMAP_TABLE);
			jdbcTemplate.execute(DROP_TOPOLOGY_TABLE);
		}
		jdbcTemplate.execute(CREATE_MHHMAP_TABLE);
		jdbcTemplate.execute(CREATE_TOPOLOGY_TABLE);
	}

}
