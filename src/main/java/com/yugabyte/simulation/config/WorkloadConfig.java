package com.yugabyte.simulation.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.yugabyte.simulation.service.AmexWorkload;
import com.yugabyte.simulation.service.CapitalGroupWorkload;
import com.yugabyte.simulation.service.CbsSportsWorkload;
import com.yugabyte.simulation.service.GenericCassandraWorkload;
import com.yugabyte.simulation.service.GenericWorkload;
import com.yugabyte.simulation.service.NewFormatWorkload;
import com.yugabyte.simulation.service.PitrSqlDemoWorkload;
import com.yugabyte.simulation.service.SimpleSelectWorkload;
import com.yugabyte.simulation.service.SonosWorkload;
import com.yugabyte.simulation.service.TeslaWorkload;
import com.yugabyte.simulation.service.WorkloadSimulation;

@Configuration
public class WorkloadConfig {
    @Bean(name="SimpleSelectWorkload")
    public WorkloadSimulation simpleSelectWorkload(){
        return new SimpleSelectWorkload();
    }

    @Bean(name="PitrDemoWorkload")
    public WorkloadSimulation pitrDemoWorkload(){
        return new PitrSqlDemoWorkload();
    }

    @Bean(name="NewFormatWorkload")
    public WorkloadSimulation newFormatWorkload(){
        return new NewFormatWorkload();
    }

    @Bean(name="GenericWorkload")
    public WorkloadSimulation genericWorkload(){
        return new GenericWorkload();
    }

    @Bean(name="GenericCassandraWorkload")
    public WorkloadSimulation genericCassandraWorkload(){
        return new GenericCassandraWorkload();
    }


    // ------------------------------------
    // Customer workloads
    @Bean(name="AmexWorkload")
    public WorkloadSimulation amexWorkload(){
        return new AmexWorkload();
    }

    @Bean(name="SonosWorkload")
    public WorkloadSimulation sonosWorkload(){
        return new SonosWorkload();
    }

    @Bean(name="TeslaWorkload")
    public WorkloadSimulation teslaWorkload(){
        return new TeslaWorkload();
    }

    @Bean(name="CapitalGroupWorkload")
    public WorkloadSimulation capitalGroupWorkload(){
        return new CapitalGroupWorkload();
    }

    @Bean(name="CbsSportsWorkload")
    public WorkloadSimulation cbsSportsWorkload(){
        return new CbsSportsWorkload();
    }


}
