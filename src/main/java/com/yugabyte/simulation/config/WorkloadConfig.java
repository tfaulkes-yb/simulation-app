package com.yugabyte.simulation.config;

import com.yugabyte.simulation.service.*;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

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

    @Bean(name="NewFormatWorkload")
    public WorkloadSimulation newFormatWorkload(){
        return new NewFormatWorkload();
    }

    @Bean(name="GenericWorkload")
    public WorkloadSimulation genericWorkload(){
        return new GenericWorkload();
    }
}
