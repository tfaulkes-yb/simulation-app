package com.yugabyte.simulation.config;

import com.yugabyte.simulation.service.CapitalGroupWorkload;
import com.yugabyte.simulation.service.SonosWorkload;
import com.yugabyte.simulation.service.TeslaWorkload;
import com.yugabyte.simulation.service.WorkloadSimulation;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class WorkloadConfig {
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

}
