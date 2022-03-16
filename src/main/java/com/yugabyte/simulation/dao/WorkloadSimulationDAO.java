package com.yugabyte.simulation.dao;

import com.yugabyte.simulation.services.Timer;

public interface WorkloadSimulationDAO {
    void simulateSubmission(Timer timer)  throws  Exception;
    void simulateStatus(Timer timer) throws Exception;
    void simulateUpdates(Timer timer, int numberOfTimesToRunUpdateOnSameRecord) throws Exception;
    int createDBTableIfNeeded();
}
