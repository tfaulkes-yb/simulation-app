package com.yugabyte.simulation.dao;

import com.yugabyte.simulation.services.Timer;

public interface WorkloadSimulationDAO {
    int simulateSubmission(Timer timer)  throws  Exception;
    int simulateStatus(Timer timer) throws Exception;
    int simulateUpdates(Timer timer, int numberOfTimesToRunUpdateOnSameRecord) throws Exception;
    int createDBTableIfNeeded();
    int truncateDBTable();
}
