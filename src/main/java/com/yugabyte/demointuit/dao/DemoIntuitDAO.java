package com.yugabyte.demointuit.dao;

import com.yugabyte.demointuit.services.Timer;

public interface DemoIntuitDAO {
    void simulateSubmission(Timer timer)  throws  Exception;
    void simulateStatus(Timer timer) throws Exception;
    void simulateUpdates(Timer timer, int numberOfTimesToRunUpdateOnSameRecord) throws Exception;
    int createDBTableIfNeeded();
}
