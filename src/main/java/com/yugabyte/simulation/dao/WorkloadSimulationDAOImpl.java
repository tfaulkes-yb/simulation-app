package com.yugabyte.simulation.dao;

import com.yugabyte.simulation.services.ExecutionStatus;
import com.yugabyte.simulation.services.Timer;
import com.yugabyte.simulation.util.GeneralUtility;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

@Repository
public class WorkloadSimulationDAOImpl implements WorkloadSimulationDAO {
    @Autowired
    JdbcTemplate jdbcTemplate;

    private static AtomicLong filingIdForInsert;
    @Value("${idCounter:1}")
    public void setFilingIdInit(long fid){
        filingIdForInsert = new AtomicLong(fid);
    }
    private static final Logger LOGGER = LoggerFactory.getLogger(WorkloadSimulationDAOImpl.class);

    @Override
    public void simulateSubmission(Timer mainTimer) throws  Exception{
        // Each submission does 10 selects.
        // Lets get list of Random values.
        List<Long> listOfRandomIds = GeneralUtility.getRandomIds(10);
        Timer timer = null;
        // Aim here is to do 10 selects. We will spread the select queries to various tables.
        for(int i = 0; i < listOfRandomIds.size(); i++){
            Long id = listOfRandomIds.get(i);
            timer = mainTimer.timeSubPortion("select-query");
            try {
                timer.start();
                if(i == 0 || i == 1){
                    // Query for join between FILING table and DL_FILING table
                    jdbcTemplate.queryForList(DAOUtil.QUERY_FILING_AND_DL_FILING_JOIN, new Object[]{id});
                }
                else if(i == 2 || i == 3){
                    // Query from ROUTING_NUMBER
                    jdbcTemplate.queryForList(DAOUtil.QUERY_ROUTING_NUMBER, new Object[]{id});
                }
                else if(i == 4 || i == 5){
                    // Query from TRANSMISSION
                    jdbcTemplate.queryForList(DAOUtil.QUERY_TRANSMISSION, new Object[]{id});
                }
                else if(i == 6 || i == 7){
                    // Query from TRANSMISSION_FILING
                    jdbcTemplate.queryForList(DAOUtil.QUERY_TRANSMISSION_FILING, new Object[]{id});
                }
                else if(i == 8 || i == 9){
                    // Query from TRANSMIT_DATA_VALUE
                    jdbcTemplate.queryForList(DAOUtil.QUERY_TRANSMIT_DATA_VALUE, new Object[]{id});
                }
                timer.end(ExecutionStatus.SUCCESS);
            } catch (Exception e) {
                timer.end(ExecutionStatus.ERROR);
                e.printStackTrace();
                throw e;
            }
        }

        // we will insert now - 12 inserts (2 inserts to each table)
        for(int i = 0; i < 2; i++){
            long id = filingIdForInsert.getAndIncrement();
            for(int j = 0; j < 6; j++){

                timer = mainTimer.timeSubPortion("insert-query");
                try {
                    timer.start();
                    if(j == 0){
                        // Add data to FILING table
                        jdbcTemplate.update(DAOUtil.INSERT_QUERY_TABLE_FILING,DAOUtil.getRandomData(id,78,true));
                    }
                    else if(j == 1){
                        // Add data to DL_FILING table
                        jdbcTemplate.update(DAOUtil.INSERT_QUERY_TABLE_DL_FILING,DAOUtil.getRandomData(id,29,false));
                    }
                    else if(j == 2){
                        // Add data to ROUTING table
                        jdbcTemplate.update(DAOUtil.INSERT_QUERY_TABLE_ROUTING_NUMBER,DAOUtil.getRandomData(id,3,false));
                    }
                    else if(j == 3){
                        // Add data to TRANSMISSION
                        jdbcTemplate.update(DAOUtil.INSERT_QUERY_TABLE_TRANSMISSION,DAOUtil.getRandomData(id,5,false));
                    }
                    else if(j == 4){
                        // Add data to TRANSMISSION_FILING
                        jdbcTemplate.update(DAOUtil.INSERT_QUERY_TABLE_TRANSMISSION_FILING,DAOUtil.getRandomData(id,7,false));
                    }
                    else if(j == 5){
                        // Add data to TRANSMIT_DATA_VALUE
                        jdbcTemplate.update(DAOUtil.INSERT_QUERY_TABLE_TRANSMIT_DATA_VALUE,DAOUtil.getRandomData(id,7,false));
                    }
                    timer.end(ExecutionStatus.SUCCESS);
                } catch (Exception e) {
                    timer.end(ExecutionStatus.ERROR);
                    e.printStackTrace();
                    throw e;
                }
            }
        }
    }

    @Override
    public void simulateStatus(Timer mainTimer) throws Exception {
        // Here we are going to perform 20 selects and 20 updates
        Timer timer = mainTimer.timeSubPortion("select-query");
        // Lets get list of Random values.
        List<Long> listOfRandomIds = GeneralUtility.getRandomIds(20);

        // 20 Selects -  4 queries on each table
        for(int i = 0; i <  listOfRandomIds.size(); i++){
            Long id = listOfRandomIds.get(i);
            timer = mainTimer.timeSubPortion("select-query");
            try {
                timer.start();
                if(i >= 0 && i <= 3){
                    jdbcTemplate.queryForList(DAOUtil.QUERY_FOR_STATUS_CHECK_FILING,new Object[]{id});
                }
                else if(i >= 4 && i <= 7){
                    jdbcTemplate.queryForList(DAOUtil.QUERY_FOR_STATUS_CHECK_DL_FILING,new Object[]{id});
                }
                else if(i >= 8 && i <= 11){
                    jdbcTemplate.queryForList(DAOUtil.QUERY_FOR_STATUS_CHECK_TRANSMISSION,new Object[]{id});
                }
                else if(i >= 12 && i <= 15){
                    jdbcTemplate.queryForList(DAOUtil.QUERY_FOR_STATUS_CHECK_TRANSMISSION_FILING,new Object[]{id});
                }
                else if(i >= 16 && i <= 20){
                    jdbcTemplate.queryForList(DAOUtil.QUERY_FOR_STATUS_CHECK_TRANSMIT_DATA_VALUE,new Object[]{id});
                }
                timer.end(ExecutionStatus.SUCCESS);
            } catch (Exception e) {
                timer.end(ExecutionStatus.ERROR);
                e.printStackTrace();

            }
        }

        // 10 Updates (2 to each table)
        for(int i = 0; i <  listOfRandomIds.size()/2; i++){
            Long id = listOfRandomIds.get(i);
            timer = mainTimer.timeSubPortion("update-query");
            try {
                timer.start();
                if(i == 0 || i == 1){
                    jdbcTemplate.update(DAOUtil.QUERY_FOR_STATUS_UPDATE_FILING,new Object[]{GeneralUtility.randomIntegerVal(1,2000),id});
                }
                else if(i == 2 || i == 3){
                    jdbcTemplate.update(DAOUtil.QUERY_FOR_STATUS_UPDATE_DL_FILING,new Object[]{GeneralUtility.randomIntegerVal(1,2000),id});
                }
                else if(i == 4 || i == 5){
                    jdbcTemplate.update(DAOUtil.QUERY_FOR_STATUS_UPDATE_TRANSMISSION,new Object[]{GeneralUtility.randomIntegerVal(1,2000),id});
                }
                else if(i == 6 || i == 7){
                    jdbcTemplate.update(DAOUtil.QUERY_FOR_STATUS_UPDATE_TRANSMISSION_FILING,new Object[]{GeneralUtility.randomIntegerVal(1,2000),id});
                }
                else if(i == 8 || i == 9){
                    jdbcTemplate.update(DAOUtil.QUERY_FOR_STATUS_UPDATE_TRANSMIT_DATA_VALUE,new Object[]{GeneralUtility.randomIntegerVal(1,2000),id});
                }
                timer.end(ExecutionStatus.SUCCESS);
            } catch (Exception e) {
                timer.end(ExecutionStatus.ERROR);
                e.printStackTrace();
            }
        }
    }

    @Override
    public void simulateUpdates(Timer mainTimer, int numberOfTimesToRunUpdateOnSameRecord) throws Exception {
        // Get a random number between 1 and 10000. Note that this number may or may not exist is database yet.
        long randomFilingId = GeneralUtility.randomLongVal(1,10000);
        // Same Record will keep getting updated over and over for n number of times.
        Timer timer = null;
        for(int i = 0; i < numberOfTimesToRunUpdateOnSameRecord ; i++){
            timer = mainTimer.timeSubPortion("update-query");
            try {
                timer.start();
                jdbcTemplate.update(DAOUtil.QUERY_FOR_STATUS_UPDATE_FILING,new Object[]{GeneralUtility.randomIntegerVal(1,10),randomFilingId});
                timer.end(ExecutionStatus.SUCCESS);
            } catch (Exception e) {
                timer.end(ExecutionStatus.ERROR);
                e.printStackTrace();
                Thread.sleep(10);
            }
        }
    }

    @Override
    public int createDBTableIfNeeded() {
        jdbcTemplate.execute(DAOUtil.CREATE_EXTENSION); // I don't need it now since we are not using gen random uuid anymore.
        jdbcTemplate.execute(DAOUtil.CREATE_TABLE_FILING);
        jdbcTemplate.execute(DAOUtil.CREATE_TABLE_DL_FILING);
        jdbcTemplate.execute(DAOUtil.CREATE_TABLE_ROUTING_NUMBER);
        jdbcTemplate.execute(DAOUtil.CREATE_TABLE_TRANSMISSION);
        jdbcTemplate.execute(DAOUtil.CREATE_TABLE_TRANSMISSION_FILING);
        jdbcTemplate.execute(DAOUtil.CREATE_TABLE_TRANSMIT_DATA_VALUE);
        return 0;
    }


}
