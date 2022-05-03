package com.yugabyte.simulation.controller;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.yugabyte.simulation.dao.InvocationResult;
import com.yugabyte.simulation.dao.ParamValue;
import com.yugabyte.simulation.dao.WorkloadDesc;
import com.yugabyte.simulation.dao.WorkloadSimulationDAO;
import com.yugabyte.simulation.service.SonosWorkload;
import com.yugabyte.simulation.service.WorkloadSimulation;
import com.yugabyte.simulation.services.TimerService;
import com.yugabyte.simulation.services.TimerType;

@RestController
@RequestMapping("/api")
public class WorkloadSimulationController {
    @Autowired
    private WorkloadSimulationDAO workloadSimulationDAO;

    @Autowired
    private TimerService timerService;
    
    // Generic interface, to be populated with class loaded dynamically?
    private WorkloadSimulation workloadSimulation = new SonosWorkload();
    
    @GetMapping("get-workloads")
    public List<WorkloadDesc> getWorkloads() {
    	return workloadSimulation.getWorkloads();
    }

    @PostMapping("/invoke-workload/{workload}") 
    @ResponseBody
    public InvocationResult invokeWorkload(@PathVariable String workload, @RequestBody ParamValue[] params) {
    	return workloadSimulation.invokeWorkload(workload, params);
    }
    
    @GetMapping("/create-table")
    public int createTable(){
        return workloadSimulationDAO.createDBTableIfNeeded();
    }

    @GetMapping("/truncate-table")
    public int truncateTable(){
        return workloadSimulationDAO.truncateDBTable();
    }

    @GetMapping("/simulate-submissions/{threads}/{numberOfSubmissions}")
    public void simulateSubmissions(@PathVariable int threads, @PathVariable int numberOfSubmissions){
        // We need to invoke N threads.
        try {
            ExecutorService mExecutorService = Executors.newFixedThreadPool(threads);
            List<Callable<Result>> mTasks = new ArrayList<Callable<Result>>();
            long mStartTime = System.currentTimeMillis();
            for(int i = 0; i < numberOfSubmissions; i++){
                mTasks.add(new SimulateSubmissionsCallable());
            }
            List<Future<Result>> mFutures = mExecutorService.invokeAll(mTasks);
            mExecutorService.shutdown();
            for (Future<Result> mFuture : mFutures) {
                Result result = mFuture.get();
            }
            mExecutorService.awaitTermination(120, TimeUnit.MINUTES);
            System.out.printf("Simulation completed!\n");
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    @GetMapping("/simulate-status-checks/{threads}/{numberOfStatusChecks}")
    public void simulateStatusChecks(@PathVariable int threads, @PathVariable int numberOfStatusChecks){
        // We need to invoke N threads.
        try {
            ExecutorService mExecutorService = Executors.newFixedThreadPool(threads);
            List<Callable<Result>> mTasks = new ArrayList<Callable<Result>>();
            long mStartTime = System.currentTimeMillis();
            for(int i = 0; i < numberOfStatusChecks; i++){
                mTasks.add(new SimulateStatusChecksCallable());
            }
            List<Future<Result>> mFutures = mExecutorService.invokeAll(mTasks);
            mExecutorService.shutdown();
            for (Future<Result> mFuture : mFutures) {
                Result result = mFuture.get();
            }
            mExecutorService.awaitTermination(120, TimeUnit.MINUTES);
            System.out.printf("Simulation completed!\n");
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }



    @GetMapping("/simulate-updates/{threads}/{numberOfTimesToRerunUpdateOnSameRecord}")
    public int simulateUpdates(@PathVariable int threads, @PathVariable int numberOfTimesToRerunUpdateOnSameRecord){
        // We need to invoke N threads.
        try {
            ExecutorService mExecutorService = Executors.newFixedThreadPool(threads);
            List<Callable<Result>> mTasks = new ArrayList<Callable<Result>>();
            long mStartTime = System.currentTimeMillis();
            for(int i = 0; i < threads; i++){
                mTasks.add(new SimulateUpdatesCallable(numberOfTimesToRerunUpdateOnSameRecord));
            }
            List<Future<Result>> mFutures = mExecutorService.invokeAll(mTasks);
            mExecutorService.shutdown();
            for (Future<Result> mFuture : mFutures) {
                Result result = mFuture.get();
            }
            mExecutorService.awaitTermination(120, TimeUnit.MINUTES);
            System.out.printf("Simulation of Updates completed!\n");
            return 0;
        }
        catch (Exception e) {
            e.printStackTrace();
            return -1;
        }
    }

    // Dummy object for now - In future we can use it to get response data if needed
    class Result {
        public Result(){
        }
    }

    class SimulateSubmissionsCallable implements Callable<Result> {
        private Result result;

        public SimulateSubmissionsCallable(){
            result = new Result();
        }
        @Override
        public Result call() throws Exception {
            try {
                workloadSimulationDAO.simulateSubmission(timerService.getTimer(TimerType.SUBMISSION));
                //timer.end(ExecutionStatus.SUCCESS);
            }
            catch (Exception e) {
                System.out.println("@@@@@ACTEST exception:"+e.getMessage());
            }
            return  result;
        }
    }

    class SimulateStatusChecksCallable implements Callable<Result> {
        private Result result;
        public SimulateStatusChecksCallable(){
            result = new Result();
        }
        @Override
        public Result call() throws Exception {
            try {
                workloadSimulationDAO.simulateStatus(timerService.getTimer(TimerType.STATUS));
            }
            catch (Exception e) {
                System.out.println("@@@@@ACTEST exception:"+e.getMessage());
            }
            return  result;
        }
    }

    class SimulateUpdatesCallable implements Callable<Result> {
        private Result result;
        private int numberOfTimesToRerunUpdateOnSameRecord;

        public SimulateUpdatesCallable(int numberOfTimesToRerunUpdateOnSameRecord){
            result = new Result();
            this.numberOfTimesToRerunUpdateOnSameRecord = numberOfTimesToRerunUpdateOnSameRecord;
        }
        @Override
        public Result call() throws Exception {
            try {
                workloadSimulationDAO.simulateUpdates(timerService.getTimer(TimerType.SUBMISSION),numberOfTimesToRerunUpdateOnSameRecord);
            }
            catch (Exception e) {
                System.out.println("@@@@@ACTEST exception:"+e.getMessage());
            }

            return  result;
        }
    }


}

