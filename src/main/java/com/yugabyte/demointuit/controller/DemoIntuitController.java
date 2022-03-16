package com.yugabyte.demointuit.controller;

import com.yugabyte.demointuit.dao.DemoIntuitDAO;
import com.yugabyte.demointuit.services.ExecutionStatus;
import com.yugabyte.demointuit.services.Timer;
import com.yugabyte.demointuit.services.TimerService;
import com.yugabyte.demointuit.services.TimerType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

@RestController
public class DemoIntuitController {
    @Autowired
    private DemoIntuitDAO demoIntuitDAO;

    @Autowired
    private TimerService timerService;

    @GetMapping("/api/create-table")
    public int createTable(){
        return demoIntuitDAO.createDBTableIfNeeded();
    }

    @GetMapping("/api/simulate-submissions/{threads}/{numberOfSubmissions}")
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

    @GetMapping("/api/simulate-status-checks/{threads}/{numberOfStatusChecks}")
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



    @GetMapping("/api/simulate-updates/{threads}/{numberOfTimesToRerunUpdateOnSameRecord}")
    public void simulateUpdates(@PathVariable int threads, @PathVariable int numberOfTimesToRerunUpdateOnSameRecord){
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
        }
        catch (Exception e) {
            e.printStackTrace();
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
                demoIntuitDAO.simulateSubmission(timerService.getTimer(TimerType.SUBMISSION));
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
                demoIntuitDAO.simulateStatus(timerService.getTimer(TimerType.STATUS));
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
                demoIntuitDAO.simulateUpdates(timerService.getTimer(TimerType.SUBMISSION),numberOfTimesToRerunUpdateOnSameRecord);
            }
            catch (Exception e) {
                System.out.println("@@@@@ACTEST exception:"+e.getMessage());
            }

            return  result;
        }
    }


}

