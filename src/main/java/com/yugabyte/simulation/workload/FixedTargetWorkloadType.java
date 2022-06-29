package com.yugabyte.simulation.workload;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import com.yugabyte.simulation.dao.TimerResult;
import com.yugabyte.simulation.services.ExecutionStatus;
import com.yugabyte.simulation.services.Timer;
import com.yugabyte.simulation.services.TimerService;

/**
 * Run a workload with a fixed target. For example, to seed a database with 
 * 1,000,000 records. This will launch a specified number of threads to process
 * the workload. Note that it is up to the 
 * @author timfaulkes
 *
 */
public class FixedTargetWorkloadType extends WorkloadType {
	
	
			
	public interface ExecuteTask {
		public Object run(Object customData, Object threadData);
	}
	

	private static class WorkerThread implements Runnable {
		private final ExecuteTask task;
		private final AtomicBoolean terminate;
		private final Object customData;
		private final Timer timer;
		private Object threadData;
		private final AtomicLong completedCounter;
		private final AtomicLong startedCounter;
		private final long target;
		private final int workloadOrdinal;

		public WorkerThread(int threadId, AtomicBoolean terminate, AtomicLong completedCounter, AtomicLong startedCounter, long target, Object customData, TimerService timerService, ExecuteTask task,int workloadOrdinal) {
			this.terminate = terminate;
			this.customData = customData;
			this.threadData = null;
			this.timer = timerService.getTimer();
			this.task = task;
			this.completedCounter = completedCounter;
			this.startedCounter = startedCounter;
			this.target = target;
			this.workloadOrdinal = workloadOrdinal;
		}
		
		@Override
		public void run() {
			while (!terminate.get() && startedCounter.incrementAndGet() < target) {
				timer.start();
				try {
					this.threadData = task.run(customData, threadData);
					timer.end(ExecutionStatus.SUCCESS, workloadOrdinal);
				}
				catch (Exception e) {
					timer.end(ExecutionStatus.ERROR, workloadOrdinal);
					// TODO Log exception?
				}
				this.completedCounter.incrementAndGet();
			}
		}
	}
	
	public static class FixedTargetTimerResult extends TimerResult {
		private final double percentageComplete;
		private final long timeRemainingInMs;
		private final long completed;
		private final long target;
		public FixedTargetTimerResult(TimerResult orig, double percentageComplete, long timeRemainingInMs, long completed, long target) {
			super(orig);
			this.percentageComplete = percentageComplete;
			this.timeRemainingInMs = timeRemainingInMs;
			this.completed = completed;
			this.target = target;
		}
		
		public double getPercentageComplete() {
			return percentageComplete;
		}
		
		public long getTimeRemainingInMs() {
			return timeRemainingInMs;
		}
		
		public long getCompleted() {
			return completed;
		}
		public long getTarget() {
			return target;
		}
	}
	
	public class FixedTargetWorkloadInstance extends WorkloadTypeInstance {
		private AtomicLong completedCounter = new AtomicLong(0);
		private AtomicLong startedCounter = new AtomicLong(0);
		private AtomicBoolean terminate = new AtomicBoolean(false);
		private long startTime = 0;
		private ExecutorService executor = null;
		private long target = 0;
		private Object customData = null;
		
		public FixedTargetWorkloadInstance(TimerService timerService) {
			super(timerService);
		}
		public FixedTargetWorkloadInstance setCustomData(Object customData) {
			this.customData = customData;
			return this;
		}
		public Object getCustomData() {
			return customData;
		}
		
		@Override
		public WorkloadType getType() {
			return FixedTargetWorkloadType.this;
		}
		@Override
		public boolean isComplete() {
			return this.isTerminated();
		}
		
		@Override
		public void doTerminate() {
			try {
				this.terminate.set(true);
				if (this.executor != null) {
					this.executor.shutdown();
					this.executor.awaitTermination(1, TimeUnit.DAYS);
				}
			} catch (InterruptedException e) {
			}
		}

		public void execute(int numThreads, int target, ExecuteTask runner) {
			this.executor = Executors.newFixedThreadPool(numThreads);
			this.startTime = System.currentTimeMillis();
			for (int i = 0; i < numThreads; i++) {
				WorkerThread worker = new WorkerThread(i, terminate, completedCounter, startedCounter, target, customData, getTimerService(), runner, this.getWorkloadOrdinal());
				executor.submit(worker);
			}
		}
		
		public double getPercentComplete() {
			if (terminate.get()) {
				return 100.0;
			}
			else if (target <= 0) {
				return 0.0;
			}
			else {
				return Math.min(100.0, completedCounter.get() * 100.0 / target);
			}
		}
		
		public long getTimeRemainingEstimateInMs() {
			long now = System.currentTimeMillis();
			double percentComplete = getPercentComplete();
			if (percentComplete >= 100.0) {
				return 0L;
			}
			long elapsedTime = now - startTime;
			return (long)((100.0*elapsedTime/percentComplete) - elapsedTime);
		}
		
		@Override
		protected TimerResult doAugmentTimingResult(TimerResult result) {
			return new FixedTargetTimerResult(result, getPercentComplete(), getTimeRemainingEstimateInMs(), completedCounter.get(), target);
		}
	}
	
	@Override
	public String getTypeName() {
		return "FIXED_TARGET";
	}

	@Override
	public FixedTargetWorkloadInstance createInstance(TimerService timerService, WorkloadManager workloadManager) {
		FixedTargetWorkloadInstance result = new FixedTargetWorkloadInstance(timerService);
		workloadManager.registerWorkloadInstance(result);
		return result;
	}
}
