package com.yugabyte.simulation.workload;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yugabyte.simulation.service.SonosWorkload;
import com.yugabyte.simulation.services.ExecutionStatus;
import com.yugabyte.simulation.services.Timer;
import com.yugabyte.simulation.services.TimerService;
import com.yugabyte.simulation.services.TimerType;

import ch.qos.logback.core.recovery.ResilientSyslogOutputStream;

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
	
	private static class ThreadManager implements Runnable {
		private volatile int desiredRate;
		private volatile double currentRate;
		private ExecutorService executor;
		private ExecuteTask runner;
		private final Object customData;
		private final AtomicBoolean terminate = new AtomicBoolean(false);
		private static final int DEFAULT_PROCESSING_TIME_MS = 10;
		private static final int PASS_INTERVAL_MSECS = 3000;
		private static final int WARMUP_INTERVALS = 5;
		private static final double UPPER_TOLERANCE = 1.1;
		private static final double AIMING_TOLERANCE = 1.07;
		private final AtomicInteger threadDelay;
		private final int maxThreads;
		private int currentThreads = 0;
		private final TimerService timerService;
		private final AtomicLong idleTimeCounter;
		private final AtomicInteger transactionCounter;
	    private static final Logger LOGGER = LoggerFactory.getLogger(FixedTargetWorkloadType.class);
		
		public ThreadManager(int desiredRate, int maxThreads, ExecutorService executor, ExecuteTask runner, Object customData, TimerService timerservice) {
			super();
			this.desiredRate = desiredRate;
			this.executor = executor;
			this.runner = runner;
			this.customData = customData;
			this.maxThreads = maxThreads;
			this.currentThreads = 0;
			this.threadDelay = new AtomicInteger();
			this.timerService = timerservice;
			this.idleTimeCounter = new AtomicLong(0);
			this.transactionCounter = new AtomicInteger(0);
		}

		public int getDesiredRate() {
			return desiredRate;
		}
		
		public void setDesiredRate(int desiredRate) {
			this.desiredRate = desiredRate;
		}
		
		public void terminate() throws InterruptedException {
			this.terminate.set(true);
			this.executor.shutdown();
			executor.awaitTermination(1, TimeUnit.DAYS);
		}
		
		public double getCurrentRate() {
			return currentRate;
		}
		
		public int getCurrentThreadCount() {
			return this.currentThreads;
		}
		private int getNeededThreads(int avgDelay) {
			return Math.max(1, Math.min(this.maxThreads, this.desiredRate * avgDelay/ 1000));
		}
		
		private void createAndSubmitThread() {
			if (LOGGER.isDebugEnabled()) {
				LOGGER.debug(String.format("Creating and Submitting a new thread, %d -> %d, rate = %f\n", this.getCurrentThreadCount(), this.getCurrentThreadCount()+1, this.getCurrentRate()));
			}
			this.executor.submit(new WorkerThread(threadDelay, idleTimeCounter, transactionCounter, terminate, runner, customData, timerService));
			this.currentThreads++;			
		}
		
		private void setThreadDelay(int delay) {
			if (LOGGER.isDebugEnabled()) {
				LOGGER.debug("Setting thread delay to " + threadDelay);
			}
			this.threadDelay.set(delay);
		}
		
		@Override
		public void run() {
			// Create the initial threads
			int warmupIntervals = WARMUP_INTERVALS;
			int defaultNumberOfThreads = getNeededThreads(DEFAULT_PROCESSING_TIME_MS);
			if (LOGGER.isDebugEnabled()) {
				LOGGER.debug("Default threads = " + defaultNumberOfThreads);
			}
			this.setThreadDelay(DEFAULT_PROCESSING_TIME_MS);
			for (int i = 0; i < defaultNumberOfThreads; i++) {
				this.createAndSubmitThread();
			}
			
			while (!this.terminate.get()) {
				try {
					Thread.sleep(PASS_INTERVAL_MSECS);
				} catch (InterruptedException e) {
					this.terminate.set(true);
				}
				long idleTimeThisPass = idleTimeCounter.getAndSet(0);
				int transactionsThisPass = transactionCounter.getAndSet(0);
				double tps = transactionsThisPass * (1000.0/PASS_INTERVAL_MSECS);
				double idleTimePerSecond = idleTimeThisPass * (1000.0/PASS_INTERVAL_MSECS);
				this.currentRate = tps;
				
				if (LOGGER.isDebugEnabled()) {
					LOGGER.debug(String.format("tps = %f, desired rate = %d (%f), threads = %d, current rate = %f, idleTimePerSecondPerThread = %f\n", 
						tps, desiredRate, desiredRate *AIMING_TOLERANCE, currentThreads, getCurrentRate(), idleTimePerSecond / this.currentThreads));
				}
				
				if (warmupIntervals > 0) {
					warmupIntervals--;
					System.out.printf("Warming up, %d remaining\n", warmupIntervals);
				}
				else {
					if (tps > desiredRate * UPPER_TOLERANCE) {
						// Going too fast, slow down
						// Allow to be within 5% tolerance
						if (LOGGER.isDebugEnabled()) {
							LOGGER.debug(String.format("Running too fast, %d / %d = %d\n", this.desiredRate, this.currentThreads, this.desiredRate/this.currentThreads));
						}
						this.setThreadDelay(1000 * this.currentThreads / this.desiredRate);
					}
					else if (tps < desiredRate) {
						// we're going too slowly, see if there's any capacity to increase it
						int optimalThreadDelay = Math.floorDiv(1000 * this.currentThreads, (int)(this.desiredRate * AIMING_TOLERANCE));
						int availableTimePerThreadPerSecond = (int)(idleTimePerSecond / this.currentThreads);
	
						if (LOGGER.isDebugEnabled()) {
							LOGGER.debug(String.format("Running too slow, optimal thread delay = %d, current threadDelay = %d, idleTimePerSecondPerThread = %d\n",
								optimalThreadDelay, threadDelay.get(), availableTimePerThreadPerSecond));
						}
						
						if (optimalThreadDelay <= threadDelay.get() && availableTimePerThreadPerSecond >= 100) {
							// There's at least 100ms spare per thread per second, try lowering the thread delay
							if (threadDelay.get() - availableTimePerThreadPerSecond < optimalThreadDelay) {
								this.setThreadDelay(optimalThreadDelay);
							}
							else {
								this.setThreadDelay(this.threadDelay.get() - availableTimePerThreadPerSecond);
							}
						}
						else {
							// We need to increase the threads
							this.createAndSubmitThread();
							this.setThreadDelay(Math.floorDiv(1000 * this.currentThreads, (int)(this.desiredRate * AIMING_TOLERANCE)));						
						}
					}
				}
			}
		}
	}
	
	private static class WorkerThread implements Runnable {
		private final AtomicInteger threadDelay;
		private final ExecuteTask task;
		private final AtomicBoolean terminate;
		private final Object customData;
		private final Timer timer;
		private Object threadData;
		private final AtomicLong idleTimeCounter;
		private final AtomicInteger transactionCounter;
		
		public WorkerThread(AtomicInteger threadDelay, AtomicLong idleTimeCounter, AtomicInteger transactionCounter, AtomicBoolean terminate, ExecuteTask task, Object customData, TimerService timerService) {
			this.threadDelay = threadDelay;
			this.task = task;
			this.terminate = terminate;
			this.customData = customData;
			this.threadData = null;
			this.timer = timerService.getTimer(TimerType.WORKLOAD2);
			this.idleTimeCounter =  idleTimeCounter;
			this.transactionCounter = transactionCounter;
		}
		
		private void sleep(int milliseconds) {
			if (milliseconds <= 1 ) {
				return;
			}
			try {
				Thread.sleep(milliseconds);
			} catch (InterruptedException e) {
				throw new RuntimeException("Interrupted", e);
			}
		}
		
		@Override
		public void run() {
			sleep(ThreadLocalRandom.current().nextInt(threadDelay.get()));
			while (!terminate.get()) {
				timer.start();
				long timeInNs;
				try {
					this.threadData = task.run(customData, threadData);
					timeInNs = timer.end(ExecutionStatus.SUCCESS);
				}
				catch (Exception e) {
					timeInNs = timer.end(ExecutionStatus.ERROR);
				}
				this.transactionCounter.incrementAndGet();
				int idleTime = threadDelay.get() - (int)(timeInNs / 1000000);
				if (idleTime > 0) {
					this.idleTimeCounter.addAndGet(idleTime);
					sleep(idleTime);
				}
			}
		}
	}
	
	public class ThroughputWorkloadInstance extends WorkloadTypeInstance {
		private final ExecutorService executor = Executors.newCachedThreadPool();
		private Object customData = null;
		private TimerService timerService;
		private ThreadManager threadManager;
		private Thread threadManagerThread;
		private int maxThreads = 96;
		
		public ThroughputWorkloadInstance(TimerService timerService) {
			this.timerService = timerService;
		}
		public ThroughputWorkloadInstance setCustomData(Object customData) {
			this.customData = customData;
			return this;
		}
		public Object getCustomData() {
			return customData;
		}
		public ThroughputWorkloadInstance setMaxThreads(int maxThreads) {
			this.maxThreads = maxThreads;
			return this;
		}
		public int getMaxThreads() {
			return maxThreads;
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
		public void terminate() {
			super.terminate();
			
			try {
				this.threadManager.terminate();
			} catch (InterruptedException e) {
			}
		}
		
		public double getCurrentRate() {
			return this.threadManager == null ? 0.0 : this.threadManager.getCurrentRate();
		}
		
		public void execute(int throughputRate, ExecuteTask runner) {
			this.threadManager = new ThreadManager(throughputRate, maxThreads, executor, runner, this.customData, this.timerService);
			this.threadManagerThread = new Thread(threadManager, "Thread Manager for " + this.getWorkloadId());
			this.threadManagerThread.setDaemon(true);
			this.threadManagerThread.setPriority(Thread.MAX_PRIORITY);
			this.threadManagerThread.start();
		}
		
		public int getDesiredRate() {
			return this.threadManager.getDesiredRate();
		}
		
		public void setDesiredRate(int desiredRate) {
			this.threadManager.setDesiredRate(desiredRate);
		}
	}
	
	@Override
	public String getTypeName() {
		return "THROUGHPUT";
	}

	@Override
	public ThroughputWorkloadInstance createInstance(TimerService timerService) {
		return new ThroughputWorkloadInstance(timerService);
	}
}
