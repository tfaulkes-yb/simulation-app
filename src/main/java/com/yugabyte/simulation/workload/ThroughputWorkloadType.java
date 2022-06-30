package com.yugabyte.simulation.workload;

import java.lang.reflect.InvocationTargetException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yugabyte.simulation.dao.TimerResult;
import com.yugabyte.simulation.services.ExecutionStatus;
import com.yugabyte.simulation.services.Timer;
import com.yugabyte.simulation.services.TimerService;

public class ThroughputWorkloadType extends WorkloadType {
	
	public interface ExecuteTask {
		public void run(Object customData, Object threadData);
	}

	public static interface CallbackHandler {
		public void invoke(Object customData, Object threadData);
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
		private final CallbackHandler initializationHandler;
		private final CallbackHandler terminationHandler;
		private final Class<?> threadDataClass;
		private final int workloadOrdinal;
	    private static final Logger LOGGER = LoggerFactory.getLogger(ThroughputWorkloadType.class);
		
		public ThreadManager(int desiredRate, int maxThreads, ExecutorService executor, ExecuteTask runner, Object customData, TimerService timerservice, CallbackHandler initializationHandler, CallbackHandler terminationHandler, Class<?> threadDaClass, int workloadOrdinal) {
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
			this.initializationHandler = initializationHandler;
			this.terminationHandler = terminationHandler;
			this.threadDataClass = threadDaClass;
			this.workloadOrdinal = workloadOrdinal;
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
			if (this.currentThreads < this.maxThreads) {
				if (LOGGER.isDebugEnabled()) {
					LOGGER.debug(String.format("Creating and Submitting a new thread, %d -> %d, rate = %f\n", this.getCurrentThreadCount(), this.getCurrentThreadCount()+1, this.getCurrentRate()));
				}
				if (!this.terminate.get()) {
					Object threadData = null;
					if (this.threadDataClass != null) {
						try {
							threadData = this.threadDataClass.getConstructor().newInstance();
						} catch (InstantiationException | IllegalAccessException | IllegalArgumentException
								| InvocationTargetException | NoSuchMethodException | SecurityException e) {
							System.err.printf("Error creating an instance of class %s for thread specific data", this.threadDataClass.getName());
							e.printStackTrace();
							throw new RuntimeException(e);
						}
					}
					this.executor.submit(new WorkerThread(threadDelay, idleTimeCounter, transactionCounter, terminate, runner, customData, timerService, initializationHandler, terminationHandler, threadData, workloadOrdinal));
					this.currentThreads++;
				}
			}
			else {
				LOGGER.warn("Cannot create a new worker thread: current threads = " + this.currentThreads + ", max allowed = " + this.maxThreads);
			}
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
		private final CallbackHandler initializationHandler;
		private final CallbackHandler terminationHandler;
		private final int workloadOrdinal;
	    private static final Logger LOGGER = LoggerFactory.getLogger(ThroughputWorkloadType.class);
		
		public WorkerThread(AtomicInteger threadDelay, AtomicLong idleTimeCounter, AtomicInteger transactionCounter, AtomicBoolean terminate, ExecuteTask task, Object customData, TimerService timerService, CallbackHandler initializationHandler, CallbackHandler terminationHandler, Object threadData, int workloadOrdinal) {
			this.threadDelay = threadDelay;
			this.task = task;
			this.terminate = terminate;
			this.customData = customData;
			this.threadData = threadData;
			this.timer = timerService.getTimer();
			this.idleTimeCounter =  idleTimeCounter;
			this.transactionCounter = transactionCounter;
			this.initializationHandler = initializationHandler;
			this.terminationHandler = terminationHandler;
			this.workloadOrdinal = workloadOrdinal;
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
			if (this.initializationHandler != null) {
				this.initializationHandler.invoke(customData, threadData);
			}
			try {
				long lastError = 0;
				int skippedErrors = 0;
				sleep(ThreadLocalRandom.current().nextInt(threadDelay.get()));
				while (!terminate.get()) {
					timer.start();
					long timeInNs;
					try {
						task.run(customData, threadData);
						timeInNs = timer.end(ExecutionStatus.SUCCESS,this.workloadOrdinal);
					}
					catch (Exception e) {
						timeInNs = timer.end(ExecutionStatus.ERROR, this.workloadOrdinal);
						long now = System.currentTimeMillis();
						if (LOGGER.isDebugEnabled()) {
							LOGGER.debug("Exception thrown executing task " + task.getClass().getName(), e);
						}
						else if (LOGGER.isInfoEnabled() && now-lastError > 2000) {
							if (skippedErrors > 0) {
								LOGGER.info("Skipped "+skippedErrors+ " other errors");
							}
							LOGGER.info("Exception thrown executing task " + task.getClass().getName(), e);
							lastError = now;
							skippedErrors = 0;
						}
						else {
							skippedErrors++;
						}
					}
					this.transactionCounter.incrementAndGet();
					int idleTime = threadDelay.get() - (int)(timeInNs / 1000000);
					if (idleTime > 0) {
						this.idleTimeCounter.addAndGet(idleTime);
						sleep(idleTime);
					}
				}
			}
			finally {
				if (this.terminationHandler != null) {
					this.terminationHandler.invoke(customData, threadData);
				}
			}
		}
	}
	
	public class ThroughputWorkloadInstance extends WorkloadTypeInstance {
		private final ExecutorService executor = Executors.newCachedThreadPool();
		private Object customData = null;
		private ThreadManager threadManager;
		private Thread threadManagerThread;
		private int maxThreads = 1;
		private CallbackHandler threadTerminationHandler = null;
		private CallbackHandler threadInitializationHandler = null;
		private Class<?> threadDataClass;
		
		public ThroughputWorkloadInstance(TimerService timerService) {
			super(timerService);
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
		
		public Class<?> getThreadDataClass() {
			return threadDataClass;
		}
		public ThroughputWorkloadInstance setThreadDataClass(Class<?> threadDataClass) {
			this.threadDataClass = threadDataClass;
			return this;
		}
		public ThroughputWorkloadInstance onThreadTermination(CallbackHandler handler) {
			this.threadTerminationHandler = handler;
			return this;
		}
		
		public ThroughputWorkloadInstance onThreadInitialization(CallbackHandler handler) {
			this.threadInitializationHandler = handler;
			return this;
		}
		
		@Override
		protected void doInitialize() {
		}
		
		@Override
		public WorkloadType getType() {
			return ThroughputWorkloadType.this;
		}
		@Override
		public boolean isComplete() {
			return this.isTerminated();
		}
		
		@Override
		public void doTerminate() {
			try {
				this.threadManager.terminate();
			} catch (InterruptedException e) {
			}
		}
		
		private static final String csvHeader = "Start Time,Min Time Us,Average Time Us,Max Time Us,Num Succeeded,Num Failed\n";
		private static final String csvFormat = "%d,%d,%d,%d,%d,%d\n";
		
		@Override
		public String formatToCsv(TimerResult result) {
			return String.format(csvFormat, result.getStartTimeMs(), result.getMinUs(), result.getAvgUs(),
					result.getMaxUs(), result.getNumSucceeded(), result.getNumFailed());
		}
		
		@Override
		public String getCsvHeader() {
			return csvHeader;
		}
		
		public double getCurrentRate() {
			return this.threadManager == null ? 0.0 : this.threadManager.getCurrentRate();
		}
		
		public ThroughputWorkloadInstance execute(int throughputRate, ExecuteTask runner) {
			this.threadManager = new ThreadManager(throughputRate, maxThreads, executor, runner, this.customData, getTimerService(), this.threadInitializationHandler, this.threadTerminationHandler, this.threadDataClass, this.getWorkloadOrdinal());
			this.threadManagerThread = new Thread(threadManager, "Thread Manager for " + this.getWorkloadId());
			this.threadManagerThread.setDaemon(true);
			this.threadManagerThread.setPriority(Thread.MAX_PRIORITY);
			this.threadManagerThread.start();
			return this;
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
	public ThroughputWorkloadInstance createInstance(TimerService timerService, WorkloadManager workloadManager) {
		ThroughputWorkloadInstance result = new ThroughputWorkloadInstance(timerService);
		workloadManager.registerWorkloadInstance(result);
		return result;
	}
}
