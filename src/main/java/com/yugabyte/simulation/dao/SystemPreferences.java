package com.yugabyte.simulation.dao;

public class SystemPreferences {
	private String loggingDir;
	private boolean doLogging;
	
	public String getLoggingDir() {
		return loggingDir;
	}
	public void setLoggingDir(String loggingDir) {
		this.loggingDir = loggingDir;
	}
	public boolean isDoLogging() {
		return doLogging;
	}
	public void setDoLogging(boolean doLogging) {
		this.doLogging = doLogging;
	}
	
	
}
