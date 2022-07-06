package com.yugabyte.simulation.services;

import javax.annotation.PostConstruct;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.yugabyte.simulation.dao.SystemPreferences;

@Service
public class SystemPreferencesService {
    
	@Autowired
    private LoggingFileManager loggingManager;

	// For now we will just keep these in memory
	private final SystemPreferences currentPreferences;
	
	public SystemPreferencesService() {
		this.currentPreferences = new SystemPreferences();
		this.currentPreferences.setNetworkRefreshMs(1000);
		this.currentPreferences.setGraphRefreshMs(350);
		this.currentPreferences.setLoggingDir("/tmp");
		this.currentPreferences.setDoLogging(false);		
	}

	@PostConstruct
	private void setLoggingPreferences() {
		this.loggingManager.updateLoggingPreferences(
				this.currentPreferences.isDoLogging(), this.currentPreferences.getLoggingDir());
	}


    public void saveSystemPreferences(SystemPreferences preferences) {
    	loggingManager.updateLoggingPreferences(preferences.isDoLogging(), preferences.getLoggingDir());
    	this.currentPreferences.setNetworkRefreshMs(preferences.getNetworkRefreshMs());
    	this.currentPreferences.setGraphRefreshMs(preferences.getGraphRefreshMs());
    	this.currentPreferences.setDoLogging(preferences.isDoLogging());
    	this.currentPreferences.setLoggingDir(preferences.getLoggingDir());
    }

    public SystemPreferences getSystemPreferences() {
    	return this.currentPreferences;
    }
    
    public void setName(String name) {
    	this.currentPreferences.setWorkloadName(name);
    }
}
