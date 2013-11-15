package com.heb.finance.analytics;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.springframework.beans.factory.annotation.Autowired;

import com.datastax.driver.core.PreparedStatement;
import com.heb.finance.analytics.model.RiskSensitivity;
import com.sun.istack.NotNull;

public class RiskSensitivityPersisterServiceImpl implements RiskSensitivityPersisterService{ 

	@Autowired
	private RiskSensitivityPathPersister riskSensitivityPathPersister;
	
	private ExecutorService executorService; 
	
	public void setRiskSensitivityPathPersister(RiskSensitivityPathPersister riskSensitivityPathPersister) {
		this.riskSensitivityPathPersister = riskSensitivityPathPersister;
	}

	@SuppressWarnings("serial")
	public RiskSensitivityPersisterServiceImpl(int noOfWorkers) {
		executorService = Executors.newFixedThreadPool(noOfWorkers);
	}
	
	@Override
	public void persist(@NotNull final RiskSensitivity riskSensitivity){
		
		executorService.execute(new Runnable(){
			@Override
			public void run() {
				riskSensitivityPathPersister.batchInsert(riskSensitivity);				
			}			
		});	
	}
}
