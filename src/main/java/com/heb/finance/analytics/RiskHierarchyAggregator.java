package com.heb.finance.analytics;

import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class RiskHierarchyAggregator {

	private final RiskSensitivityDao riskSensitivityDao;
	private final Map<String, Long> timedAggMap;
	private final Queue<String> aggQueue = new LinkedList<String>();
	
	private ScheduledExecutorService exec = Executors.newScheduledThreadPool(1); 
	
	public RiskHierarchyAggregator(RiskSensitivityDao riskSensitivityDao, Map<String, Long> timedAggMap){
		this.riskSensitivityDao = riskSensitivityDao;
		this.timedAggMap = timedAggMap;
		
		startTimeAggMap();		
		startAggregator();
	}

	private void startTimeAggMap() {
		
		exec.scheduleWithFixedDelay(new Runnable() {

			@Override
			public void run() {
			}

		}, 500, 500, TimeUnit.MILLISECONDS);
	}
	

	private void startAggregator() {
		// TODO Auto-generated method stub
		
	}
	
}
