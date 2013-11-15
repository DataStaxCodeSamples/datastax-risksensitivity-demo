package com.heb.finance.analytics;

import java.util.Map;

import com.heb.finance.analytics.model.RiskSensitivity;

public class RiskSensitivityPathPersister{

	private final RiskSensitivityDao riskSensitivityDao;
	private final Map<String, Long> timedAggMap;
	
	public RiskSensitivityPathPersister(RiskSensitivityDao riskSensitivityDao, Map<String, Long> timedAggMap){
		this.riskSensitivityDao = riskSensitivityDao;
		this.timedAggMap = timedAggMap;
	}

	public void batchInsert(RiskSensitivity riskSensitivity) {
		
		String hierarchyParent = riskSensitivity.getPath();
		String position = riskSensitivity.getPosition();
		this.riskSensitivityDao.insertBatch(riskSensitivity.getName(), hierarchyParent, position, riskSensitivity.getValue().doubleValue());		
	}
}
