package com.heb.finance.analytics;

import com.heb.finance.analytics.model.RiskSensitivity;

public class RiskSensitivityPathPersister{

	private RiskSensitivityDao riskSensitivityDao;	
	
	public RiskSensitivityPathPersister(RiskSensitivityDao riskSensitivityDao){
		this.riskSensitivityDao = riskSensitivityDao;
	}

	public void insert(RiskSensitivity riskSensitivity) {
		
		String hierarchyParent = riskSensitivity.getPath();
		String position = riskSensitivity.getPosition();
		this.riskSensitivityDao.insert(riskSensitivity.getName(), hierarchyParent, position, riskSensitivity.getValue().doubleValue());		
	}
}
