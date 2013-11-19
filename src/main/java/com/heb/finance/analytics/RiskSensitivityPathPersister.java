package com.heb.finance.analytics;

import org.springframework.beans.factory.annotation.Autowired;

import com.heb.finance.analytics.model.RiskSensitivity;

public class RiskSensitivityPathPersister{

	@Autowired
	private RiskSensitivityDao riskSensitivityDao;	
	
	public RiskSensitivityPathPersister(){
	}

	public void insert(RiskSensitivity riskSensitivity) {
		
		String hierarchyParent = riskSensitivity.getPath();
		String position = riskSensitivity.getPosition();
		this.riskSensitivityDao.insert(riskSensitivity.getName(), hierarchyParent, position, riskSensitivity.getValue().doubleValue());		
	}
}
