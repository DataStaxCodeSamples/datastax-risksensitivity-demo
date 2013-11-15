package com.heb.finance.analytics;

import org.junit.Test;

import com.heb.finance.analytics.RiskSensitivityDao;


public class RiskSensitivityDaoTest {

	@Test
	public void testSetup(){
		RiskSensitivityDao riskSensitivityDao = new RiskSensitivityDao("MyCluster", "localhost", "Analytics");
		
		//riskSensitivityDao.insert("irDelta", "Test", 1l);
	}

}
