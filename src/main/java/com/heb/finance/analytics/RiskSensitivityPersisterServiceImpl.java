package com.heb.finance.analytics;

import java.util.Map;

import javax.annotation.Resource;

import org.springframework.beans.factory.annotation.Autowired;

import com.heb.finance.analytics.model.RiskSensitivity;
import com.sun.istack.NotNull;

public class RiskSensitivityPersisterServiceImpl implements RiskSensitivityPersisterService{ 

	@Autowired
	private RiskSensitivityPathPersister riskSensitivityPathPersister;
	
	@Resource
	private Map<String, Long> timedAggMap;

	public RiskSensitivityPersisterServiceImpl() {
	}
	
	@Override
	public void persist(@NotNull final RiskSensitivity riskSensitivity){
		
		riskSensitivityPathPersister.insert(riskSensitivity);
		timedAggMap.put(riskSensitivity.getPath(), System.currentTimeMillis() + 1000);
	}
}
