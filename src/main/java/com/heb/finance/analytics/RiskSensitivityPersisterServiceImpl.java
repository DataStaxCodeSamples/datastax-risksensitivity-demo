package com.heb.finance.analytics;

import java.util.Map;

import com.heb.finance.analytics.model.RiskSensitivity;
import com.sun.istack.NotNull;

public class RiskSensitivityPersisterServiceImpl implements RiskSensitivityPersisterService{ 

	private RiskSensitivityPathPersister riskSensitivityPathPersister;	
	private Map<String, Long> timedAggMap;

	public RiskSensitivityPersisterServiceImpl(RiskSensitivityPathPersister riskSensitivityPathPersister, Map<String, Long> timedAggMap) {
		this.riskSensitivityPathPersister = riskSensitivityPathPersister;
		this.timedAggMap = timedAggMap;
	}
	
	@Override
	public void persist(@NotNull final RiskSensitivity riskSensitivity){
		
		riskSensitivityPathPersister.insert(riskSensitivity);
		timedAggMap.put(riskSensitivity.getPath(), System.currentTimeMillis() + 10);
	}
}
