package com.heb.finance.analytics;

import com.heb.finance.analytics.model.RiskSensitivity;

public interface RiskSensitivityPersisterService {

	public abstract void persist(final RiskSensitivity riskSensitivity);

}