package com.heb.finance.analytics;


public class Main {
	
	public static void main(String[] args) {		
		try {
			new RiskSensitivityInsertRunner().testPersister();
		} catch (InterruptedException e) {		
			e.printStackTrace();
		};
	}

}
