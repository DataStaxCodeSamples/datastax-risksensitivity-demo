package com.heb.finance.analytics;

import java.math.BigDecimal;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;

import com.datastax.demo.utils.PropertyHelper;
import com.heb.finance.analytics.model.RiskSensitivity;

public class RiskSensitivityInsertRunner {

	private static final Logger LOG = Logger.getLogger("RiskSensitivityInsertRunner");
	
	private static final String MAX_SIZE = "1000000";

	private static long STOP_COUNTER = 0;

	private static String[] roots = { "London", "New York", "Hong Kong", "Singapore", "Tokyo", "Paris", "Frankfurt", "Sydney", "Chicago", "Madrid" };
	private static String[] divisions = { "FX", "Equity", "Equity Derivatives", "Bonds", "IRS", "Futures", "CDS", "ABS", "Funds", "Commodities" };
	private static String[] sensitivities = { "irDelta", "irGamma", "irVega", "fxDelta", "fxGamma", "fxVega", "crDelta", "crGamma",
			"crVega", "maturity" };
	private static String DESK = "desk";
	private static String POSITION = "position";
	private static String TRADER = "trader";
	private static Set<String> noOfDistinctPaths = new HashSet<String>();

	private RiskSensitivityPersisterService riskSensitivityPersisterService;
		
	public RiskSensitivityInsertRunner(){
		
		String stopSizeStr = PropertyHelper.getProperty("stopSize", MAX_SIZE);
		STOP_COUNTER = Long.parseLong(stopSizeStr);
		
		String keyspace = PropertyHelper.getProperty("keyspace", "datastax_risk_demo");
		String contactPoints = PropertyHelper.getProperty("contactPoints", "127.0.0.1");
		String clusterName = PropertyHelper.getProperty("clusterName", "Test Cluster");
		
		RiskSensitivityDao dao = new RiskSensitivityDao(clusterName, contactPoints.split(","), keyspace);
		RiskSensitivityPathPersister riskSensitivityPathPersister = new RiskSensitivityPathPersister(dao);
		
		//Shared map for Insertion and aggregation
		Map<String, Long> timedAggMap = new ConcurrentHashMap<String, Long>();
		
		riskSensitivityPersisterService = new RiskSensitivityPersisterServiceImpl(riskSensitivityPathPersister, timedAggMap);
		
		RiskHierarchyAggregator aggregator = new RiskHierarchyAggregator(dao, timedAggMap);
		aggregator.init();
	}
	
	public void testPersister() throws InterruptedException {

		long counter = 0;
		long start = System.currentTimeMillis();

		while (true) {
			RiskSensitivity object = createNewRandomRiskSensitivity();

			riskSensitivityPersisterService.persist(object);

			object = null;
			counter++;

			if (counter % 10000 == 0) {
				LOG.info("Processed " + counter);
				sleep(20);
			}

			if (counter == STOP_COUNTER) {
				break;
			}
		}
		long end = System.currentTimeMillis();

		LOG.info(counter + " done in " + (end - start) + "ms.");
		LOG.info(counter / ((end - start) / 1000) + " per second.");
		LOG.info("No of distinct paths " + noOfDistinctPaths.size());

		noOfDistinctPaths = null;
		
		sleep(5000);
		System.exit(0);
	}

	private void sleep(long millis) {
		try {
			Thread.sleep(millis);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	private RiskSensitivity createNewRandomRiskSensitivity() {

		String root = roots[(int) (Math.random() * roots.length)];
		String division = divisions[(int) (Math.random() * divisions.length)];
		String sensitivity = sensitivities[(int) (Math.random() * sensitivities.length)];
		String desk = DESK + new Double(Math.random() * 20).intValue();
		String trader = TRADER + new Double(Math.random() * 20).intValue();
		String position = POSITION  + new Double(Math.random() * 100).intValue();

		String path = root + "/" + division + "/" + desk + "/" + trader;

		noOfDistinctPaths.add(path);

		return new RiskSensitivity(sensitivity, path, position, new BigDecimal(Math.random()*10));
	}

}
