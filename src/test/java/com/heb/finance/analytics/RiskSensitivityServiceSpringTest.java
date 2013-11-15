package com.heb.finance.analytics;

import java.math.BigDecimal;
import java.util.HashSet;
import java.util.Set;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.heb.finance.analytics.model.RiskSensitivity;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = { "classpath:test-risk-loader-server-spring-config.xml" })
public class RiskSensitivityServiceSpringTest {

	private static final long STOP_COUNTER = 5000000;

	private static String[] roots = { "London", "New York", "Hong Kong", "Singapore", "Tokyo", "Paris", "Frankfurt", "Sydney" };
	private static String[] divisions = { "FX", "Equity", "Equity Derivatives", "Bonds", "IRS", "Futures", "CDS", "ABS" };
	private static String[] sensitivities = { "irDelta", "irGamma", "irVega", "fxDelta", "fxGamma", "fxVega", "crDelta", "crGamma",
			"crVega", "maturity" };
	private static String DESK = "desk";
	private static String POSITION = "position";
	private static String TRADER = "trader";
	private static Set<String> noOfDistinctPaths = new HashSet<String>();

	@Autowired
	private RiskSensitivityPersisterService riskSensitivityPersisterService;


	@Test
	public void testPersister() throws InterruptedException {
		
		//final ConsoleReporter reporter = ConsoleReporter.forRegistry().convertRatesTo(TimeUnit.SECONDS).convertDurationsTo(TimeUnit.MILLISECONDS).build();
		//reporter.start(20, TimeUnit.SECONDS);

		long counter = 0;
		long start = System.currentTimeMillis();

		while (true) {
			RiskSensitivity object = createNewRandomRiskSensitivity();

			riskSensitivityPersisterService.persist(object);

			object = null;
			counter++;

			if (counter % 10000 == 0) {
				System.out.println("Processed " + counter);
				sleep(20);
			}

			if (counter == STOP_COUNTER) {
				break;
			}
		}
		long end = System.currentTimeMillis();

		System.out.println(counter + " done in " + (end - start) + "ms.");
		System.out.println(counter / ((end - start) / 1000) + " per second.");
		System.out.println("No of distinct paths " + noOfDistinctPaths.size());

		noOfDistinctPaths = null;

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
		String desk = DESK + new Double(Math.random() * 40).intValue();
		String trader = TRADER + new Double(Math.random() * 40).intValue();
		String position = POSITION  + new Double(Math.random() * 100).intValue();

		String path = root + "/" + division + "/" + desk + "/" + trader;

		noOfDistinctPaths.add(path);

		return new RiskSensitivity(sensitivity, path, position, new BigDecimal(Math.random()*1000));
	}
}
