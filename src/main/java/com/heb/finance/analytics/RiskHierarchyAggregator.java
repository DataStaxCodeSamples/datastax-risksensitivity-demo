package com.heb.finance.analytics;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import javax.annotation.Resource;

import org.springframework.beans.factory.annotation.Autowired;

import com.heb.finance.analytics.model.RiskSensitivity;

public class RiskHierarchyAggregator {

	@Autowired
	private RiskSensitivityDao riskSensitivityDao;

	@Resource
	private Map<String, Long> timedAggMap;

	private long aggregateCount = 0;
	private final Queue<String> aggQueue = new ArrayBlockingQueue<String>(100000);
	private ScheduledExecutorService execTimed = Executors.newScheduledThreadPool(1);
	private ExecutorService execAggreg = Executors.newSingleThreadExecutor();

	public RiskHierarchyAggregator() {
	}

	public void init() {
		startAggregator();
		startTimeAggMap();
	}

	private void startTimeAggMap() {
		execTimed.scheduleWithFixedDelay(new Runnable() {

			@Override
			public void run() {
				long now = System.currentTimeMillis();
				int counter = 0;

				for (String hierarchyName : timedAggMap.keySet()) {
					Long time = timedAggMap.get(hierarchyName);

					if (time > now) {
						if (!aggQueue.contains(hierarchyName)) {
							aggQueue.offer(hierarchyName);
						}
						timedAggMap.remove(hierarchyName);
						counter++;
					}
				}
				if (counter > 0) {
					System.out.println("Added " + counter + " (" + aggQueue.size() + ")");
				}
			}
		}, 5000, 2000, TimeUnit.MILLISECONDS);
	}

	private void startAggregator() {
		execAggreg.execute(new Runnable(){

			@Override
			public void run() {
				
				while(true){
					String hier = aggQueue.poll();
					
					while(hier != null)	{				
						aggregate(hier);					
						hier = aggQueue.poll();				
						aggregateCount++;
						
						if (aggregateCount%100000 == 0){
							System.out.println("Aggregated " + aggregateCount + " Total Size (" + aggQueue.size() + ")");
						}
					}
					
					System.out.println("Finish Aggregate Run - AggSize : " + aggQueue.size() + " TimedQueueSize : " + timedAggMap.size());
					
					sleep(1000);
				}
			}		
		});
	}

	private void sleep(int millis) {
		try {
			Thread.sleep(millis);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private void aggregate(String hier) {
		List<RiskSensitivity> riskByHier = this.riskSensitivityDao.getRiskByHier(hier);

		// Run some groupings.
		Map<String, Double> pathSensNameMap = new HashMap<String, Double>();

		for (RiskSensitivity riskSens : riskByHier) {
			// Aggregate by riskSens
			String riskSensName = riskSens.getName();
			double value = riskSens.getValue().doubleValue();

			if (pathSensNameMap.containsKey(riskSensName)) {
				double existingValue = pathSensNameMap.get(riskSensName);
				pathSensNameMap.put(riskSensName, value + existingValue);
			} else {
				pathSensNameMap.put(riskSensName, value);
			}
		}

		// Get hierarchy
		String newhierarchyParent = "";
		String newhierarchyChild = hier;

		if (hier.contains("/")) {
			newhierarchyParent = hier.substring(0, hier.lastIndexOf('/'));
			newhierarchyChild = hier.substring(hier.lastIndexOf('/') + 1);
			
			for (String sensName : pathSensNameMap.keySet()) {
				this.riskSensitivityDao.insert(sensName, newhierarchyParent, newhierarchyChild,
						pathSensNameMap.get(sensName));
			}
			
			timedAggMap.put(newhierarchyParent, System.currentTimeMillis() + 1000);
		}
	}
}
