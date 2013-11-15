package com.heb.finance.analytics;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.heb.finance.analytics.model.RiskSensitivity;

public class RiskSensitivityDao {

	private Cluster cluster;
	private Session session;
	
	private String INSERT = "insert into risk_sensitivities_hierarchy (hier_path, sub_hier_path, risk_sens_name, value) VALUES (?, ?, ?, ?)";
	private String GET = "select * from risk_sensitivities_hierarchy where hier_path = ?";
	
	private PreparedStatement insertStmt;
	private PreparedStatement getStmt;

	private Set<String> hierarchySet = new HashSet<String>();
	
	public RiskSensitivityDao(String clusterName, String url, String keyspace) {

		this.cluster = Cluster.builder().addContactPoints(url).build();
		this.session = cluster.connect(keyspace);
		
		this.insertStmt = this.session.prepare(INSERT);
		this.getStmt = this.session.prepare(GET);
	}

	@Override
	public void finalize(){
		this.shutdown();
	}
	
	public void shutdown(){
		System.out.println("No of distinct hierarchys : " + hierarchySet.size());
		
		this.cluster.shutdown();
	}

	public void insertBatch(String riskSensitivityName, String hierarchyParent, String position, double value) {
		BoundStatement stmt = new BoundStatement(insertStmt);				
		session.execute(stmt.bind(hierarchyParent, position, riskSensitivityName, value));
		hierarchySet.add(hierarchyParent);
	}
	
	public List<RiskSensitivity> getRiskByHier(String hierarchyParent){
		
		List<RiskSensitivity> riskSensitivities = new ArrayList<RiskSensitivity>();
		
		BoundStatement stmt = new BoundStatement(getStmt);				
		ResultSet resultSet = session.execute(stmt.bind(hierarchyParent));
		
		for (Row row : resultSet.all()){
			riskSensitivities.add(mapRow(row));
		}		
		
		return riskSensitivities;
	}

	private RiskSensitivity mapRow(Row row) {
		
		String hier = row.getString("hier_path");
		String position = row.getString("sub_hier_path");
		String name = row.getString("risk_sens_name");
		double value = row.getDouble("value");
		
		return new RiskSensitivity(name, hier, position, new BigDecimal(value));
	}
}
