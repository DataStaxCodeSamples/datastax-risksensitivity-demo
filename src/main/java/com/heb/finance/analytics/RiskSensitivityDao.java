package com.heb.finance.analytics;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.heb.finance.analytics.model.RiskSensitivity;

public class RiskSensitivityDao {

	static final Logger logger = LoggerFactory.getLogger(RiskSensitivityDao.class);
	
	private Cluster cluster;
	private Session session;
	
	private String INSERT = "insert into risk_sensitivities_hierarchy (hier_path, sub_hier_path, risk_sens_name, value) VALUES (?, ?, ?, ?)";
	private String GET = "select * from risk_sensitivities_hierarchy where hier_path = ?";
	
	private PreparedStatement insertStmt;
	private PreparedStatement getStmt;

	private Set<String> hierarchySet = new HashSet<String>();
	
	public RiskSensitivityDao(String clusterName, String[] contactPoints, String keyspace) {

		this.cluster = Cluster.builder().addContactPoints(contactPoints).build();
		this.session = cluster.connect(keyspace);
	
		this.insertStmt = this.session.prepare(INSERT);
		this.getStmt = this.session.prepare(GET);
	}
	
	public void shutdown(){
		logger.info("No of distinct hierarchys : " + hierarchySet.size());
		
		this.cluster.close();
	}

	public void insert(String riskSensitivityName, String hierarchyParent, String position, double value) {
		if (hierarchyParent.equals("")){ 
			System.err.println("Hierarchy is empty ! " + position + "-" + riskSensitivityName);
			return;
		}
		
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
