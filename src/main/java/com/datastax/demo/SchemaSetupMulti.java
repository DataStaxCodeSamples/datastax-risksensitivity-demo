package com.datastax.demo;

import com.datastax.demo.utils.PropertyHelper;


public class SchemaSetupMulti extends SchemaSetup {

	public void setUp() {
		
		String keyspace = PropertyHelper.getProperty("keyspace", "dse_demo_analytics");
		
		DROP_KEYSPACE = "DROP KEYSPACE " + keyspace;
		CREATE_KEYSPACE = "CREATE KEYSPACE " + keyspace + " WITH replication = "
				+ "{'class' : 'NetworkTopologyStrategy', 'Cassandra' : 3, 'Analytics' : 1, 'Solr' : 1}";
		
		LOG.info ("Starting Multi Center DSE setup.");

		this.internalSetup();
		LOG.info ("Finished Multi Center DSE setup.");
	}


	public static void main(String args[]) {

		SchemaSetupMulti setup = new SchemaSetupMulti();
		setup.setUp();
		setup.shutdown();
	}
}
