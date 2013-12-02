package com.datastax.demo;

import com.datastax.demo.utils.PropertyHelper;


public class SchemaSetupSingle extends SchemaSetup {
		
	public void setUp(){
		
		String keyspace = PropertyHelper.getProperty("keyspace", "dse_demo_analytics");
		
		DROP_KEYSPACE = "DROP KEYSPACE " + keyspace;
		CREATE_KEYSPACE = "CREATE KEYSPACE " + keyspace + " WITH replication = "
				+ "{'class' : 'SimpleStrategy', 'replication_factor' : 1}";
		
		LOG.info ("Running Single Node DSE setup.");
		
		internalSetup();
		
		LOG.info ("Finished Single Node DSE setup.");
	}

	public static void main(String args[]){
		
		SchemaSetupSingle setup = new SchemaSetupSingle();
		setup.setUp();
		setup.shutdown();
	}
}
