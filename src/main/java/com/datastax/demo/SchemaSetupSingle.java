package com.datastax.demo;

import com.datastax.demo.utils.PropertyHelper;


public class SchemaSetupSingle extends SchemaSetup {
		
	public void setUp(){
		
		String keyspace = PropertyHelper.getProperty("keyspace", "dse_demo_analytics");
		
		CREATE_KEYSPACE = "CREATE KEYSPACE if not exists " + keyspace + " WITH replication = "
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
