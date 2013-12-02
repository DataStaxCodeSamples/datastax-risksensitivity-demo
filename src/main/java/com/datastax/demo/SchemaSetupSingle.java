package com.datastax.demo;


public class SchemaSetupSingle extends SchemaSetup {
		
	public void setUp(){
		
		DROP_KEYSPACE = "DROP KEYSPACE analytics";
		CREATE_KEYSPACE = "CREATE KEYSPACE analytics WITH replication = "
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
