# Realtime Risk Aggregator Demo

This is a demo to show how Cassandra can be used to provide realtime aggregation of risk sensitivities. On a single machine it should process approx 5000 inserts per second while aggregating parts of the hierarchy. 

## The problem

In a financial risk system positions have sensitivity to certain variable. E.g. a bond will be sensitive to the price, a stock will be sensitive to the currency it was bought in etc.
In this example we have picked some common sensitivities and create dummy positions. 

Each positions is associated with a trader, each trader is part of a desk, each desk is part asset type (eg bond, equity) and each asset type has a location. So for each position it will have a hierarchy like Position29's hierarchy is Frankfurt/FX/desk10/trader7

In this demo, the idea is that position prices will be changing all the time so new sensitivity calculations will be inserted into Cassandra at a constant rate. The Aggregator then has too aggregator up the tree to ensure that each level in the tree has a up to date value. 
There is a lag due to duplicate parts of the hierarchy updating more than others

For example 

    We insert Frankfurt/FX/desk10/trader7/Position23 with sensitivities calculations
    We pass Frankfurt/FX/desk10/trader7 to the queue to be aggregated. 
    After all positions for Frankfurt/FX/desk10/trade7 have been aggregated then
    We pass Frankfurt/FX/desk10 to the queue 
    And so on until we get aggregate all asset types for Frankfurt.
   
We want to be able to run the following types of queries

	select * from risk_sensitivities_hierarchy  where hier_path = 'Paris/FX';

	select * from risk_sensitivities_hierarchy  where hier_path = 'Paris/FX/desk4' and sub_hier_path='trader3';

	select * from risk_sensitivities_hierarchy  where hier_path = 'Paris/FX/desk4' and sub_hier_path='trader3' and risk_sens_name='irDelta';


In this demo there are 10 locations, 10 asset types, 20 desks, 20 Traders and 100 positions. This gives 4 millions variations of the hierarchy. Each position can have 10 sensitivities. 

## Running the demo 

You will need a java runtime (preferably 7) along with maven 3 to run this demo. You will need to be comfortable installing and starting Cassandra and DSE (hadoop and solr nodes included).

This demo uses quite a lot of memory so it is worth setting the MAVEN_OPTS to run maven with more memory

    export MAVEN_OPTS=-Xmx512M

## Schema Setup
Note : This will drop the keyspace "dse_demo_analytics" and create a new one. All existing data will be lost. 

To specify contact points use the contactPoints command line parameter e.g. '-DcontactPoints=192.168.25.100,192.168.25.101'
The contact points can take mulitple points in the IP,IP,IP (no spaces).

To create the a single node cluster with replication factor of 1 for standard localhost setup, run the following

    mvn clean compile exec:java -Dexec.mainClass="com.datastax.demo.SchemaSetup"

To run the insert

    mvn clean compile exec:java -Dexec.mainClass="com.heb.finance.analytics.Main" -DstopSize=1000000
		
The stopSize property allows us to specify the number of inserts we want to run. 

To remove the tables and the schema, run the following.

    mvn clean compile exec:java -Dexec.mainClass="com.datastax.demo.SchemaTeardown"
    
    
    
