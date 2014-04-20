create keyspace if not exists datastax_risk_demo with  replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

use datastax_risk_demo;

create table if not exists risk_sensitivities_hierarchy ( 
	hier_path text,
	sub_hier_path text, 
	risk_sens_name text, 
	value double, 
	PRIMARY KEY (hier_path, sub_hier_path, risk_sens_name)
) WITH compaction={'class': 'LeveledCompactionStrategy'};

truncate risk_sensitivities_hierarchy;
