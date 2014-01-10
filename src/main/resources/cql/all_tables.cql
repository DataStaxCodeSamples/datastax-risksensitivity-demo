use dse_demo_analytics; 

create table risk_sensitivities_hierarchy ( 
	hier_path text,
	sub_hier_path text, 
	risk_sens_name text, 
	value double, 
	PRIMARY KEY (hier_path, sub_hier_path, risk_sens_name)
) WITH compaction={'class': 'LeveledCompactionStrategy'};

truncate risk_sensitivities_hierarchy;
