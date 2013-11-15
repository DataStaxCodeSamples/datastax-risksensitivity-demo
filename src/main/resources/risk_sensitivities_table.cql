create table risk_sensitivities_hierarchy ( 
	hier_path text,
	sub_hier_path text, 
	risk_sens_name text, 
	value double, 
	PRIMARY KEY (hier_path, sub_hier_path, risk_sens_name)
) WITH compaction={'class': 'LeveledCompactionStrategy'};

truncate table risk_sensitivities_hierarchy;
insert into risk_sensitivities_hierarchy (hier_path, sub_hier_path, risk_sens_name, value) values ('LONDON', 'BOND', 'irDelta', 1231);
insert into risk_sensitivities_hierarchy (hier_path, sub_hier_path, risk_sens_name, value) values ('LONDON', 'BOND', 'fxDelta', 98);
insert into risk_sensitivities_hierarchy (hier_path, sub_hier_path, risk_sens_name, value) values ('LONDON', 'IRS', 'irDelta', 1226);
insert into risk_sensitivities_hierarchy (hier_path, sub_hier_path, risk_sens_name, value) values ('LONDON', 'IRS', 'irGamma', 646);

insert into risk_sensitivities_hierarchy (hier_path, sub_hier_path, risk_sens_name, value) values ('LONDON/BOND', 'Desk1', 'irDelta', 1231);
insert into risk_sensitivities_hierarchy (hier_path, sub_hier_path, risk_sens_name, value) values ('LONDON/BOND', 'Desk1','fxDelta', 98);
insert into risk_sensitivities_hierarchy (hier_path, sub_hier_path, risk_sens_name, value) values ('LONDON/BOND', 'Desk2', 'irDelta', 601);
insert into risk_sensitivities_hierarchy (hier_path, sub_hier_path, risk_sens_name, value) values ('LONDON/BOND', 'Desk2','fxDelta', 131);
insert into risk_sensitivities_hierarchy (hier_path, sub_hier_path, risk_sens_name, value) values ('LONDON/BOND', 'Desk3', 'irDelta', 76);
insert into risk_sensitivities_hierarchy (hier_path, sub_hier_path, risk_sens_name, value) values ('LONDON/BOND', 'Desk4','fxDelta', 23);
insert into risk_sensitivities_hierarchy (hier_path, sub_hier_path, risk_sens_name, value) values ('LONDON/IRS', 'Desk1','irDelta', 1226);
insert into risk_sensitivities_hierarchy (hier_path, sub_hier_path, risk_sens_name, value) values ('LONDON/IRS', 'Desk1','irGamma', 646);

insert into risk_sensitivities_hierarchy (hier_path, sub_hier_path, risk_sens_name, value) values ('US/BOND', 'Desk1', 'irDelta', 621);
insert into risk_sensitivities_hierarchy (hier_path, sub_hier_path, risk_sens_name, value) values ('US/BOND', 'Desk1','fxDelta', 3);
insert into risk_sensitivities_hierarchy (hier_path, sub_hier_path, risk_sens_name, value) values ('US/IRS', 'Desk1','irDelta', 353);
insert into risk_sensitivities_hierarchy (hier_path, sub_hier_path, risk_sens_name, value) values ('US/IRS', 'Desk1','irGamma', 6245);