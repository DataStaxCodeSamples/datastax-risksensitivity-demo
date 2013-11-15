use keyspace Analytics;

create column family risk_sensitivity_aggregator with default_validation_class=CounterColumnType and comparator=UTF8Type;

ASSUME risk_sensitivity_aggregator KEYS AS ascii;

get risk_sensitivity_aggregator['Frankfurt/Equity Derivatives/desk2']

