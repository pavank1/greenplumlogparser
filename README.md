# greenplumlogparser
This log parser workson Greenplum logs or Postgresql logs when full logging is enabled and query response time is also enbled. logging should not be in csv format.
This code is greenplum spcecifc,so it expects query queue name. It also calculates cost of query by running explain plan for the query.
It categorizes query based on the tables used in the query. It uses a conf file for categorizing.
