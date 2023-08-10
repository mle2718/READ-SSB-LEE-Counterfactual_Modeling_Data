#delimit;
clear;

odbc load, exec("select port, portnm, stateabb, county, doc from port ;")  $oracle_cxn; 
renvars, lower;

destring, replace;
notes: made by "port_key_file.do" ;

saveold $my_workdir/cfdbs_port_keyfile_$today_date_string.dta , version(12) replace;
