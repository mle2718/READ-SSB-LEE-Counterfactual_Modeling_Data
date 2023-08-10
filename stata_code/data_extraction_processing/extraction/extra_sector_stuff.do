

#delimit;
clear;
odbc load,  exec("select * from MQRS.SECTOR_PROFILE;") $oracle_cxn;
