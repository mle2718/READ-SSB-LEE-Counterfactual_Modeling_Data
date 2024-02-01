#delimit;

clear;
odbc load,  exec("select trip_id as dmis_trip_id, docid as tripid, trip_date, sector_id, mult_mri, permit from  NEFSC_GARFO.APSD_T_SSB_TRIP_CURRENT T
       where  t.mult_year>=2007;") $myNEFSC_USERS_conn;
renvars, lower;
save $data_main/dmis_trips_$vintage_string.dta, replace;