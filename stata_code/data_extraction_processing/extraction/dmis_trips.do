#delimit;

clear;
odbc load,  exec("select trip_id, trip_date, sector_id, mult_mri, permit from APSD.t_ssb_trip_current@garfo_nefsc T
       where  t.mult_year>=2007;") $mysole_conn;
renvars, lower;
save $data_main/dmis_trips_$vintage_string.dta, replace;