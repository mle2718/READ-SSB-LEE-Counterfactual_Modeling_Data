#delimit;

use $my_workdir/veslog_T$today_date_string.dta, replace ;
cap drop _merge;
rename portlnd1 portlnd1_clean;
rename state1 state1_clean;

rename raw_portlnd1 portlnd1;
rename raw_state1 state1;

merge m:1 state1 portlnd1 using "$my_workdir/port_geoid_keyfile_$today_date_string.dta", update;
drop if _merge==2;
replace portlnd1_clean=portlnd1 if _merge==4;
replace state1_clean=state1 if _merge==4;

drop portlnd1 state1;
rename portlnd1_clean portlnd1;
rename state1_clean state1;
drop _merge ;
drop namelsad;

save $my_workdir/veslog_T$today_date_string.dta, replace ;
