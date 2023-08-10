#delimit;

/* Processed data subset */

use "$spacepanels_data", clear;


*keep if dbyear>=2003;
drop LADAS GC NGOM IFQ INC Nopermit_scal revenue1 adj_qtykept state_xfactor;
compress;
notes drop _dta in 5;
save $my_workdir/processed_trips_$today_date_string.dta, replace;

use "$spacepanels_data", clear;
gen mark=1;
collapse (sum) mark, by(state1 portlnd1 geoid namelsad);
drop mark;
replace state1=ltrim(itrim(rtrim(state1)));
replace portlnd1=ltrim(itrim(rtrim(portlnd1)));
drop if portlnd1=="OTHER NY" & geoid==.;
drop if portlnd1=="OTHER ME" & geoid==.;

bysort portlnd1 state1: assert _N==1;
save $my_workdir/port_geoid_keyfile_$today_date_string.dta, replace;
