#delimit ;
local date: display %td_CCYY_NN_DD date(c(current_date), "DMY");
global today_date_string = subinstr(trim("`date'"), " " , "_", .);

pause on;

clear;

/* costs*/
odbc load,  exec("select ob.datesail, ob.port, ob.fuelgal, ob.fuelprice, po.portnm, po.stateabb, po.county from obtrp@nova ob, port po where 
year>=2004 and fuelprice is not null and po.port=ob.port;")  $oracle_cxn;

drop if inlist(stateabb,"DE","MD","NC","NK", "VA");



replace datesail=dofc(datesail);
format datesail %td ;
saveold "raw_fuel_prices_$today_date_string.dta", version(11) replace;
gen monthly=mofd(datesail);

collapse (mean) fuelprice, by(stateabb monthly);
format monthly %tm;
saveold "${my_workdir}/monthly_state_fuelprices_$today_date_string.dta", version(11) replace;
