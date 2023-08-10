/* live-landed pounds */

#delimit;
pause on;

*log using "AB_extraction.smcl", replace;
timer on 1;

quietly do "/home/mlee/Documents/Workspace/technical folder/do file scraps/odbc_connection_macros.do";
global oracle_cxn "conn("$mysole_conn") lower";
local date: display %td_CCYY_NN_DD date(c(current_date), "DMY");
global today_date_string = subinstr(trim("`date'"), " " , "_", .);

global pass groundfish;




use  species_keyfile_2017_01_18.dta, clear;
gen species=floor(nespp4/10);
keep species nespp4 common_name cf_lndlb_livlb;
bysort species: egen mcf=mode(cf_lndlb_livlb), minmode;
egen t=tag(species);
browse if t==1;
drop nespp4 common cf_lnd;
rename mcf landed_to_live_multiplier;
keep if t==1;
drop t;

tempfile cfs;
save `cfs', replace;


use "/home/mlee/Documents/projects/Birkenbach/aceprice/quarterly_prices.dta", clear;
merge m:1 species using `cfs', keep(1 3);
notes landed_to_live_multiplier: LIVE=landed_to_live_multiplier*landed;
assert _merge==3;
drop _merge;
rename species nespp3;
label var nespp3;

save "/home/mlee/Documents/projects/Birkenbach/aceprice/quarterly_prices2.dta", replace;



use "/home/mlee/Documents/projects/Birkenbach/data_folder/discard_dmis_07_17_2017.dta";
keep stock_id;
bysort stock_id: keep if _n==1;
gen stock2=stock_id;


recode stock2 1 2 3=81 9 10 11=147  20 21 22=123 12=159 13=153 14=250 16=269 17=240 4 5 =125  6 7 8=120  15=124 18=122 19=512, gen(species);
drop stock2;
merge m:1 species using `cfs', keep(1 3);
notes landed_to_live_multiplier: LIVE=landed_to_live_multiplier*landed;

assert _merge==3;
drop _merge;
rename species nespp3;
label var nespp3;

save "/home/mlee/Documents/projects/Birkenbach/aceprice/dmis_landed_to_live.dta", replace;

