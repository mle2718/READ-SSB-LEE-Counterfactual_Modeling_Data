
# delimit ;
use "$income_mobility/vsh_operator_key_mod.dta", replace ;
keep tripid operator operator_key_modified ;

save $my_workdir/operators_$today_date_string.dta, replace ;
/* pull the data that gives me jops_operator_clean.dta"*/
use "$income_mobility/jops_operator_clean.dta", replace ;


save $my_workdir/jops_operator_clean_$today_date_string.dta, replace ;
