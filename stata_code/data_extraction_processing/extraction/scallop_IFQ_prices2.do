

use "$data_main/approved_leases_lp_2018_06_25.dta", clear


local date: display %td_CCYY_NN_DD date(c(current_date), "DMY")
global today_date_string = subinstr(trim("`date'"), " " , "_", .)

keep approval_date  quantity value price to_permit from_permit
gen approval_year=yofd(approval_date)
gen approval_month=month(approval_date)

drop if price>10

drop if value==1
drop if value==0
drop if price<=0.20
collapse (sum) quantity value, by(approval_year approval_month to_permit from_permit)
egen tfrom=tag(approval_month approval_year from_permit)
egen tto=tag(approval_month approval_year to_permit)

collapse (sum) quantity value tfrom tto, by(approval_year approval_month)
/*Sept 2010 has only 2 buyers
 */
/* */
replace approval_month=8 if approval_month==9 & approval_year==2010

collapse (mean) quantity value, by(approval_year approval_month)
expand 2 if (approval_month==8  & approval_year==2010), gen(dupl)

replace approval_month=9 if approval_month==8 & approval_year==2010 & dupl==1

gen cal_month=ym(approval_year, approval_month)

tsset cal_month

format cal_month %tm
gen scallop_fy=approval_year
replace scallop_fy=scallop_fy-1 if approval_month<=2
rename dupl obscured
gen price=value/quantity

drop approval*

order cal_month
save "$data_main/scallop_IFQ_prices_$today_date_string.dta", replace 

/*
use "/home/mlee/Documents/projects/scallop IFQ project/data folder/approved_leases3_2016_08_08.dta", clear
global my_workdir "/home/mlee/Documents/projects/Birkenbach/data_folder"

keep approval_date approval_year approval_month quantity value price to_permit from_permit

drop if price>10

drop if value==1
drop if value==0
drop if price<=0.20
collapse (sum) quantity value, by(approval_year approval_month to_permit from_permit)
egen tfrom=tag(approval_month approval_year from_permit)
egen tto=tag(approval_month approval_year to_permit)

collapse (sum) quantity value tfrom tto, by(approval_year approval_month)
/*Sept 2010 has only 2 buyers
  Aug 2016  has 1 seller and 2 buyers
 */
/* */
replace approval_month=8 if approval_month==9 & approval_year==2010
replace approval_month=7 if approval_month==8 & approval_year==2016

collapse (mean) quantity value, by(approval_year approval_month)
expand 2 if (approval_month==8  & approval_year==2010) |(approval_month==7  & approval_year==2016), gen(dupl)

replace approval_month=9 if approval_month==8 & approval_year==2010 & dupl==1
replace approval_month=8 if approval_month==7 & approval_year==2016 & dupl==1

gen cal_month=ym(approval_year, approval_month)

tsset cal_month

format cal_month %tm
gen scallop_fy=approval_year
replace scallop_fy=scallop_fy-1 if approval_month<=2
rename dupl obscured
gen price=value/quantity

drop approval*

order cal_month
save $my_workdir/scallop_IFQ_prices.dta, replace 
*/
