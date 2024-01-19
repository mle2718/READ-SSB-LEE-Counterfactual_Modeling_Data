/*  Import and process quarterly ACE prices that comes from 
\READ-SSB-Lee-QuotaPriceReplication\data_folder\main\inter_prices_qtr_${vintage_string}.dta
Also requires
from the aceprice project stock_codes....dta

*/


#delimit cr
version 15.1
pause off
mat drop _all
est drop _all
global vintage_string 2024_01_19

local out_coefficients "C:\Users\min-yang.lee\Documents\READ-SSB-Lee-QuotaPriceReplication\data_folder\main\inter_prices_qtr_${vintage_string}.dta" 

use `out_coefficients', clear
keep fy q_fy var stock_name Estimate std_error n
rename fy fishing_year 
rename q_fy quarter_of_fy
rename Estimate aceprice_nominal 
rename std_error se 
rename n trades

expand 3
sort stock_name fishing_year quarter
bysort stock_name fishing_year (quarter): gen month_of_fy=_n
gen month=month_of_fy+4
replace month=month-12 if month>=13

gen year=fishing_year
replace year=year+1 if month<=4

order year month fishing_year quarter 
label values var

rename var stockcode

merge  m:1 stockcode using $data_main\stock_codes.dta, keep(1 3)
/* _2 are stockcodes that are not allocated */
/* merge=1 should be intercept */
drop if _merge==1
drop if stock_name=="NA"

/*
merge m:1 month year using "$working\PPI_seafood.dta", keep(1 3)

gen aceprice=round(aceprice_nominal/ppi_jan2016,.01)
drop ppi_jan2016
gen spstock=trim(subinstr(stockname," ","_",.)) if stockarea=="Unit"
 NAMES/STOCKS MUST MATCH THE FOLLOWING
American_Plaice_Flounder FLOUNDER, AMERICAN PLAICE /DAB U
spstock sppname stock
Cod_GB COD GB
Cod_GOM COD GOM
Haddock_GB HADDOCK GB
Haddock_GOM HADDOCK GOM
Pollock POLLOCK U
Redfish REDFISH / OCEAN PERCH U
White_Hake HAKE, WHITE U
Winter_Flounder_GB FLOUNDER, WINTER / BLACKBACK GB
Winter_Flounder_GOM FLOUNDER, WINTER / BLACKBACK GOM
Witch_Flounder FLOUNDER, WITCH / GRAY SOLE U
Yellowtail_Flounder_CCGOM FLOUNDER, YELLOWTAIL CCGOM
Yellowtail_Flounder_GB FLOUNDER, YELLOWTAIL GB
Yellowtail_Flounder_SNEMA FLOUNDER, YELLOWTAIL SNEMA
*/
gen spstock=trim(subinstr(stock_name," ","_",.)) if stockarea=="Unit"

replace spstock="American_Plaice_Flounder" if stock_name=="Plaice"
replace spstock="Haddock_GOM" if stock_name=="GOM_haddock"
replace spstock="Haddock_GBE" if stock_name=="GB_Haddock_East"
replace spstock="Haddock_GBW" if stock_name=="GB_Haddock_West"
replace spstock="Yellowtail_Flounder_SNEMA" if stock_name=="SNE/MA_Yellowtail_Flounder"

replace spstock="Yellowtail_Flounder_GB" if stock_name=="GB_Yellowtail_Flounder"
replace spstock="Yellowtail_Flounder_CCGOM" if stock_name=="CC/GOM_Yellowtail_Flounder"

replace spstock="Winter_Flounder_GOM" if stock_name=="GOM_Winter_Flounder"
replace spstock="Winter_Flounder_SNEMA" if stock_name=="SNE/MA_Winter_Flounder"
replace spstock="Winter_Flounder_GB" if stock_name=="GB_Winter_Flounder"
replace spstock="Cod_GOM" if stock_name=="GOM_Cod"
replace spstock="Cod_GBE" if stock_name=="GB_Cod_East"
replace spstock="Cod_GBW" if stock_name=="GB_Cod_West"

replace spstock="Cod_GBW" if stock_name=="GB_Cod_West"

drop _merge
sort year month fishing_year quarter_of_fy spstock stock_name aceprice_nominal se trades stockcode stock_id stock nespp3 stockarea 
save "${data_main}\aceprices_ml.dta", replace
