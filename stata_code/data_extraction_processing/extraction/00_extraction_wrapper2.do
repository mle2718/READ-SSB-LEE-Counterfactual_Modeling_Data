/*This extracts data for Anna.  There is a collection of do files that does the actual work, this is essentially a wrapper and table of contents. 
 */

#delimit ;

 
global my_projdir "C:/Users/Min-Yang.Lee/Documents/READ-SSB-Lee-Counterfactual_Modeling_Data";

global oracle_cxn " $mysole_conn";
global my_codedir "${my_projdir}/stata_code/data_extraction_processing/extraction";
global data_main  "${my_projdir}/data_folder/main";
global results  "${my_projdir}/results";

global processing_code "${my_projdir}/stata_code/data_extraction_processing/processing";




pause off;

log using "${results}/AB_extraction2.smcl", replace;
timer on 1;

local date: display %td_CCYY_NN_DD date(c(current_date), "DMY");
global today_date_string = subinstr(trim("`date'"), " " , "_", .);

global pass groundfish;


global firstyr 2004;
global secondyr =$firstyr+1;

global lastyr 2022;
global firstders 2004;
/*
do "${my_codedir}/observer_fuel_prices.do";


do "${my_codedir}/discards.do";

do "${my_codedir}/dmis_trips.do";

*/

/*
See 
https://github.com/NEFSC/READ-SSB-Lee-reanalysis
For code to extract the wind and storm data from NARR.

*/



/*  ACE Prices
You need to run the R code that constructs this:

\READ-SSB-Lee-QuotaPriceReplication\data_folder\main\inter_prices_qtr_${vintage_string}.dta
Also requires
from the aceprice project stock_codes....dta

*/
/*
do "${processing_code}/process_quarterly_ACE_prices.do";
*/

do "${my_codedir}/scallop_IFQ_prices3.do";




log close;



