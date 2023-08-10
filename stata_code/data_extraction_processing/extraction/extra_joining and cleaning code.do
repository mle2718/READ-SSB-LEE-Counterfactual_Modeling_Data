
/* make a table of nespp3 and 4 */
tempfile nespp34;
odbc load, exec("select distinct nespp3, nespp4, sppcode from vlsppsyn;") $oracle_cxn;   
destring, replace ;
renvarlab, lower ;
duplicates drop (sppcode), force  ;
save `nespp34', replace ;
clear; 



/* Fix WHITE, SILVER, and BLack Hake data errors */
replace sppcode="SHAK" if sppcode=="WHAK" & mesh<=3.5 ;
replace sppcode="SHAK" if sppcode=="HAKOS" | sppcode=="WHB"  ;
drop mesh gearqty gearcode;
collapse (sum) qtykept, by(dealnum sppcode state1 portlnd1 dbyear permit port tripid datesell);
append using "veslog_species.dta";
save "veslog_species.dta", replace;




/*Geret's edits in next two lines*/
replace sppcode = "BAR" if sppcode == "HAGB" & tripid == 2787176;
replace sppcode = "SKATE" if sppcode == "SKLARGE" & inlist(permit, 125520, 146679, 146669,125520);

/* Min-Yang's -- more data cleaning of "sppcodes" that do to match or are just wrong */
#delimit ;
replace qtykept=qtykept*50/8.33 if sppcode=="SCBB" & permit==109652 & tripid==479812;
replace sppcode="SCAL" if sppcode=="SCBB" & permit==109652 & tripid==479812;
replace sppcode="SCUP" if sppcode=="P" & permit==800129;
replace sppcode="FLSD" if sppcode=="FLSP";





/* Convert surfclam bushels to meat weights  17lbs of meats per bushel
  Convert surfclam (lbs) to mean weights (5.24 lb whole= 1 lb meat)

  Convert ocean quahog to meat weights (10 lbs of meats per bushel)
  convert oq to meat weights 11 lbs of meats/bushel if maine)

  convert oq to meat weights if reported in lbs (7.51 lb whole=1lb meat)
 */


replace qtykept=qtykept*17 if sppcode=="CLSUB" & qtykept<=10000 ;
replace qtykept=qtykept/5.24 if sppcode=="CLSU";

replace qtykept=qtykept*10 if sppcode=="CLQUB";
replace qtykept=qtykept*11 if sppcode=="CLQUB" & state1=="ME";

replace qtykept=qtykept/7.51 if sppcode=="CLQU";

drop if inlist(sppcode,"CLSU","CLSUB","CLQUB","CLQU") & state1~="ME";











 /***********A FEW SCALG CORRECTIONS*******************************/
replace sppcode="SCALB" if permit==240954 & tripid==3296229 & sppcode=="SCALG";   /* This was SCALB incorrectly coded as SCALG, so renamed and then dealt with later.*/
replace sppcode="SCAL" if permit==221273 & tripid==2187129 & sppcode=="SCALG";
replace sppcode="SCAL" if permit==121685 & tripid==2045517 & sppcode=="SCALG";
replace sppcode="SCAL" if permit==221555 & tripid==610575 & sppcode=="SCALG";   /************this is the only vessel in our states that had a gallon of scallops********/
replace qtykept=qtykept*8.3 if permit==221555 & tripid==610575 & sppcode=="SCALG";
/***************end SCALG corrections**************/


replace sppcode="SQL" if permit==410349 & tripid==3210591 & qtykept==11000 & sppcode=="SCAL";




replace sppcode="RED" if strmatch(sppcode, "REDG")==1;
merge m:1 sppcode using `nespp34', keep(1 3) ; /********get nespp3/4*********/   
assert _merge==3;
drop _merge;


rename nespp3 myspp;
replace myspp=509 if myspp==508 | myspp==507;







/*Changing VTR nespp3 numbers to be consisent with CFDBS numbers*/
replace nespp3 = 365 if nespp3 == 373;
compress;
/*Cancer crab seems to be Atlantic Rock crab, so replacing cancer crab price, which doesn't exist in CFDBS, for Atlantic Rock crab*/
replace nespp3 = 712 if nespp3 == 714;

/*A small number of people seem to be reporting calico scallop instead of sea scallop*/
replace sppcode = "SCAL" if sppcode == "SCC" & dbyear>=2005;
replace nespp3 = 800 if nespp3 == 797 & dbyear>=2005;
/*We still need to check if the SCC VTR reports from pre-2005 are "wrong"*/

/*********now merge in prices
This merges prices for everything. 
**************/


merge m:1 nespp3 port date using "`prefix'1.dta", keep (1 3) nogenerate ;

merge m:1 nespp3 state county date using "`prefix'2.dta", keep (1 3) nogenerate ;
replace price1=price2 if price1==.;

merge m:1 nespp3 state date using "`prefix'3.dta", keep (1 3) nogenerate ;
replace price1=price3 if price1==.;

merge m:1 nespp3 date using "`prefix'4.dta", keep (1 3) nogenerate ;
replace price1=price4 if price1==.;

merge m:1 nespp3 port year month using "`prefix'5.dta", keep (1 3) nogenerate ;
replace price1=price5 if price1==.;

merge m:1 nespp3 state county year month using "`prefix'6.dta", keep (1 3) nogenerate ;
replace price1=price6 if price1==.;

merge m:1 nespp3 state month year using "`prefix'7.dta", keep (1 3) nogenerate ;
replace price1=price7 if price1==.;

merge m:1 nespp3 month year using  "`prefix'8.dta", keep (1 3) nogenerate ;
replace price1=price8 if price1==.;

/*use the yearly average price for all species, but use $0.15 for herring instead of the yearly price*/
merge m:1 nespp3 year using  "`prefix'9.dta", keep (1 3) nogenerate ;
replace price1=price9 if price1==. & nespp3~=168;
replace price1=0.15 if price1==. & nespp3==168;

compress;



drop if nespp3==662 ; /**********spotted hake****************/
rename nespp3 myspp;

/* This is a good place to fix monkfish */

preserve;
tempfile monk_fix;

keep if myspp==012;
local monk_prefix $monk_prefix;
/*merge monk_prices here.*/
merge m:1 sppcode port date using "`monk_prefix'1.dta", keep (1 3) nogenerate ;


merge m:1 sppcode state county date using "`monk_prefix'2.dta", keep (1 3) nogenerate ;
replace price1=price2 if price1==.;

merge m:1 sppcode state date using "`monk_prefix'3.dta", keep (1 3) nogenerate ;
replace price1=price3 if price1==.;

merge m:1 sppcode date using "`monk_prefix'4.dta", keep (1 3) nogenerate ;
replace price1=price4 if price1==.;

merge m:1 sppcode port year month using "`monk_prefix'5.dta", keep (1 3) nogenerate ;
replace price1=price5 if price1==.;

merge m:1 sppcode state county year month using "`monk_prefix'6.dta", keep (1 3) nogenerate ;
replace price1=price6 if price1==.;

merge m:1 sppcode state month year using "`monk_prefix'7.dta", keep (1 3) nogenerate ;
replace price1=price7 if price1==.;

merge m:1 sppcode month year using  "`monk_prefix'8.dta", keep (1 3) nogenerate ;
replace price1=price8 if price1==.;
/*use the yearly average price for all species, but use $0.15 for herring instead of the yearly price*/
merge m:1 sppcode year using  "`monk_prefix'9.dta", keep (1 3) nogenerate ;
replace price1=price9 if price1==.;


save `monk_fix';

restore;
drop if myspp==012;
append using `monk_fix';






save "veslog_species.dta", replace ;






gen raw_revenue=qtykept*price1;




/*Deal with missing revenue due to missing prices */
/* pout and wolffishes  -- set this to $1/lb for the missing observations*/
replace raw_revenue=qtykept if (myspp==250 | myspp==512) & raw_revenue==.;



