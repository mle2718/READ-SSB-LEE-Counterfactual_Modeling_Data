#delimit;

clear;
local prefix $allprefix;









quietly forvalues yr=$firstders/$lastders{;
	tempfile new5555;
	local dsp1 `"`dsp1'"`new5555'" "'  ;
	clear;
	odbc load,  exec("select sum(spplndlb) as landings, sum(sppvalue) as value, county, nespp3, state, port, month, day, year from cfders`yr' 
		where spplndlb is not null 
		and spplndlb>=1 and sppvalue/spplndlb<=40  
		group by nespp3, state, port, month, day, year, county;") $oracle_cxn;
	renvarlab, lower;
	destring, replace;
	compress;

	
	quietly save `new5555';
};

quietly forvalues yr=$firstdets/$lastdets{;
	tempfile nes321;
	local dsp2 `"`dsp2'"`nes321'" "'  ;
	clear;
	odbc load,  exec("select sum(spplndlb) as landings, sum(sppvalue) as value, county, nespp3, state, port, month, day, year from cfdets`yr' 
		where spplndlb is not null 
		and spplndlb>=1 and sppvalue/spplndlb<=40  
		group by nespp3, state, port, month, day, year, county;") $oracle_cxn;
	renvarlab, lower;
	destring, replace;
	compress;

	
	quietly save `nes321';
};


dsconcat `dsp1' `dsp2';

	renvarlab, lower;
	destring, replace	;
	compress;

gen date=mdy(month, day, year);
format date %td;
compress;
	

/* GROUP THE HAKES TOGETHER PROPERLY */
/* Black Whiting is aggregated into Silver hake.   */
replace nespp3=509 if nespp3==508;
/* Mix Black/Silver is aggregated into Silver hake.   */
replace nespp3=509 if nespp3==507;
replace nespp3 = 365 if nespp3 == 373;

/* In general, I'll discard infeasible prices.  Then I'll aggregate together.  
Then I will discard observations that have quantities that are so small that they are not representative */	

drop if date==. ;
gen pricetemp=value/landings;
drop if pricetemp>= 1.50 & nespp3==168;
drop if pricetemp >7 & ~inlist(nespp3,727,800,159,799,467,789,715,429,81,799,12);
drop if pricetemp ==0;


save "`prefix'$first$lastyr.dta", replace;


/* this is port level pricing */



collapse (sum) value landings, by(port date nespp3); /*replace this line */

gen price1=value/landings; /* and this one */
drop if landings<50;

drop landings value;
label var price1 "port-day price";
compress;

save "`prefix'1.dta", replace;
/* Create the county price dataset*/

use "`prefix'$first$lastyr.dta", replace;
collapse (sum) landings value, by(date state county nespp3);


gen price2=value/landings;
drop if landings<50;

drop landings value;


label var price2 "county-day price";
save "`prefix'2.dta", replace;



/* Create state level prices */
use "`prefix'$first$lastyr.dta", replace;

collapse (sum) landings value, by(date state nespp3);


gen price3=value/landings;
drop if landings<50;

drop landings value;



label var price3 "state-day price";
save "`prefix'3.dta", replace;




/* Create region level prices */
use "`prefix'$first$lastyr.dta", replace;



collapse (sum) landings value, by(date nespp3);




gen price4=value/landings;
label var price4 "region-day price";

drop if landings<50;

drop landings value;
save "`prefix'4.dta", replace;

/* Create port-month prices */
use "`prefix'$first$lastyr.dta", replace;

collapse (sum) landings value, by(month year port nespp3);
gen price5=value/landings;
drop if landings<50;

drop landings value;





drop if price5==0;

label var price5 "port-month prices";
save "`prefix'5.dta", replace;


/* Create county-month prices */

use "`prefix'$first$lastyr.dta", replace;
collapse (sum) landings value, by(month year state county nespp3);

gen price6=value/landings;
drop if landings<50;

drop landings value;
drop if price6==0;




label var price6 "monthly county price";
label data "Monthly County Level Prices";
save "`prefix'6.dta", replace;



/* Create state-month prices */
use "`prefix'$first$lastyr.dta", replace;

collapse (sum) landings value, by(month year state nespp3);
gen price7=value/landings;
drop if landings<50;
drop landings value;
drop if price7 ==0;
label var price7 "monthly state price";
label data "Monthly State Level Prices";
save "`prefix'7.dta", replace;



/* Create region-month prices */
use "`prefix'$first$lastyr.dta", replace;

collapse (sum) landings value, by(month year nespp3);
gen price8=value/landings;
drop if landings<50;

drop if price8 ==0;
drop landings value;

label var price8 "monthly region price";
label data "Monthly Region-wide prices";
save "`prefix'8.dta", replace;

/* Create year-nespp3 prices */
use "`prefix'$first$lastyr.dta", replace;
collapse (sum) landings value, by(year nespp3);
gen price9=value/landings;
drop if landings<50;

drop landings value;




label var price9 "yearly region price by species";

label data "yearly region price";
save "`prefix'9.dta", replace;

/* Create year prices --hopefully everything matches at the MERGE9 level*/
use "`prefix'$first$lastyr.dta", replace;

collapse (sum) landings value, by(year);
gen price10=value/landings;
drop if landings<50;

drop landings value;







label var price10 "yearly region price";

label data "yearly region price all species";
save "`prefix'10.dta", replace;



/* Create nespp3 prices 
use "`prefix'`first'`last'.dta", replace;
save "`prefix'11.dta", replace;

collapse (sum) landings value;
rename landings landings11;
rename value value11;
gen price11=value11/landings11;
label var price11 "nespp4 price";

label data "region price by species";
save "`prefix'11.dta", replace;


 */














