#delimit;


clear;
quietly forvalues yr=$firstders/$lastyr{;
	tempfile new5555;
	local dsp1 `"`dsp1'"`new5555'" "'  ;
	odbc load,  exec("select dealnum, sum(spplndlb) as landings, sum(spplivlb) as live, sum(sppvalue) as value, port, year,  month, day, nespp4,  species_itis, grade_code, market_code  from cfdbs.cfders`yr' 
		where spplndlb is not null and dealnum<>1385 and (dealnum<>4062 and permit<>241397)
		and spplndlb>=1 and sppvalue/spplndlb<=40 
		group by port, year, month, day, dealnum, nespp4, species_itis, grade_code, market_code ;") $myNEFSC_USERS_conn;
	renvarlab, lower;
	quietly save `new5555';
	clear;

};


append using  `dsp1';
	destring, replace;
	compress;

	renvarlab, lower;

gen date=mdy(month, day, year);
format date %td;
compress;

saveold $my_workdir/cfdbs_$today_date_string.dta, replace version(12);

# delimit ;
use $my_workdir/cfdbs_$today_date_string.dta, clear;

gen nespp3=floor(nespp4);
replace nespp3=81 if nespp3==80;


replace nespp3=12 if nespp3==11;
replace nespp3=120 if nespp3==119;
replace nespp3=147 if nespp3==148;
replace nespp3=153 if nespp3==154;
replace nespp3=269 if nespp3==270;

collapse (sum) landings live value, by(year port dealnum nespp3);
/* this is a dataset of landings at the dealer-nespp3-port number level */
save $my_workdir/dealnum_port_nespp3_year_$today_date_string.dta, replace;
drop if dealnum<=5;
