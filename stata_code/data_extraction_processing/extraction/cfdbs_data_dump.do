#delimit;


clear;
quietly forvalues yr=$firstders/$lastyr{;
	tempfile new5555;
	local dsp1 `"`dsp1'"`new5555'" "'  ;
	odbc load,  exec("select sum(spplndlb) as landings, sum(spplivlb) as live, sum(sppvalue) as value, port, year,  month, day, nespp4,  species_itis, grade_code, market_code  from cfders`yr' 
		where spplndlb is not null and dealnum<>1385 and (dealnum<>4062 and permit<>241397)
		and spplndlb>=1 and sppvalue/spplndlb<=40 
		group by port, year, month, day, nespp4, species_itis, grade_code, market_code ;") $oracle_cxn;
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
