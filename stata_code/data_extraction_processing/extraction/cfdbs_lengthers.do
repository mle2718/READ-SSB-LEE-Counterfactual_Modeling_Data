#delimit;

/* Send this to character*/

quietly forvalues yr=$firstders/$lastyr{;
	tempfile lengthers;
	local dsp12 `"`dsp12'"`lengthers'" "'  ;
	clear;
	odbc load,  exec("select sum(numlen) as count, length, nespp4, permit from cflen`yr' 
group by length, nespp4, permit;") $oracle_cxn;
	renvarlab, lower;
	destring, replace;
	compress;

	gen dbyear=`yr';

	quietly save `lengthers';
};





dsconcat `dsp12';

	renvarlab, lower;
	destring, replace	;
	compress;

compress;

saveold $my_workdir/cfdbs_length_key$today_date_string.dta, replace version(12);







