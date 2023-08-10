#delimit;

/* Send this to character*/

quietly forvalues yr=$firstders/$lastyr{;
	tempfile gearids;
	local dsp1 `"`dsp1'"`gearids'" "'  ;
	clear;
	odbc load,  exec("select gearid, tripid, subtrip, serial_num, gearcode, mesh, gearqty, gearsize, nhaul,  soakhrs, soakmin, round(nvl(soakhrs,0)+ nvl(soakmin,0)/60,3) as soakhours, 
		depth , carea, tenmsq, qdsq, round(clatdeg+nvl(clatmin,0)/60+nvl(clatsec,0)/3600,8) as latitude,round(clondeg+nvl(clonmin,0)/60+nvl(clonsec,0)/3600,8) as longitude
	from vtr.veslog`yr'g g
	where g.tripid in 
		(select distinct tripid from vtr.veslog`yr't where tripcatg in('1','4'));") $oracle_cxn;
	renvarlab, lower;
	destring, replace;
	compress;

	gen dbyear=`yr';

	quietly save `gearids';
};



dsconcat `dsp1';

	renvarlab, lower;
	destring, replace	;
	compress;

compress;

save $my_workdir/veslog_G$today_date_string.dta, replace ;

quietly forvalues yr=$firstders/$lastyr{;
	tempfile catchids;
	local dsp2 `"`dsp2'"`catchids'" "'  ;
	clear;
	odbc load,  exec("select catch_id, gearid, tripid, subtrip, sppcode, nvl(qtykept, 0) as qtykept, nvl(qtydisc,0) as qtydisc, dealnum, dealname, datesold from vtr.veslog`yr's  s
	where s.tripid in 
		(select distinct tripid from vtr.veslog`yr't where tripcatg in('1','4'));") $oracle_cxn;
	renvarlab, lower;
	destring, replace;
	compress;
	gen dbyear=`yr';
	
	quietly save `catchids';
};



dsconcat `dsp2';

	renvarlab, lower;
	destring, replace	;
	compress;

compress;

save $my_workdir/veslog_S$today_date_string.dta, replace ;



/* Read in the tripids and corrected ports from our processed data, dro out some extraneous stuff.  I'll do a "merge update" to fill in portlnd1 and state1, treating the  processed data as correct if it exists

It may not match if either there were no landings or
use "/home/mlee/Documents/projects/spacepanels/scallop/spatial_project_10192017/veslog_species_huge.dta", clear;

*/

#delimit ;
use "$spacepanels_data", clear;
drop if tripid==.;
drop if dbyear<=2002;
keep tripid portlnd1 state1 geoid namelsad ;
dups, drop terse;
tempfile tports;
drop _expand;
save `tports', replace;


quietly forvalues yr=$firstders/$lastyr{;
	tempfile tripids;
	local dsp3 `"`dsp3'"`tripids'" "'  ;
	clear;
	odbc load,  exec("select tripid, nsubtrip, hullnum, permit, datesail, tripcatg, crew, datelnd1, operator, opernum, portlnd1 as raw_portlnd1, state1 as raw_state1 from vtr.veslog`yr't  t
	where t.tripid in 
		(select distinct tripid from vtr.veslog`yr't where tripcatg in('1','4'));") $oracle_cxn;
	renvarlab, lower;
	destring, replace;
	compress;
	gen dbyear=`yr';

	
	quietly save `tripids';
};



dsconcat `dsp3';

	renvarlab, lower;
	destring, replace	;
	compress;

compress;
/*there's two trips that get filed into the wrong place */
drop if tripid==4864477 & dbyear==2015;

/* this one is actually in 2017 */
drop if tripid==4959274 & dbyear==2018;
replace datesail=clock("21mar2017 16:30:00", "DMY hms") if tripid==4959274 & dbyear==2016;

replace datelnd1=clock("22mar2017 06:00:00", "DMY hms") if tripid==4959274 & dbyear==2016;
replace dbyear=2017 if tripid==4959274 & dbyear==2016;



merge 1:1 tripid using `tports', keep(1 3);
drop _merge;
save $my_workdir/veslog_T$today_date_string.dta, replace ;







