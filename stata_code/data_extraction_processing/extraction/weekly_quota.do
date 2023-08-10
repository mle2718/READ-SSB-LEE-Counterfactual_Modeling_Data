global my_scraperdir "/home/mlee/Documents/projects/scraper/data_out"

use "$my_scraperdir/allyears.dta", clear


format report_date data_date %td
replace sourcefile=substr(sourcefile,-6,6)
egen ssource=sieve(sourcefile), keep(n)
gen l=length(ssource)
assert l==6
drop sourcefile l
rename ssource sourcefile


foreach var of varlist subaclmt cumulativecatchmt percentcaught cumulativediscardmt cumulativekeptmt subacl{
	egen s`var'=sieve(`var'), char(0123456789.)
	destring s`var', replace
	drop `var'
	rename s`var' `var'
}

decode quota_period, gen(qp)
drop quota
rename qp quota_period
decode title, gen(myt)
drop title
rename myt title


replace stock=subinstr(stock,"*","",.)
replace stock=subinstr(stock,"/","",.)
replace stock=subinstr(stock,"SNE Winter","SNEMA Winter",.)
notes: the data come from the GARFO quota monitoring websites

replace title=ltrim(itrim(rtrim(title)))
replace title="Summary Table Sector Catch Monitoring"
replace subaclmt=subacl if subaclmt==.
drop subacl
 destring quota, replace
save "$my_scraperdir/quota_usage.dta", replace
*export excel using "/home/mlee/Documents/projects/scraper/data_out/quota_monitoring_2010_2017.xlsx", firstrow(variables) replace
