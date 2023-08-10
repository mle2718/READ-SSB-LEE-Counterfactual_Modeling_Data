#delimit;
tempfile t1 t2;
	clear;
	odbc load, exec("select * from sector.transfers;")  $oracle_cxn;  


save `t1', replace;

#delimit;


	clear;
	odbc load, exec("select * from sector.transfer_stock;")  $oracle_cxn;  


save `t2', replace;
use `t1';
merge 1:m transfer_number using `t2', keep(2 3);
assert _merge==3;
drop _merge;

/* Probably want to keep status=C only! Completed, denied, pending, X*/
keep if status=="C";
drop if transfer_number==3369;
replace stock=subinstr(stock,"_"," ",.);

gen stock_area="All Areas";
replace stock_area="GB" if strmatch(stock,"GB*");
replace stock_area="GBE" if strmatch(stock,"GB*East");
replace stock_area="GBW" if strmatch(stock,"GB*West");
replace stock_area="CC/GOM" if strmatch(stock,"CC/GOM*");
replace stock_area="GOM" if strmatch(stock,"GOM*");
replace stock_area="SNE/MA" if strmatch(stock,"SNE/MA");

gen species="Cod" if strmatch(stock,"*Cod*");
replace species="Haddock" if strmatch(stock,"*Haddock*");
replace species="Yellowtail Flounder" if strmatch(stock,"*Yellowtail Flounder*");
replace species="Winter Flounder" if strmatch(stock,"*Winter Flounder*");
replace species="White Hake" if strmatch(stock,"*White Hake*");
replace species="Witch Flounder" if strmatch(stock,"*Witch Flounder*");
replace species=stock if wordcount(stock)==1;
drop transfer_app transfer_ack transfer_init status;
saveold $my_workdir/sector_transfers_$today_date_string.dta, replace version(12);
