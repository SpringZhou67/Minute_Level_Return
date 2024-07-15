/* Step 1: Set up the library reference */
libname crsp '/wrds/crsp/sasdata/a_stock';
libname scratch '/scratch/wustl/spring_research/';

/* Step 2: Define the date range */
%let start_date = '01APR2014'd;
%let end_date = '30JUN2014'd;

/* Step 3: Query and filter CRSP data */
proc sql;
  create table crsp_data as
  select PERMNO, DATE, ret
  from crsp.dsf
  where DATE between &start_date and &end_date
  order by DATE, PERMNO; 
quit;

/* Step 4: Sort crsp_data by PERMNO and DATE */
proc sort data=crsp_data;
  by PERMNO DATE;
run;

/* Step 5: Merge crsp_data with crsp.dsenames to get TICKER */
data crsp_combined;
  merge crsp_data (in=a)
        crsp.dsenames (where=(trdstat='A' and primexch in ('A', 'N', 'Q')) in=b);
  by PERMNO;
  if a and b; 
run;

/* Step 6: Sort crsp_combined by TICKER */
proc sort data=crsp_combined;
  by TICKER;
run;

/* Step 7: Print summary statistics including TICKER */
proc means data=crsp_combined n mean std min max;
  var ret;
  by TICKER;
  output out=scratch.summary_daily mean=mean std=std min=min max=max;
run;

