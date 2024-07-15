/* Define libnames for datasets */
libname scratch '/scratch/wustl/spring_research';
libname crsp '/wrds/crsp/sasdata/a_stock';

/* Step 1: Sort datasets */
proc sort data=scratch.combined_minutereturns;
  by sym_root date;
run;

/* Step 2: Merge with crsp.dsenames to obtain permno */
data minutereturns_with_permno;
    merge scratch.combined_minutereturns (in=a)
          crsp.dsenames (rename=(ticker=sym_root));
    by sym_root;

    if a;
run;

/* Step 3: Sort combined_minutereturns_with_permno dataset */
proc sort data=minutereturns_with_permno;
    by permno date;
run;

/* Step 4: Merge datasets */
data temp_merged_data;
    merge minutereturns_with_permno (in=a)
          crsp.dse (rename=(dclrdt=dividend_date rcrddt=record_date) where=(divamt ne .));
    by permno date;

    if a;

    /* Initialize adj_price and adj_iret */
    adj_price = price;
    adj_iret = iret; /* Initialize iret as missing */
   
    /* Calculate adjustment factor for splits */
    if not missing(facpr) then adjust_factor = 1 / facpr;
    else adjust_factor = 1;

    /* Determine ex-dividend date (one day before record date) */
    ex_dividend_date = record_date - 1;

    /* Adjust price for splits and dividends */
    if date = ex_dividend_date then adj_price = price - divamt; /* Adjust for dividend */
    else adj_price = price; /* No dividend adjustment */

    /* Apply split adjustment */
    adj_price = adj_price * adjust_factor;

    /* Calculate minute return adjusted for splits and dividends */
    lag_price = lag(price);
    if lag_price ne . then adj_iret = log(adj_price / lag_price);
    else adj_iret = adj_iret; /* handle missing values */

    drop adjust_factor lag_price ex_dividend_date;
    
    /* Filter for trdstat = 'A' and primexch in ("N", "A", "Q") */
    if trdstat = 'A' and primexch in ('N', 'A', 'Q');
run;

/* Step 4b: Merge with dsedelist to get delisting date */
data scratch.adjusted_minute_panel; 
    merge temp_merged_data (in=a)
          crsp.dsedelist (keep=permno dlstdt);
    by permno;

    if a;

    /* Adjust returns for delisting */
    if date = dlstdt then adj_iret = .; /* Set return to missing on delisting date */

    drop adjust_factor lag_price ex_dividend_date;
run;

/* Step 5: Sort merged dataset if not already sorted */
proc sort data=scratch.adjusted_minute_panel;
  by sym_root date TIME_M; 
run;

/*Optional*/
/* Step 6: Report summary statistics using proc means */
proc means data=scratch.adjusted_minute_panel;
  var adj_iret;
  output out=scratch.summary_iret_all mean=mean_value min=min_value max=max_value std=std_value;
run;

proc means data=scratch.adjusted_minute_panel noprint;
  by sym_root;
  var adj_iret;
  output out=scratch.summary_iret_ticker mean=mean_value min=min_value max=max_value std=std_value /autoname;
run;

/* Add overnight return observations */

/* Step 1: Ensure adjusted_minute_panel is sorted */
proc sort data=scratch.adjusted_minute_panel;
    by sym_root date time_m;
run;

/* Step 2: Calculate Overnight Returns */
data scratch.overnight_returns;
    set scratch.adjusted_minute_panel;
    by sym_root date time_m;

    /* Lagged price and date */
    lag_price = lag(price);
    lag_date = lag(date);
    
    /* Calculate overnight return */
    if first.ticker then do;
        /* Reset values at the start of each ticker */
        lag_price = .;
        lag_date = .;
    end;
    
    if lag_price ne . and lag_date = intnx('day', date, -1) then do;
        /* Calculate overnight return */
        overnight_return = price - lag_price;
        output;
    end;

    drop lag_price lag_date;
run;

/* Step 3: Report summary statistics using proc means */
proc means data=scratch.overnight_returns mean min max std;
    by sym_root;
    var overnight_return;
    output out=scratch.summary_overnight(drop=_type_ _freq_) mean=mean_value min=min_value max=max_value std=std_value;
run;