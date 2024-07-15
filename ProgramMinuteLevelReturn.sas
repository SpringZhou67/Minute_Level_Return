%macro dailytaq;
    /* Load parameters */
    %let dateprefix = %sysget(DATEPREFIX);
    %let subsample = %sysget(SUBSAMPLE); 
    %let interval_minutes = 1; /* Define the interval in minutes */

    /* Define fixed parameters */
    %let start_time = '9:30:00't;
    %let end_time = '16:00:00't; 
    
    /* Conditional setup of whereclause, no specific filters right now */
    %if %upcase(&subsample) = TEST %then %do;
        %let whereclause = 1=1;
    %end;
    %else %do;
        %let whereclause = 1; 
    %end;

    /* Print or log the parameters for verification */
    %put DATEPREFIX: &dateprefix;
    %put SUBSAMPLE: &subsample;
    %put WHERECLAUSE: &whereclause;

    /* Retrieve NBBO data */
    libname nbbo '/wrds/nyse/sasdata/taqms/nbbo';
    data DailyNBBO;
        set nbbo.nbbom_&dateprefix:;
        where &whereclause 
            and sym_suffix = '' /* Select common stocks only */
            and ("09:00:00.000000000"t <= time_m <= &end_time); 
        
        format date date9.;
        format time_m TIME20.9;
    run;

    /* Retrieve Trade data */
    libname ct '/wrds/nyse/sasdata/taqms/ct';
    data DailyTrade_raw;
        set ct.ctm_&dateprefix:;
        where &whereclause 
            and sym_suffix = '' /* Select common stocks only */
            and ("09:30:00.000000000"t <= time_m <= &end_time); 
        
        type = 'T'; 
        format date date9.;
        format time_m TIME20.9;
    run;

     /* Aggregate Trade data to minute level */
    data trade2(keep=date time_m sym_root price volume);
        set DailyTrade_raw;
        by sym_root date time_m;

        /* Initialize variables */
        retain last_minute .; 

        /* Output for every minute */
        if first.sym_root or first.date or time_m >= intnx('MINUTE', last_minute, &interval_minutes) then do;
            output;
            last_minute = time_m; /* Update last_minute to current time_m */
        end;
    run;


    /* Clean and prepare NBBO data */
    data NBBO2;
        set DailyNBBO;
        where Qu_Cond in ('A','B','H','O','R','W') /* Filter quote conditions */
            and Qu_Cancel ne 'B' /* Exclude canceled quotes */
            and not (Best_Ask le 0 and Best_Bid le 0) /* Exclude both bid and ask being 0 or missing */
            and not (Best_Asksiz le 0 and Best_Bidsiz le 0) /* Exclude both bid and ask size being 0 or missing */
            and not (Best_Ask = . and Best_Bid = .) /* Exclude both bid and ask being missing */
            and not (Best_Asksiz = . and Best_Bidsiz = .); /* Exclude both bid and ask size being missing */

        /* Create spread and midpoint */
        Spread = Best_Ask - Best_Bid;
        Midpoint = (Best_Ask + Best_Bid) / 2;

        /* Convert bid/ask sizes from round lots to shares */
        Best_BidSizeShares = Best_BidSiz * 100;
        Best_AskSizeShares = Best_AskSiz * 100;

        keep date time_m sym_root Best_Bidex Best_Bid Best_BidSizeShares 
             Best_Askex Best_Ask Best_AskSizeShares Qu_SeqNum Midpoint Spread;
    run;

    /* Sort NBBO2 by sym_root, date, and time_m */
    proc sort data=NBBO2;
        by sym_root date time_m;
    run;

    /* Process NBBO data to identify changes */
    data NBBO2_processed;
        set NBBO2;
        by sym_root date time_m;

        /* Calculate lagged Midpoint */
        lmid = lag(Midpoint);
        if first.sym_root or first.date then lmid = .;
        lm25 = lmid - 2.5;
        lp25 = lmid + 2.5;

        /* Adjust Bid and Ask based on Spread and thresholds */
        if Spread gt 5 then do;
            if Best_Bid lt lm25 then do;
                Best_Bid = .;
                Best_BidSizeShares = .;
            end;
            if Best_Ask gt lp25 then do;
                Best_Ask = .;
                Best_AskSizeShares = .;
            end;
        end;

        /* Keep only the latest record per minute */
        if last.time_m;

        keep date time_m sym_root Best_Bidex Best_Bid Best_BidSizeShares 
             Best_Askex Best_Ask Best_AskSizeShares Qu_SeqNum;
    run;

    /* Sort NBBO2_processed by sym_root, date, and time_m */
    proc sort data=NBBO2_processed;
        by sym_root date time_m;
    run;

    /* Calculate Interval Returns */
    data MinuteReturns_raw;
        merge NBBO2_processed(in=a) trade2(in=b);
        by sym_root date time_m;

        if a and b; /* Only keep records present in both datasets */
    
        /* Calculate logarithmic return */
        if not missing(lag(price)) then do;
            iret = log(price / lag(price));
        end;
        else iret = .; /* Set iret to missing if lag(price) is missing */
    
        /* Retain last_time_m within each sym_root and date group */
        retain last_time_m;
        if first.sym_root or first.date then last_time_m = time_m;
    
        last_time_m = time_m; /* Update last_time_m to current time_m */
    
        keep sym_root date time_m price iret;
    run;

    /* Output the minute return panel */
    libname output '/scratch/wustl/spring_research/minreturn_folder'; 
    data output.MinuteReturns_&dateprefix;
        set MinuteReturns_raw;
    run;

%mend dailytaq;

%dailytaq;
