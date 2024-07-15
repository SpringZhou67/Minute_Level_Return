Unitial tasks are:

1. Build a standard daily returns panel based on CRSP with the usual filters. This data will be our benchmark for comparison.
2. Report summary stats for this data and see it makes sense.
3. Build a minute level returns panel for each day-stock in TAQ. Because the TAQ data is large, the way to do this is to use the WRDS cloud and queue separate jobs for each day which save the result to a file, say taqret20240620
4. Combine the daily files of minute-level returns to a single taqret dataset (if size allows).
5. Merge this data with the CRSP events data to account for dividends, repurchases, splits, delistings, etc. This could require adding an overnight return observation.
6. Report summary stats for this minute level data and compare against the CRSP dataset
7. Do more rigorous comparisons to the CRSP daily dataset of a version of the minute level data cumulated to the daily frequency. If we did this correctly, they should be very close.
