library(dplyr)

scac = read.csv('~/Downloads/scac_gvkey.csv', stringsAsFactors = FALSE) %>% 
  select(defendant_name = Filing.Name, Filing_Date = Filing.Date, Court = District.Court, Exchange, Ticker, 
         gvkey, conm, bad_date_gvkey_link)

patentlawsuits = read.csv('~/Downloads/patent_lawsuits_gvkey.csv', stringsAsFactors = FALSE) %>% 
  select(case_node_id, Case_Title, Civil_Action__, Court = Venue, Filing_Date, patents, Alleged_Infringer, 
         Filing_Name = Patent_Asserter,asserter_id, Asserter_Category, Asserter_Category_Text, defendant_name = individual_defendant, 
         defendant_gvkey = gvkey, bad_date_gvkey_link)

# write.csv(unique(c(scac$gvkey, patentlawsuits$gvkey)), '~/Downloads/available_gvkeys.txt', row.names = FALSE)

date_to_quarter = function(d) {
  switch(format(d, '%b'), 
         "Jan" = 1,
         "Feb" = 1,
         "Mar" = 1,
         "Apr" = 2,
         "May" = 2,
         "Jun" = 2,
         "Jul" = 3,
         "Aug" = 3,
         "Sep" = 3,
         "Oct" = 4,
         "Nov" = 4,
         "Dec" = 4)
}

all_cases = bind_rows(scac, patentlawsuits) 
all_cases$filing_date_formatted = as.Date(all_cases$Filing_Date, format="%m/%d/%y")
all_cases$fyear = format(all_cases$filing_date_formatted, '%Y')
all_cases$fquarter = sapply(all_cases$filing_date_formatted, date_to_quarter)

sec = read.csv('~/Downloads/sec_analytics.csv', stringsAsFactors = FALSE)
sec$filing_date_formatted = as.Date(as.character(sec$lindexdate), format="%Y%m%d")
sec$fyear = format(sec$filing_date_formatted, '%Y')
sec$fquarter = sapply(sec$filing_date_formatted, date_to_quarter)

sec_with_cases = sec %>% select(-filing_date_formatted) %>% left_join(all_cases %>% select(-filing_date_formatted), by=c('gvkey', 'fyear', 'fquarter'))

write.csv(sec_with_cases, '~/Downloads/sec_with_cases.csv', row.names = FALSE)
