library(dplyr)

scac = read.csv('~/Downloads/scac_filings.csv', stringsAsFactors = FALSE)
scac$filingdate_formatted = as.Date(scac$Filing.Date, format='%m/%d/%y')

ticker_to_gvkey = read.csv('~/Downloads/ticker_to_gvkey.csv', stringsAsFactors = FALSE)
ticker_to_gvkey$LINKDT_formatted = as.Date(as.character(ticker_to_gvkey$LINKDT), format='%Y%m%d')
ticker_to_gvkey$LINKENDDT_formatted = as.Date(as.character(ticker_to_gvkey$LINKENDDT), format='%Y%m%d')

linktable = ticker_to_gvkey %>% select(Ticker = TIC, gvkey, conm, LINKDT = LINKDT_formatted, LINKENDDT = LINKENDDT_formatted) %>% unique()
scac_with_gvkey = left_join(scac, linktable, by='Ticker') %>% filter(is.na(gvkey) | (LINKDT <= filingdate_formatted
                                                                                     & (is.na(LINKENDDT) | filingdate_formatted <= LINKENDDT)))
scac_with_gvkey$bad_date = FALSE

bad_date = anti_join(scac, scac_with_gvkey, by=c('Filing.Name', 'Filing.Date', 'District.Court', 'Exchange', 'Ticker'))
bad_date$bad_date = TRUE
linktable_gvkeyunique = linktable %>% select(-LINKDT, -LINKENDDT) %>% unique()
baddate_with_gvkey = bad_date %>% select(-filingdate_formatted) %>% left_join(linktable_gvkeyunique, by='Ticker')

joined_scac = scac_with_gvkey %>% select(-LINKDT, -LINKENDDT, -filingdate_formatted) %>% bind_rows(baddate_with_gvkey)

write.csv(joined_scac %>% unique(), '~/Downloads/scac_gvkey.csv', row.names = FALSE)
