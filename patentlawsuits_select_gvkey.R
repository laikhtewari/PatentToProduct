library(dplyr)
library(tidyr)
library(tm)

cleanse_company_name = function(s, remove_stop_words = TRUE) {
  if (is.na(s)) {
    return(NA)
  }
  s = gsub("[[:punct:]]", "", s) # get rid of punctuation
  s = tolower(s) # convert to lowercase
  # Stop words taken as first ~30 most common words appearing in company names in patent lawsuits DS
  stop_words = c("inc",
                 "llc",
                 "corporation",
                 "ltd",
                 "company",
                 "usa",
                 "america",
                 "co",
                 "international",
                 "electronics",
                 "systems",
                 "pharmaceuticals",
                 "technologies",
                 "corp",
                 "group",
                 "products",
                 "of",
                 "limited",
                 "does",
                 "laboratories",
                 "communications",
                 "industries",
                 "incorporated",
                 "the",
                 "technology",
                 "services",
                 "holdings",
                 "north",
                 "and",
                 "solutions",
                 "manufacturing")
  if (remove_stop_words) {
    s = removeWords(s, stop_words) # remove stop words
  }
  s = gsub('\\s+', ' ', trimws(s)) # remove excess whitespace
  if (s == '') {
    return(NA)
  }
  return(s)
}

patentslawsuits = read.csv('~/Downloads/070220_Patent_Lawsuits.csv', stringsAsFactors=FALSE)
patentslawsuits$clean_name = sapply(patentslawsuits$individual_defendant, cleanse_company_name)
patentslawsuits$filingdate_formatted = as.Date(patentslawsuits$Filing_Date, format='%m/%d/%y')

conm_to_gvkey = read.csv('~/Downloads/conm_to_gvkey.csv', stringsAsFactors = FALSE)
conm_to_gvkey$LINKDT_formatted = as.Date(as.character(conm_to_gvkey$LINKDT), format='%Y%m%d')
conm_to_gvkey$LINKENDDT_formatted = as.Date(as.character(conm_to_gvkey$LINKENDDT), format='%Y%m%d')
conm_to_gvkey$clean_name = sapply(conm_to_gvkey$conm, cleanse_company_name)

linktable = conm_to_gvkey[!is.na(conm_to_gvkey$clean_name),] %>% select(clean_name, gvkey, LINKDT = LINKDT_formatted, LINKENDDT = LINKENDDT_formatted) %>% unique()

patentlawsuits_with_gvkey = left_join(patentslawsuits, linktable, by='clean_name') %>% filter(is.na(gvkey) | (LINKDT <= filingdate_formatted
                                                                                     & (is.na(LINKENDDT) | filingdate_formatted <= LINKENDDT)))
patentlawsuits_with_gvkey$bad_date = FALSE

patentlawsuits_bad_date = anti_join(patentslawsuits, patentlawsuits_with_gvkey, by=colnames(patentslawsuits))
patentlawsuits_bad_date$bad_date = TRUE
linktable_gvkeyunique = linktable %>% select(-LINKDT, -LINKENDDT) %>% unique()
baddate_with_gvkey = patentlawsuits_bad_date %>% select(-filingdate_formatted) %>% left_join(linktable_gvkeyunique, by='clean_name')

joined_patentlawsuits = patentlawsuits_with_gvkey %>% select(-LINKDT, -LINKENDDT, -filingdate_formatted, -clean_name) %>% 
  bind_rows(baddate_with_gvkey %>% select(-clean_name))

write.csv(joined_patentlawsuits %>% unique(), '~/Downloads/patent_lawsuits_gvkey.csv', row.names = FALSE)







