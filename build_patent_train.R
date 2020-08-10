suppressMessages(library(dplyr))

base_path = "/home/users/ltewari/new_patent_scrape/"

print("Reading in patents data...")
patents = read.csv(paste(base_path, 'patents_raw_from_backup_0720.csv', sep=''), stringsAsFactors=FALSE)

patent_to_num_figures = read.csv(paste(base_path, 'data/patent_num_to_num_figures.csv', sep=''), stringsAsFactors=FALSE)
patents = patents %>% left_join(patent_to_num_figures, by=c('patent_number' = 'patent_id'))

patents[is.na(patents)] = 0
print("Done reading patents data")



print("Dealing with prodpat data...")
products = read.csv(paste(base_path, 'data/prodpat.csv', sep=""), stringsAsFactors=FALSE)
patents$master_contains_patent = patents$patent_number %in% products$patent_no
print("Done with prodpat.")
rm(products)



print("Reading in firm id info")
firmids = read.csv(paste(base_path, 'data/assignee_to_firmid.csv', sep=''), stringsAsFactors=FALSE)
permco_to_gvkey = read.csv(paste(base_path, 'data/wrds_permco_gvkey.csv', sep=''), stringsAsFactors=FALSE) %>% select(selected_permco = LPERMCO, selected_gvkey = GVKEY)

firmids$app_date_0 = as.Date(firmids$app_date_0)
patents$app_date = as.Date(patents$app_date)
with_firmid = patents %>% left_join(firmids, by=c('app_date'='app_date_0', 'assignee'))
with_firmid = with_firmid %>% left_join(permco_to_gvkey, by=c('selected_permco'))
print("Done adding firm ids")



print("Bringing in firm characteristics")
firm_characs = read.csv(paste(base_path, 'data/firm_characteristics_0802.csv', sep=''), stringsAsFactors=FALSE)
firm_characs$cusip_full = firm_characs$cusip
firm_characs$cusip = sapply(firm_characs$cusip_full, substr, 1, 8)
firm_characs$datayear = as.integer(format(as.Date(firm_characs$datadate,format='%m/%d/%Y'), "%Y"))
with_firmid$datayear = as.integer(format(as.Date(with_firmid$app_date), "%Y"))

with_firmchar = with_firmid %>% left_join(firm_characs, by=c('selected_gvkey' = 'gvkey', 'datayear'))
print("Done bringing in firm characteristics")



# print("Bringing in kogan...")
# kogan = read.csv(paste(base_path, 'data/kogan_unique.csv', sep=""), stringsAsFactors=FALSE)
# df = left_join(patents, kogan, by=c('patent_number' = 'patnum'))
# rm(kogan)
# print("Done with kogan")

# print("Permco stuff")
# permco_permno = read.csv(paste(base_path, 'data/cusip_permno_permco.csv', sep=""), stringsAsFactors=FALSE)
# df = left_join(df, permco_permno, by=c('permco_1' = 'permco'))
# rm(permco_permno)
# print("Done with permco")



print("Writing out...")
out_df = with_firmchar
write.csv(out_df, paste(base_path, 'patents_augmented_nona_0802.csv', sep=""), row.names=FALSE) # change back to df when adding in kogan/permco
print("Goodbye :)")