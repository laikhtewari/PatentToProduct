suppressPackageStartupMessages(library(dplyr))
suppressPackageStartupMessages(library(tidyr))
suppressPackageStartupMessages(library(stringr))
suppressPackageStartupMessages(library(tm))

# BASE VARS
# base_url = '/Users/laikhtewari/Downloads/'
base_url = '/home/users/ltewari/chembl/'
included_versions = c(20, 21, 22, 23, 24, 25, 26)
n = length(included_versions)
#version_string = paste(included_versions, collapse="+")
version_string = paste(min(included_versions), max(included_versions), sep='_')

gen_full_prodpat = function() {
  # READ MOST RECENT PRODUCTS DF
  print('Loading products...')
  products = read.csv(paste(base_url, 'chembl_', max(included_versions), '/products_', max(included_versions), '.csv', sep=''))
  print('Done loading products.')

  # CONSTRUCT MASTER PRODPAT DF
  # Work backwards and add in dropped rows
  print('Building prodpat...')
  id_cols = c('product_id', 'patent_no', 'patent_use_code')
  for (i in 1:n) {
    curr_version = included_versions[n - i + 1]
    new_df = read.csv(paste(base_url, 'chembl_', curr_version, '/prodpat_', curr_version, '.csv', sep=''))[,-1] # -1 gets rid of useless prodpat id col
    new_df$version = curr_version
    if (i == 1) {
      prodpat_master = new_df
      print(paste('Version', curr_version, 'adds', nrow(new_df), 'rows'))
      next
    }
    dropped = anti_join(new_df, prodpat_master, by=id_cols)
    print(paste('Version', curr_version, 'adds', nrow(dropped), 'rows'))
    prodpat_master = bind_rows(prodpat_master, dropped)
  }
  print('Done building prodpat.')

  # MAKE MASTER DF
  print('Building Prod+Pat Master...')
  no_patents = anti_join(products, prodpat_master, by='product_id')
  no_patents$version = max(included_versions)
  patents = inner_join(products, prodpat_master, by='product_id')
  df = bind_rows(patents, no_patents)
  print('Done building Prod+Pat Master.')

  # PROCESS MASTER DF
  print('Adding computed columns...')
  colnames(df)[colnames(df) == 'patent_no'] = 'raw_patent_no'
  df = separate(df, raw_patent_no, into=c('patent_no', 'PED'), sep="\\*", remove = FALSE, convert = FALSE, fill='right')
  df$PED = ifelse(is.na(df$PED), 0, 1)
  # df$submission_to_approval = ifelse(df$submission_date != '', as.Date(df$approval_date) - as.Date(df$submission_date), NA)
  print('Done adding computed columns.')

  return(df)
}

strp = function(s) {
  if (is.na(s)) {
    return(NA)
  }
  s = gsub("[[:punct:]]", "", s)
  s = tolower(s)
  stop_words = c('ab',
    'america',
    'and',
    'bioscience',
    'care',
    'central',
    'chemicals',
    'co',
    'company',
    'consumer',
    'corp',
    'corporation',
    'delivery systems',
    'design',
    'deutschland',
    'development',
    'div',
    'division',
    'drug',
    'endocrine',
    'gainesville',
    'global',
    'gmbh',
    'group',
    'health care',
    'healthcare',
    'holdings',
    'inc',
    'infection',
    'innovative',
    'institute',
    'international',
    'ireland',
    'kg',
    'laboratories',
    'labs',
    'limited',
    'llc',
    'lp',
    'ltd',
    'manufacturing',
    'medical',
    'north',
    'of',
    'operations',
    'pharma',
    'pharmaceutical',
    'pharmaceuticals',
    'plc',
    'prevention',
    'production',
    'products',
    'properties',
    'research',
    'sa',
    'sales',
    'spa',
    'sub',
    'technologies',
    'therapeutics',
    'uk',
    'unlimited',
    'unltd',
    'us',
    'usa')
  s = removeWords(s, stop_words)
  s = gsub('\\s+', ' ', trimws(s))
  return(s)
}

# min_lev_dist = function(row) {
#   if (is.na(row[28])) {
#     return(NA)
#   } 
#   else {
#     candidates = c(row[27], row[28], row[29], row[30])
#     dist = rep(NA, 4)
#     for (i in 1:4) {
#       dist[i] = adist(strp(row[12]), strp(candidates[i]), ignore.case=TRUE)[1]
#     }
#     return(min(dist, na.rm=TRUE))
#   }
# }

# to_primary_company = function(row) {
#   if (is.na(row[28])) {
#     return(row[27])
#   } 
#   else {
#     candidates = c(row[27], row[28], row[29], row[30])
#     dist = rep(NA, 4)
#     for (i in 1:4) {
#       dist[i] = adist(strp(row[12]), strp(candidates[i]), ignore.case=TRUE)[1]
#     }
#     return(candidates[which.min(dist)])
#   }
# }

lev_dist = function(s1, s2) {
  return(adist(s1, s2, ignore.case=TRUE))
}

#requires org to be present
select_primary = function(clean_dist, raw_dist, freq, org, isBadMatch=FALSE) {
  if (min(clean_dist, na.rm=TRUE) < 6) {
    if (isBadMatch) {
      return(FALSE)
    } else{
      return(org[which.min(clean_dist)])
    }
  } else {
    if (isBadMatch) {
      return(TRUE)
    } else {
      return(org[which.min(freq)])
    }
  }
}

join_assignees = function(df=NA, backup=FALSE) {
  if (backup) {
    print('Reading checkpoint (primary company)...')
    return(read.csv('/home/users/ltewari/afs-home/Desktop/patentproduct/prodpat_primco.csv', stringsAsFactors=FALSE))
    print('Done reading checkpoint.')
  } 
  else {
    # # JOIN ASSIGNEES
    # print('Reading in assignees (this could take a while)...')
    # # Copy assignees file into the following path on yen: /home/users/laikhtewari/chembl/assignee_uspto.csv
    # assignees = read.csv(paste('/home/users/ltewari/data/', 'assignee_uspto.csv', sep=''))
    # print('Done reading in assignees')
    # print('Joining assignees into prodpat...')
    # prodpat_assignee_naive = left_join(df, assignees, by=c("patent_no" = "patent_id"))
    # print('Saving naive checkpoint...')
    # write.csv(prodpat_assignee_naive, '/home/users/ltewari/afs-home/Desktop/patentproduct/prodpat_assignee_naive.csv', row.names = FALSE)
    # print('Done saving naive checkpoint.')
    # print('Done joining into prodpat.')

    print('Reading checkpoint (naive join)...')
    prodpat_assignee_naive = read.csv('/home/users/ltewari/afs-home/Desktop/patentproduct/prodpat_assignee_naive.csv', stringsAsFactors=FALSE)
    print('Done reading checkpoint (naive join).')

    # # COLLAPSE DUPS (PUC, Asignees, PED, etc)
    # print('Collapsing dups (this could take a while)...')
    # prodpat_full = summarize(group_by(prodpat_assignee_naive, patent_no, product_id), 
    #   dosage_form = first(dosage_form), route = first(route), trade_name = first(trade_name), 
    #   approval_date = first(approval_date), ad_type = first(ad_type), oral = first(oral), 
    #   topical = first(topical), parenteral = first(parenteral), black_box_warning = first(black_box_warning), 
    #   applicant_full_name = first(applicant_full_name), innovator_company = first(innovator_company), 
    #   nda_type = first(nda_type), patent_expire_date = first(patent_expire_date), drug_substance_flag = first(drug_substance_flag), 
    #   drug_product_flag = first(drug_product_flag), submission_date = first(submission_date), 
    #   patent_use_codes_list = paste(unique(patent_use_code), collapse=';'), PED = max(PED), delist_flag = first(delist_flag), 
    #   latest_version = max(version), type = first(type), name_first = paste(unique(name_first), collapse=';'), name_last = paste(unique(name_last), collapse=';'),
    #   assignees_list = paste(unique(assignee_id), collapse=';'), organization_list = paste(unique(organization), collapse=';'))
    # print('Done collapsing dups.')
    # print('Saving collapsed checkpoint...')
    # write.csv(prodpat_full, '/home/users/ltewari/afs-home/Desktop/patentproduct/prodpat_full.csv', row.names = FALSE)
    # print('Done saving collapsed checkpoint.')

    print('Reading checkpoint (pp full)...')
    prodpat_full = read.csv('/home/users/ltewari/afs-home/Desktop/patentproduct/prodpat_full.csv', stringsAsFactors=FALSE)
    print('Done reading checkpoint (pp full).')

    print('Identifying applicant to primary company...')
    g = group_by(prodpat_assignee_naive, applicant_full_name, organization)
    app_org = summarize(g, assignee_id=first(assignee_id), freq=n())
    app_org$raw_dist = mapply(lev_dist, app_org$applicant_full_name, app_org$organization)
    app_org$clean_applicant = sapply(app_org$applicant_full_name, strp)
    app_org$clean_org = sapply(app_org$organization, strp)
    app_org$clean_dist = mapply(lev_dist, app_org$clean_applicant, app_org$clean_org)
    applicant_to_primary = summarize(group_by(app_org[!is.na(app_org$organization),], clean_applicant),
                        primary_company = select_primary(clean_dist, raw_dist, freq, organization), 
                        primary_org_id = select_primary(clean_dist, raw_dist, freq, assignee_id), 
                        badmatch = select_primary(clean_dist, raw_dist, freq, organization, isBadMatch = TRUE))
    print('Done identifying applicant to primary company.')

    print('Selecting primary company...')
    sep_df = separate(data=prodpat_full, col=organization_list, into=c('org1','org2','org3','org4'), sep=';', remove=TRUE, convert=FALSE, extra='warn', fill='right')
    sep_df = separate(data=sep_df, col=assignees_list, into=c('org_id1','org_id2','org_id3','org_id4'), sep=';', remove=TRUE, convert=FALSE, extra='warn', fill='right')
    # sep_df$primary_company = apply(sep_df, 1, to_primary_company)
    # sep_df$assignee_distance = apply(sep_df, 1, min_lev_dist)
    sep_df$clean_applicant = sapply(sep_df$applicant_full_name, strp)
    ret_df = left_join(sep_df, applicant_to_primary, by = "clean_applicant")
    print('Done selecting primary company.')

    print('Saving primary company checkpoint...')
    write.csv(ret_df, '/home/users/ltewari/afs-home/Desktop/patentproduct/prodpat_primco.csv', row.names = FALSE)
    print('Done saving primary company checkpoint.')
    return(ret_df)
  }

}

join_parent_company = function(df) {
  print('Loading LexisNexis (this could take a while)...')
  pub = read.csv('/home/users/ltewari/data/DCAPUB.csv')[,c(5,10)]
  priv = read.csv('/home/users/ltewari/data/DCAPRIV.csv')[,c(5,10)]
  int = read.csv('/home/users/ltewari/data/DCAINT.csv')[,c(5,10)]
  null_c = read.csv('/home/users/ltewari/data/DCANULL.csv')[,c(5,10)]
  ln = bind_rows(pub, priv, int, null_c)
  rm(pub, priv, int, null_c)
  print('Done loading LexisNexis.')
  print('Joining in parent companies...')
  df = left_join(df, ln, by=c('primary_company' = 'COMPANY_NAME'))
  print('Done joining parent companies.')
  return(df)
}

write_data = function(df) {
  # WRITING DATA 
  print('Writing out data...')
  # write_path = paste(base_url, 'prodpat_', version_string, '_', format(Sys.Date(), '%Y%m%d'), '.csv', sep='')
  write_path = paste('/home/users/ltewari/afs-home/Desktop/patentproduct/', 'prodpat_', version_string, '_', format(Sys.Date(), '%Y%m%d'), '.csv', sep='')
  write.csv(df, write_path, row.names = FALSE)
  print('Done writing out data. Goodbye :)')
}

main = function() {
  # prodpat = gen_full_prodpat()
  prodpat_withassignees = join_assignees(prodpat)
  df_parent = join_parent_company(prodpat_withassignees)
  final_df = df_parent
  write_data(final_df)
}

main()


