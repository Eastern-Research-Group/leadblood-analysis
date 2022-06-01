#Before this script can be run, must ensure that 32-bit version of R is enabled. This is necessary to properly connect to the Access database and use the ODBC.

#Install and load relevant packages.
install.packages("RODBC")
library(RODBC)
install.packages("readr")
library(readr)
install.packages("dplyr")
library(dplyr)

#############General analysis as part of initial exploration.

#Create dataframe in R with lead blood level data from ICF.
#IMPORTANT NOTE: Make sure the data are still saved at the filepath below. If data need to be downloaded again, Jill's email is saved at P:\Steam Supplemental\10 EA\04 Supplemental Analyses\ATSDR\R script\Email from Jill_02222022.msg.
leadblood <- read_csv("P:/Steam Supplemental/10 EA/04 Supplemental Analyses/ATSDR/Lead-blood data from ICF/IEUBK_CBG_OptionA.csv")

#Create dataframe in R with unique Census blocks to pull corresponding lead-blood data.
#**Run this for for 2022 Proposal too!
CBlist <- read_csv("P:/Steam Supplemental/10 EA/04 Supplemental Analyses/Census Block GIS/unique-census-blocks-for-r.csv")

#Checking data types for each column.
str(leadblood)

#Outside of R, set up Data Source Name (DSN) to connect to the blank Access database through Open Database Connectivity (ODBC). This creates a connection to the filepath and allows R to export data to a table there. Used "ODBC Data Source Administrator (32-bit)" and created a User DSN named "TestLeadBlood".

#Connect to the blank Access database using the Access 2007 version of the ODBC connection command.
db_conn <- odbcConnectAccess2007("P:/Steam Supplemental/10 EA/04 Supplemental Analyses/ATSDR/Lead-blood-levels-from-ICF_02222022.accdb")


#######2022 Proposal data analysis update (see bottom for associated code)
#Run code above to create CBlist dataframe!
#Needed new database for the Proposal analysis, because the folder structure had changed. Outside of R, set up DSN to connect to new blank Access database through ODBC. Created a User DSN named "PropLeadBlood".

#Connect to the blank Access database using the Access 2007 version of the ODBC connection command.
db_conn2 <- odbcConnectAccess2007("P:/Steam Supplemental/10 EA/04 Supplemental Analyses/Joint Toxic Analysis/Lead-blood-levels-from-ICF_05262022.accdb")

######End 2022 Proposal analysis update (see below for more)


##Testing process of subsetting data by Census Block and exporting to Access.
#Filter main table to create table 'CBtest'
CBtest <- leadblood %>%
  filter(CB == "210859505002")

#Export to Access as table called "CBtest." Confirmed exported successfully.
sqlSave(db_conn, CBtest, rownames = FALSE, colnames = FALSE, safer = FALSE, addPK = FALSE, fast = FALSE)

#Noticed that some of the values in the Population column are in scientific notation. Not sure if that is a problem. Tried testing issue below
#Create filtered table without scientific notation, as intended.
CBtest2 <- leadblood %>%
  filter(CB == "482012324031")

#Export "CBtest2" table to the Access database.
sqlSave(db_conn, CBtest2, rownames = FALSE, colnames = FALSE, safer = FALSE, addPK = FALSE, fast = FALSE)

#Use sqlSave function to export the lead.blood dataframe to the Access database. 
#Note to self: May need to delete or troubleshoot "varTypes."
sqlSave(db_conn,leadblood, rownames = FALSE, colnames = FALSE, safer = FALSE, addPK = FALSE, varTypes = columnTypes, fast = FALSE)


####Separate analysis for Kristi, run on 3/3/2022.

#Calculate number of unique Census blocks in ICF's data.
length(unique(leadblood$CB))

#Create data frame with unique Census blocks.
CensusBlocks <- unique(leadblood[c("CB")])

#Export Census blocks data frame to Access database.
sqlSave(db_conn,CensusBlocks, rownames = FALSE, colnames = FALSE, safer = FALSE, addPK = FALSE, fast = FALSE)

######Extract lead-blood data for unique Census blocks. Run on 3/24/22.

#Create subset of leadblood dataframe with only Census blocks of interest (the 222 unique blocks).
#Filter the leadblood data to only include rows data for the 222 unique Census blocks. 
leadblood_filtered <- semi_join(x= leadblood, y = CBlist, by = c("CB" = "FIPS"))

#Confirm that the number of unique Census blocks in the extracted column is 222 as expected.
length(unique(leadblood_filtered$CB))

#Export the extracted data to the database.
sqlSave(db_conn,leadblood_filtered, rownames = FALSE, colnames = FALSE, safer = FALSE, addPK = FALSE, fast = FALSE)


############Manipulating lead-blood data for 2022 Proposal. Run late May 2022.

#Create dataframe with lead blood level data from ICF for 2022 Proposal.
#Naming each dataframe after the regulatory option, starting with Option 1.
Option1 <- read_csv("P:/Steam Supplemental/10 EA/04 Supplemental Analyses/from ICF/2022 Proposal_PbB_05.18.22/IEUBK_CBG_Option1.csv")

#Create subset with only 222 unique Census blocks of interest.
#Filter the Option 1 data to only include rows data for the 222 unique Census blocks. 
Option1_filtered <- semi_join(x= Option1, y = CBlist, by = c("CB" = "FIPS"))

#Checking number of unique Census blocks in the filtered dataset. Total is 205 for Option 1.
length(unique(Option1_filtered$CB))

#Export the extracted data to the database.
sqlSave(db_conn2,Option1_filtered, rownames = FALSE, colnames = FALSE, safer = FALSE, addPK = FALSE, fast = FALSE)

#Export the list of Census blocks to the database.
sqlSave(db_conn2,CBlist, rownames = FALSE, colnames = FALSE, safer = FALSE, addPK = FALSE, fast = FALSE)

#Repeating the same steps for Option 2.
Option2 <- read_csv("P:/Steam Supplemental/10 EA/04 Supplemental Analyses/from ICF/2022 Proposal_PbB_05.18.22/IEUBK_CBG_Option2.csv")
Option2_filtered <- semi_join(x= Option2, y = CBlist, by = c("CB" = "FIPS"))
length(unique(Option2_filtered$CB))
sqlSave(db_conn2,Option2_filtered, rownames = FALSE, colnames = FALSE, safer = FALSE, addPK = FALSE, fast = FALSE)

#Repeating the same steps for Option 3.
Option3 <- read_csv("P:/Steam Supplemental/10 EA/04 Supplemental Analyses/from ICF/2022 Proposal_PbB_05.18.22/IEUBK_CBG_Option3.csv")
Option3_filtered <- semi_join(x= Option3, y = CBlist, by = c("CB" = "FIPS"))
length(unique(Option3_filtered$CB))
sqlSave(db_conn2,Option3_filtered, rownames = FALSE, colnames = FALSE, safer = FALSE, addPK = FALSE, fast = FALSE)

#Repeating the same steps for Option 4.
Option4 <- read_csv("P:/Steam Supplemental/10 EA/04 Supplemental Analyses/from ICF/2022 Proposal_PbB_05.18.22/IEUBK_CBG_Option4.csv")
Option4_filtered <- semi_join(x= Option4, y = CBlist, by = c("CB" = "FIPS"))
length(unique(Option4_filtered$CB))
sqlSave(db_conn2,Option4_filtered, rownames = FALSE, colnames = FALSE, safer = FALSE, addPK = FALSE, fast = FALSE)

#Creating list of unique Census blocks for Option 1.
Opt1_CBs <- distinct(Option1_filtered, CB)

#Exporting the list to the Access database.
sqlSave(db_conn2,Opt1_CBs, rownames = FALSE, colnames = FALSE, safer = FALSE, addPK = FALSE, fast = FALSE)

#Creating and exporting the list of Census blocks for Options 2, 3, and 4.
Opt2_CBs <- distinct(Option2_filtered, CB)
sqlSave(db_conn2,Opt2_CBs, rownames = FALSE, colnames = FALSE, safer = FALSE, addPK = FALSE, fast = FALSE)

Opt3_CBs <- distinct(Option3_filtered, CB)
sqlSave(db_conn2,Opt3_CBs, rownames = FALSE, colnames = FALSE, safer = FALSE, addPK = FALSE, fast = FALSE)

Opt4_CBs <- distinct(Option4_filtered, CB)
sqlSave(db_conn2,Opt4_CBs, rownames = FALSE, colnames = FALSE, safer = FALSE, addPK = FALSE, fast = FALSE)

#Checking whether the Census blocks are the same for options. Indicates that Opt 1 = Opt 2 = Opt 3 = Opt 4, so the unique Census blocks are the same for all options. Note that this function ignores the fact that the Census blocks aren't listed in the same order (i.e., the rows are not the same).
all_equal(Opt1_CBs, Opt2_CBs, ignore_row_order = TRUE)
all_equal(Opt2_CBs, Opt3_CBs, ignore_row_order = TRUE)
all_equal(Opt3_CBs, Opt4_CBs, ignore_row_order = TRUE)



#Close the ODBC connections. Close db_conn2 for the 2022 Proposal analysis. db_conn is from the initial exploration.
odbcClose(db_conn)
odbcClose(db_conn2)
