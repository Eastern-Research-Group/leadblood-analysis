#Before this script can be run, must ensure that 32-bit version of R is enabled. This is necessary to properly connect to the Access database and use the ODBC.

#Install and load relevant packages.
install.packages("RODBC")
library(RODBC)
install.packages("readr")
library(readr)
install.packages("dplyr")
library(dplyr)

#Create dataframe in R with lead blood level data from ICF.
#IMPORTANT NOTE: Make sure the data are still saved at the filepath below. If data need to be downloaded again, Jill's email is saved at P:\Steam Supplemental\10 EA\04 Supplemental Analyses\ATSDR\R script\Email from Jill_02222022.msg.
leadblood <- read_csv("P:/Steam Supplemental/10 EA/04 Supplemental Analyses/ATSDR/Lead-blood data from ICF/IEUBK_CBG_OptionA.csv")

#Create dataframe in R with unique Census blocks to pull corresponding lead-blood data.
CBlist <- read_csv("P:/Steam Supplemental/10 EA/04 Supplemental Analyses/Census Block GIS/unique-census-blocks-for-r.csv")

#Checking data types for each column.
str(leadblood)

#Outside of R, set up Data Source Name (DSN) to connect to the blank Access database through Open Database Connectivity (ODBC). This creates a connection to the filepath and allows R to export data to a table there. Used "ODBC Data Source Administrator (32-bit)" and created a User DSN named "TestLeadBlood".

#Connect to the blank Access database using the Access 2007 version of the ODBC connection command.
db_conn <- odbcConnectAccess2007("P:/Steam Supplemental/10 EA/04 Supplemental Analyses/ATSDR/Lead-blood-levels-from-ICF_02222022.accdb")


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


#Close the ODBC connection.
odbcClose(db_conn)
