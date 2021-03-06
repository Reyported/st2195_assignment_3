library(RSQLite)
library(DBI)
library(plyr)
library(dplyr)
library(dbplyr)

        
#connect to the database
airline2 <- dbConnect(RSQLite::SQLite(), "airline2.db")

#define datasets
airportcsv <- read.csv(file='airports.csv')
carrierscsv <- read.csv(file='carriers.csv')
planescsv <- read.csv(file='plane-data.csv')

ontime1 <- read.csv(file='2000.csv')
ontime2 <- read.csv(file='2001.csv')
ontime3 <- read.csv(file='2002.csv')
ontime4 <- read.csv(file='2003.csv')
ontime5 <- read.csv(file='2004.csv')
ontime6 <- read.csv(file='2005.csv')

#create ontime table
dbWriteTable(airline2, "ontime", ontime1)
dbWriteTable(airline2, "ontime", ontime2, append=TRUE)
dbWriteTable(airline2, "ontime", ontime3, append=TRUE)
dbWriteTable(airline2, "ontime", ontime4, append=TRUE)
dbWriteTable(airline2, "ontime", ontime5, append=TRUE)
dbWriteTable(airline2, "ontime", ontime6, append=TRUE) 

#create other tables
dbWriteTable(airline2, "airports", airportcsv)
dbWriteTable(airline2, "carriers", carrierscsv)
dbWriteTable(airline2, "planes", planescsv)

##Queries in DBI

dbListFields(airline2, "planes")
#Q1
Crit1 <- "SELECT planes.model, SUM(1) As numplanes, AVG(DepDelay) As AverDelay, planes.manufacturer 
FROM ontime LEFT JOIN planes ON ontime.tailnum=planes.tailnum
WHERE (DepDelay>0 AND Diverted = 0)
GROUP BY planes.model
ORDER BY AverDelay ASC
LIMIT 0,1;"
res1 <- dbGetQuery(airline2, Crit1)
write.csv(res1, "resultsr1.csv")
dbClearResult(res1)

#Q2
Crit2 <- "SELECT ontime.Dest, SUM(1) As inflights
FROM ontime LEFT JOIN airports ON ontime.Dest=airports.iata
WHERE cancelled=0
GROUP BY ontime.Dest
ORDER BY inflights DESC
LIMIT 0,1;"
res2 <- dbGetQuery(airline2, Crit2)
write.csv(res2, "resultsr2.csv")
dbClearResult(res2)

#Q3
Crit3 <- "SELECT ontime.UniqueCarrier, SUM(1) As numflights, carriers.Description
FROM ontime LEFT JOIN carriers ON ontime.UniqueCarrier=carriers.Code
WHERE cancelled>0
GROUP BY ontime.UniqueCarrier
ORDER BY numflights DESC
LIMIT 0,1;"
res3 <- dbGetQuery(airline2, Crit3)
write.csv(res3, "resultsr3.csv")
dbClearResult(res3)
#Q4

Crit4 <- "WITH t1 AS (SELECT ontime.UniqueCarrier, SUM(1) As numflights, carriers.Description
FROM ontime LEFT JOIN carriers ON ontime.UniqueCarrier=carriers.Code
WHERE cancelled>0
GROUP BY ontime.UniqueCarrier
ORDER BY numflights DESC),
t2 AS (SELECT ontime.UniqueCarrier, SUM(1) As numflights, carriers.Description
FROM ontime LEFT JOIN carriers ON ontime.UniqueCarrier=carriers.Code
GROUP BY ontime.UniqueCarrier
ORDER BY numflights DESC)

SELECT t2.UniqueCarrier, (CAST(t1.numflights AS REAL)/CAST(t2.numflights AS REAL)) AS Ratio 
FROM t2 
JOIN t1 ON t1.UniqueCarrier=t2.UniqueCarrier
ORDER BY Ratio DESC"
res4 <- dbGetQuery(airline2, Crit4)
write.csv(res4, "resultsr4.csv")

#OR
Crit4b <- "SELECT carriers.Description as Carrier, avg(ontime.Cancelled)*100 as Cancelled_numer 
FROM ontime JOIN carriers ON ontime.UniqueCarrier=carriers.Code
GROUP BY carrier
ORDER BY Cancelled_numer DESC"
res4b <- dbGetQuery(airline2, Crit4b)
write.csv(res4b, "resultsr4b.csv")



#Queries in dplyr

ontime <- tbl(airline2, "ontime")
airports <- tbl(airline2, "airports")
carriers <- tbl(airline2, "carriers")
planes <- tbl(airline2,"planes")

colnames(ontime)
#give prefixes to prevent ambiguity
planes2 <- rename(planes, plyear= year, pltailnum=tailnum)

#Q1
 
q1b <- ontime %>%   inner_join(planes2, by=c('TailNum'='pltailnum'), copy=TRUE) %>%
  filter(DepDelay>'0' & Diverted=='0') %>%
  group_by(model) %>%
  summarize(avgDepDelay=mean(DepDelay, na.rm=TRUE))%>%
  arrange(avgDepDelay) %>%
  
  show_query()
  

write.csv(q1b, "resultsr1b.csv")  


#Q2

q2b <- ontime %>%
  left_join(airports, by=c('Dest'='iata'))%>%
  filter(Cancelled=='0')%>%
  group_by(Dest)%>%
  count(Dest) %>%
  arrange(n)%>%
  show_query()
  
  
  write.csv(q2b, "resultsr2b.csv")
  
#Q3

q3b <- ontime %>%
  left_join(carriers, by=c('UniqueCarrier'='Code'))%>%
  group_by(Description)%>%
  summarise(CANCflights= sum(Cancelled, na.rm=TRUE)) %>%
  arrange(desc(CANCflights))%>%
  show_query()

write.csv(q3b, "resultsr3b.csv")

#Q4

q4b <- ontime %>%
  inner_join(carriers, by=c('UniqueCarrier'='Code')) %>%
  group_by(Description)%>%
  mutate(Cancflights = sum(Cancelled, na.rm=TRUE))%>%
  summarise(ratio=Cancflights/n())%>%
  show_query()

write.csv(q4b, "resultsr4b.csv")

  
#disconnect from the database
dbDisconnect(airline2)