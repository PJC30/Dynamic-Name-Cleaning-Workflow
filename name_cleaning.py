import pandas as pd
import pymssql
import unicodedata
import re
from datetime import datetime


df = pd.read_csv('input.csv', low_memory=False)
df = pd.DataFrame(df, columns = ['id', 'first_name', 'last_name', 'organization', 'linkedin_url', 'Remarks'])

df1 = pd.read_csv('input.csv', low_memory=False)
df1 = pd.DataFrame(df1, columns=['first_name', 'last_name'])

df1.rename(columns={'first_name':'cleaned_first_name', 'last_name':'cleaned_last_name'}, inplace=True)

df1 = df1.astype(str).apply(lambda x: x.str.normalize('NFKD').str.encode('ascii', 'ignore').str.decode('utf-8'))
df1 = df1.astype(str).replace(r'[^A-Za-z-()"[] ]', '', regex=True)
df1 = df1.astype(str).replace(r'[.]', '', regex=True)
df1 = df1.astype(str).apply(lambda x: x.str.lstrip())
df1 = df1.astype(str).apply(lambda x: x.str.rstrip())
df1 = df1.astype(str).replace(r'\".*?\"', '', regex=True)

# Keywords to be removed from beginning of the string

regex_pat1 = re.compile(r'^Captain|^Minister', flags=re.IGNORECASE)

df1 = df1.astype(str).apply(lambda x: x.str.replace(regex_pat1, '', regex=True))


# Keywords to be removed from anywhere of the string

regex_pat2 = re.compile(r' Admin| Architects?| AspNccer| Associates?| CdfaTM| Cdfom| Cdpse| Ciampi| Cic| Cicp| Cid| Cimic| CippUs| CippUsE| Clssgb| Cmtse| CossAsp| CpcCts| Crcst| Crisci| Crisp| Cssbb| Cssgb| Cts-D| Cts-D| Cts-I| Cvpcc| Devlp| Fnp-C| Mba-Cpa| Pa-C| Pmi-Acp| Psw-C| Shrm-Cp| Vp-Cissp| -Mba| MbaHrm| Mba-Hr| Aic-M| Mba-Hc| Mbaha| MbaMha| Mba-Cpa| MdMba| Mbec| Mboe| Mdiv| Mcse| Mba-Hm| Mcj| Mbt| Mabc| Pmp| Phd| Psm| Psp| Popm| Pls| Pcip| Pfmp| Phr| Prc| Pcs| Psm-| Psc| Pcc| Pmc| Pcam| Pbc| Picq| Pga| Pgmp| Pws| Phn| Iii| Ricp| Rph| Fsgm| Rbsm| Rost| Rex| Rhit| Itil| Gisp| Rpa| Rdn| Qssp| Rcdd| Isr| Fma| Ohst| Rehs| Iws| Inc| Jnr| Gsp| Rvt| Rhia| Edfp| Recla| Otr-L| Erp| Fsa| Esq| Facp| Fscp| Olp| Fmp| Iqc| Emt-P| ItilV| Rrp| Rix| Osp| Rde| Fca| Rrt| Frs| OtrL| Icm| Rwcs| RtM| Rohr| Enz| Fpc| Rse| Rohm| Itpm| Frm| MaHrd| Fcsi| Mn-E| Erb| Qpa| Gphr| Euw| Ess| Imh-E| Qpfc| Fnp| Rdcs| Gms| Rfc| Oms| Gsec| Rn-Bc| Regt| Fmo| Fhm| Grp| Grcp| Rvs| Rdh| Rohn| Rem| Ilg| Jmac| Fpa| Chhr| Shrm-Spc| Lcadc| Rn-Bsn| Bcba| Msn Rn - Bc| - Cissp| Ms-Mgmt| Cxa| Lcsw| Jd/Mba| Csbb| Hrm| Cnor| Cprw| Pharm| Pharmd| Rn-Bsn| Bcba| Msn Rn - Bc| Cipd| Cgfm| Cde| Ccue| Clss-Hc| Lcsw-R| Clcp| RnBsCcm| Cota/L| Bcgp| Rnc-Nic| Bba| Rnc-Ob| President| Instructor| remember| Recruiter| Advisor| Director| Relations| Trading| Manager| Program| Operation| Association| Hospital| Sphr| Accounting| Shrm| Veteran| Executive| Scp| Ssm| Experience| Constant| Fitness| Center| Centre| Summit| Expert| Market| Health| Leader| Psyd| Spsc| Phrca| Certified| Consultant| Effective| Medical| Device| Supervisor| Cism Cisa Cissp| Lssbb', flags=re.IGNORECASE)

df1 = df1.astype(str).apply(lambda x: x.str.replace(regex_pat2, '', regex=True))


# Keywords to be removed from end of the string

regex_pat3 = re.compile(r' acca$| Cam$| Cos$| Mc$| Mbcs$| Ms$| Rev$| Mcc$| Mcr$| Cit$| Osb$| Com$| Arm$| Ing$| Rid$| Rcs$| Rls$| Pta$| Ply$| Pbc$| Spc$| Sgm$| Asla$| Nha$| Cep$| Clu$| Flmi$| Cet$| Med$| Cia$| Ala$| Der$| Ahr$| Spc$| Cih$| Cima$| Cir$| Fpa$| Jmac$| Ilg$| Rem$| Rohn$| Rdh$| Rvs$| Grcp$| Grp$| Fhm$| Fmo$| Regt$| Gsec$| Oms$| Rfc$| Gms$| Rdcs$| Fnp$| Qpfc$| Ess$| Euw$| Gphr$| Qpa$| Erb$| Mn-E$| Fcsi$| Frm$| Itpm$| Rohm$| Rse$| Fpc$| Enz$| Rohr$| RtM$| Rwcs$| Icm$| OtrL$| Frs$| Rrt$| Fca$| Rde$| Osp$| Rix$| Rrp$| Iqc$| Fmp$| Olp$| Fscp$| Facp$| Esq$| Fsa$| Erp$| Ing$| Osb$| Edfp$| Rhia$| Rvt$| Gsp$| Jnr$| Inc$| Iws$| Rehs$| Ohst$| Fma$| Isr$| Rcdd$| Qssp$| Rdn$| Rpa$| Gisp$| Itil$| Rhit$| Rex$| Rost$| Rbsm$| Fsgm$| Rph$| Ricp$| Iii$| Phn$| Pws$| Pgmp$| Pga$| Picq$| Pbc$| Pcam$| Pmc$| Pcc$| Psc$| Psm-$| Pcs$| Prc$| Phr$| Pfmp$| Pcip$| Pls$| Popm$| Psp$| Psm$| Phd$| Pmp$| Mabc$| Mbt$| Mcj$| Mcse$| Mdiv$| Mc$| Mcc$| Mboe$| Mbec$| Mcp$| Mcs$| Mbm$| Mbe$| Mbcp$| Mbbs$| Mcr$| -Mba$| Mcsm$| Med$| Mba$| Pa-C$| Dvs$| Dvn$| Dvm$| Dtm$| Dsc$| Dsa$| Drph$| Dpt$| Dpm$| Dnp$| Dnp$| Dmcp$| Dma$| Dhsc$| Dha$| Der$| Dds$| Dbia$| Dba$| Cyr$| Cws$| Cwi$| Cwe$| Cwd$| Cwcm$| Cui$| Ctsm$| Cts$| Ctrs$| Ctr$| Ctp$| Ctie$| Ctfa$| Cte$| Ctb$| Cta$| Csw$| Cst$| Css$| Cspo$| Cspm$| Csp$| Csme$| Csm$| Csl$| Csi$| Cshm$| Csee$| Cscp$| Crx$| Crs$| Crps$| Crpc$| Crp$| Crnp$| Crme$| Crl$| Crfp$| Crcr$| Crcm$| Crc$| Crb$| Cqe$| Cqa$| Cpsm$| Cps$| Cprs$| Cprp$| Cprm$| Cppo$| Cppm$| Cpp$| Cpo$| Cpmr$| Cpmm$| Cpma$| Cpm$| Cpl$| Cpim$| Cpia$| Cpht$| Cpfs$| Cpfo$| Cpe$| Cpdw$| Cpcu$| Cpc$| Cpa$| Cos$| Com$| Cnp$| Cnn$| Cnmt$| Cmte$| Cmsp$| Cms$| Cmrp$| Cmps$| Cmp$| Cmm$| Cmlt$| Cmip$| Cmhs$| Cmfc$| Cme$| Cmca$| Cmb$| Cma$| Cltd$| Cltc$| Clpm$| Clp$| Clm$| Clcs$| Citp$| Cit$| Cisr$| Ciso$| Cism$| Cis$| Cipm$| Cid$| Cicp$| Cic$| Cia$| Chtm$| Chst$| Chs$| Chrm$| Chpn$| Chpa$| Chg$| Chfp$| Chfm$| Chfc$| Chdm$| Chcp$| Chc$| Cgsp$| Cgcs$| Cgc$| Cfsp$| Cfs$| Cfps$| Cfp$| Cfm$| Cfi$| Cfer$| Cfe$| Cfd$| Cfcm$| Cfa$| Ces$| Cep$| Ceo$| Cem$| Ceh$| Cec$| Cebs$| Cdt$| Cds$| Cdr$| Cdpe$| Cdp$| Cdmp$| Cdm$| Cdip$| Cdia$| Cdfa$| Cdbc$| Ccxp$| Ccwp$| Ccte$| Ccsp$| Ccsm$| Ccsl$| Ccs$| Ccrp$| Ccrm$| Ccra$| Ccpm$| Ccp$| Ccoa$| Ccnp$| Ccm$| Ccim$| Cchp$| Ccda$| Cbp$| Cbet$| Cbcp$| Cbap$| Cba$| Casp$| Caps$| Capp$| Capm$| Capb$| Capa$| Cap$| Cao$| Canl$| Cams$| Cam$| Cafs$| Cadc$| Awet$| Awe$| Aud$| Auci$| Auch$| Atc$| Asp$| Asla$| Asis$| Asid$| Ashm$| Ash$| Ascp$| Arx$| Arm$| Ark$| Ari$| Arb$| Aprn$| Apn$| Aphr$| Aos$| Anh$| Amo$| Amft$| Alst$| Alc$| Alba$| Ala$| Ais$| Airc$| Ains$| Aim$| Aifd$| Aifa$| Aif$| Aicp$| Aic$| Aia$| Ahr$| Ahn$| Aft$| Adr$| Acsw$| Acs$| Acra$| Acma$| Ace$| Aca$| Abv$| Abt$| Absi$| Aas$| Eit$', flags=re.IGNORECASE)

df1 = df1.astype(str).apply(lambda x: x.str.replace(regex_pat3, '', regex=True))


df = pd.concat([df, df1], axis=1)
df = df.astype(object).where(pd.notnull(df),None)


#MSSQL

conn = pymssql.connect(host = 'localhost', user = 'sa', password = '********', database = 'sqlserver')
cur = conn.cursor()

cur.execute("DROP FUNCTION IF EXISTS SPLIT_PART ;")
cur.execute("DROP FUNCTION IF EXISTS REGEXP_REPLACE;")
cur.execute("DROP TABLE IF EXISTS name_cleansing;")
cur.execute("CREATE FUNCTION SPLIT_PART ( @x VARCHAR(MAX), @y VARCHAR(512), @z INTEGER ) RETURNS VARCHAR(MAX) AS BEGIN RETURN REVERSE(PARSENAME(REPLACE(REVERSE(@x), @y, '.'), @z)) END ;")
cur.execute("CREATE FUNCTION REGEXP_REPLACE ( @x VARCHAR(MAX), @y VARCHAR(4096), @z VARCHAR(512) ) RETURNS VARCHAR(MAX) BEGIN DECLARE @res VARCHAR(MAX) = @x, @n VARCHAR(4096) = CONCAT('%', @y, '%') WHILE PATINDEX(@n, @res) > 0 BEGIN SET @res = STUFF(@res, PATINDEX(@n, @res), 1, @z) END RETURN @res END ;")
cur.execute("CREATE TABLE name_cleansing ( id VARCHAR(512), first_name VARCHAR(512), last_name VARCHAR(512), organization VARCHAR(512), linkedin_url VARCHAR(MAX), cleaned_first_name VARCHAR(512), cleaned_last_name VARCHAR(512), Remarks VARCHAR(512) ) ;")

# creating column list for insertion 
cols = ",".join([str(i) for i in df.columns.tolist()])

# Insert DataFrame records one by one. 
for i,row in df.iterrows():
    sql = "INSERT INTO name_cleansing (" +cols + ") VALUES (" + "%s,"*(len(row)-1) + "%s)"
    cur.execute(sql, tuple(row))
    conn.commit()

# Execute query

cur.execute("UPDATE name_cleansing SET cleaned_first_name = CASE WHEN cleaned_first_name LIKE ('%(%') AND cleaned_first_name LIKE ('%)%') THEN CONCAT(dbo.SPLIT_PART(cleaned_first_name, '(', 1), dbo.SPLIT_PART(cleaned_first_name, ')', 2)) ELSE cleaned_first_name END WHERE cleaned_first_name IS NOT NULL ;")
cur.execute("UPDATE name_cleansing SET cleaned_first_name = CASE WHEN LEFT(cleaned_first_name, 1) IN ('(', ')') THEN CASE WHEN LEFT(cleaned_first_name, 1) = '(' THEN dbo.SPLIT_PART(cleaned_first_name, '(', 2) WHEN LEFT(cleaned_first_name, 1) = ')' THEN dbo.SPLIT_PART(cleaned_first_name, ')', 2) END WHEN RIGHT(cleaned_first_name, 1) IN ('(', ')') THEN CASE WHEN RIGHT(cleaned_first_name, 1) = '(' THEN dbo.SPLIT_PART(cleaned_first_name, '(', 1) WHEN RIGHT(cleaned_first_name, 1) = ')' THEN dbo.SPLIT_PART(cleaned_first_name, ')', 1) END WHEN cleaned_first_name LIKE('%)%') AND LEFT(cleaned_first_name, 1) <> ')' AND ( LEN(cleaned_first_name) - LEN(REPLACE(cleaned_first_name, ')', '')) = 1 ) THEN dbo.SPLIT_PART(cleaned_first_name, ')', 2) WHEN cleaned_first_name LIKE('%(%') AND RIGHT(cleaned_first_name, 1) <> '(' AND ( LEN(cleaned_first_name) - LEN(REPLACE(cleaned_first_name, '(', '')) = 1 ) THEN dbo.SPLIT_PART(cleaned_first_name, '(', 1) ELSE cleaned_first_name END WHERE cleaned_first_name IS NOT NULL ;")
#cur.execute("UPDATE name_cleansing SET cleaned_first_name = CASE WHEN cleaned_first_name LIKE '%"%"%' AND LEFT(cleaned_first_name, 1) <> '"' AND RIGHT(cleaned_first_name, 1) <> '"' THEN CONCAT(dbo.SPLIT_PART(cleaned_first_name, '"', 1), dbo.SPLIT_PART(cleaned_first_name, '"', 3)) WHEN cleaned_first_name LIKE '%"%"%' AND LEFT(cleaned_first_name, 1) = '"' AND RIGHT(cleaned_first_name, 1) <> '"' THEN dbo.SPLIT_PART(cleaned_first_name, '"', 3) WHEN cleaned_first_name LIKE '%"%"%' AND LEFT(cleaned_first_name, 1) <> '"' AND RIGHT(cleaned_first_name, 1) = '"' THEN dbo.SPLIT_PART(cleaned_first_name, '"', 1) ELSE cleaned_first_name END WHERE cleaned_first_name IS NOT NULL ;")
cur.execute("UPDATE name_cleansing SET cleaned_first_name = CASE WHEN cleaned_first_name LIKE '%[%]%' AND LEFT(cleaned_first_name, 1) <> '[' AND RIGHT(cleaned_first_name, 1) <> ']' THEN CONCAT(dbo.SPLIT_PART(cleaned_first_name, '[', 1), dbo.SPLIT_PART(cleaned_first_name, ']', 2)) WHEN cleaned_first_name LIKE '%[%]%' AND LEFT(cleaned_first_name, 1) = '[' AND RIGHT(cleaned_first_name, 1) <> ']' THEN dbo.SPLIT_PART(cleaned_first_name, ']', 2) WHEN cleaned_first_name LIKE '%[%]%' AND LEFT(cleaned_first_name, 1) <> '[' AND RIGHT(cleaned_first_name, 1) = ']' THEN dbo.SPLIT_PART(cleaned_first_name, '[', 1) ELSE cleaned_first_name END WHERE cleaned_first_name IS NOT NULL ;")
cur.execute("UPDATE name_cleansing SET cleaned_last_name = CASE WHEN cleaned_last_name LIKE ('%(%') AND cleaned_last_name LIKE ('%)%') THEN CONCAT(dbo.SPLIT_PART(cleaned_last_name, '(', 1), dbo.SPLIT_PART(cleaned_last_name, ')', 2)) ELSE cleaned_last_name END WHERE cleaned_last_name IS NOT NULL ;")
cur.execute("UPDATE name_cleansing SET cleaned_last_name = CASE WHEN LEFT(cleaned_last_name, 1) IN ('(', ')') THEN CASE WHEN LEFT(cleaned_last_name, 1) = '(' THEN dbo.SPLIT_PART(cleaned_last_name, '(', 2) WHEN LEFT(cleaned_last_name, 1) = ')' THEN dbo.SPLIT_PART(cleaned_last_name, ')', 2) END WHEN RIGHT(cleaned_last_name, 1) IN ('(', ')') THEN CASE WHEN RIGHT(cleaned_last_name, 1) = '(' THEN dbo.SPLIT_PART(cleaned_last_name, '(', 1) WHEN RIGHT(cleaned_last_name, 1) = ')' THEN dbo.SPLIT_PART(cleaned_last_name, ')', 1) END WHEN cleaned_last_name LIKE('%)%') AND LEFT(cleaned_last_name, 1) <> ')' AND ( LEN(cleaned_last_name) - LEN(REPLACE(cleaned_last_name, ')', '')) = 1 ) THEN dbo.SPLIT_PART(cleaned_last_name, ')', 2) WHEN cleaned_last_name LIKE('%(%') AND RIGHT(cleaned_last_name, 1) <> '(' AND ( LEN(cleaned_last_name) - LEN(REPLACE(cleaned_last_name, '(', '')) = 1 ) THEN dbo.SPLIT_PART(cleaned_last_name, '(', 1) ELSE cleaned_last_name END WHERE cleaned_last_name IS NOT NULL ;")
#cur.execute("UPDATE name_cleansing SET cleaned_last_name = CASE WHEN cleaned_last_name LIKE '%"%"%' AND LEFT(cleaned_last_name, 1) <> '"' AND RIGHT(cleaned_last_name, 1) <> '"' THEN CONCAT(dbo.SPLIT_PART(cleaned_last_name, '"', 1), dbo.SPLIT_PART(cleaned_last_name, '"', 3)) WHEN cleaned_last_name LIKE '%"%"%' AND LEFT(cleaned_last_name, 1) = '"' AND RIGHT(cleaned_last_name, 1) <> '"' THEN dbo.SPLIT_PART(cleaned_last_name, '"', 3) WHEN cleaned_last_name LIKE '%"%"%' AND LEFT(cleaned_last_name, 1) <> '"' AND RIGHT(cleaned_last_name, 1) = '"' THEN dbo.SPLIT_PART(cleaned_last_name, '"', 1) ELSE cleaned_last_name END WHERE cleaned_last_name IS NOT NULL ;")
cur.execute("UPDATE name_cleansing SET cleaned_last_name = CASE WHEN cleaned_last_name LIKE '%[%]%' AND LEFT(cleaned_last_name, 1) <> '[' AND RIGHT(cleaned_last_name, 1) <> ']' THEN CONCAT(dbo.SPLIT_PART(cleaned_last_name, '[', 1), dbo.SPLIT_PART(cleaned_last_name, ']', 2)) WHEN cleaned_last_name LIKE '%[%]%' AND LEFT(cleaned_last_name, 1) = '[' AND RIGHT(cleaned_last_name, 1) <> ']' THEN dbo.SPLIT_PART(cleaned_last_name, ']', 2) WHEN cleaned_last_name LIKE '%[%]%' AND LEFT(cleaned_last_name, 1) <> '[' AND RIGHT(cleaned_last_name, 1) = ']' THEN dbo.SPLIT_PART(cleaned_last_name, '[', 1) ELSE cleaned_last_name END WHERE cleaned_last_name IS NOT NULL ;")
cur.execute("UPDATE name_cleansing SET cleaned_first_name = T2.cleaned_f_name FROM ( SELECT id, first_name, cleaned_first_name, cleaned_last_name, CASE WHEN cleaned_first_name = '' AND LEN(cleaned_last_name) - LEN(REPLACE(cleaned_last_name, ' ', '')) = 1 THEN dbo.SPLIT_PART(cleaned_last_name, ' ', 1) WHEN cleaned_first_name LIKE('Ed%') AND LEN(cleaned_first_name) = 2 THEN cleaned_first_name WHEN dbo.SPLIT_PART(cleaned_last_name, ' ', 1) LIKE 'de' THEN cleaned_first_name WHEN LEN(cleaned_last_name) - LEN(REPLACE(cleaned_last_name, ' ', '')) = 0 THEN cleaned_first_name WHEN LEN(cleaned_first_name) = 1 AND LEN(dbo.SPLIT_PART(cleaned_last_name, ' ', 1)) = 1 THEN cleaned_first_name WHEN LEN(cleaned_first_name) <= 2 OR cleaned_first_name = '' THEN CASE WHEN LEN(cleaned_last_name) - LEN(REPLACE(cleaned_last_name, ' ', '')) = 1 AND LEN(dbo.SPLIT_PART(cleaned_last_name, ' ', 1)) < 2 THEN cleaned_first_name WHEN LEN(cleaned_last_name) - LEN(REPLACE(cleaned_last_name, ' ', '')) = 1 THEN dbo.SPLIT_PART(cleaned_last_name, ' ', 1) WHEN LEN(cleaned_last_name) - LEN(REPLACE(cleaned_last_name, ' ', '')) = 2 THEN CASE WHEN LEN(dbo.SPLIT_PART(cleaned_last_name, ' ', 1)) >= 3 THEN dbo.SPLIT_PART(cleaned_last_name, ' ', 1) ELSE dbo.SPLIT_PART(cleaned_last_name, ' ', 2) END ELSE NULL END ELSE cleaned_first_name END AS cleaned_f_name FROM name_cleansing ) AS T2 WHERE name_cleansing.id = T2.id ;")
cur.execute("UPDATE name_cleansing SET cleaned_last_name = cleaned_l_name FROM ( SELECT id, last_name, cleaned_first_name, cleaned_last_name, CASE WHEN LEN(cleaned_last_name) - LEN(REPLACE(cleaned_last_name, ' ', '')) = 0 THEN cleaned_last_name WHEN LEN(cleaned_last_name) - LEN(REPLACE(cleaned_last_name, ' ', '')) = 1 THEN CASE WHEN LEN(dbo.SPLIT_PART(cleaned_last_name, ' ', 2)) = 1 AND LEN(dbo.SPLIT_PART(cleaned_last_name, ' ', 1)) = 1 THEN dbo.SPLIT_PART(cleaned_last_name, ' ',2) WHEN LEN(dbo.SPLIT_PART(cleaned_last_name, ' ', 2)) <= 2 THEN dbo.SPLIT_PART(cleaned_last_name, ' ', 1) ELSE dbo.SPLIT_PART(cleaned_last_name, ' ', 2) END WHEN LEN(cleaned_last_name) - LEN(REPLACE(cleaned_last_name, ' ', '')) = 2 THEN CASE WHEN (LEN(dbo.SPLIT_PART(cleaned_last_name, ' ', 3)) = 1 AND LEN(dbo.SPLIT_PART(cleaned_last_name, ' ', 2)) = 1) OR (LEN(dbo.SPLIT_PART(cleaned_last_name, ' ', 3)) = 1 AND LEN(dbo.SPLIT_PART(cleaned_last_name, ' ', 2)) = 1 AND LEN(dbo.SPLIT_PART(cleaned_last_name, ' ', 1)) = 1) THEN dbo.SPLIT_PART(cleaned_last_name, ' ',3) WHEN LEN(dbo.SPLIT_PART(cleaned_last_name, ' ', 3)) <=2 THEN CASE WHEN LEN(dbo.SPLIT_PART(cleaned_last_name, ' ', 2)) <=2 THEN dbo.SPLIT_PART(cleaned_last_name, ' ', 1) ELSE dbo.SPLIT_PART(cleaned_last_name, ' ', 2) END ELSE dbo.SPLIT_PART(cleaned_last_name, ' ', 3) END WHEN LEN(cleaned_last_name) - LEN(REPLACE(cleaned_last_name, ' ', '')) = 3 THEN CASE WHEN LEN(dbo.SPLIT_PART(cleaned_last_name, ' ', 4)) <=2 THEN CASE WHEN LEN(dbo.SPLIT_PART(cleaned_last_name, ' ', 3)) <=2 THEN CASE WHEN LEN(dbo.SPLIT_PART(cleaned_last_name, ' ', 2)) <=2 THEN dbo.SPLIT_PART(cleaned_last_name, ' ', 1) ELSE dbo.SPLIT_PART(cleaned_last_name, ' ', 2) END ELSE dbo.SPLIT_PART(cleaned_last_name, ' ', 3) END ELSE dbo.SPLIT_PART(cleaned_last_name, ' ', 4) END ELSE cleaned_last_name END AS cleaned_l_name FROM name_cleansing ) AS T2 WHERE name_cleansing.id = T2.id ;")
cur.execute("UPDATE name_cleansing SET cleaned_first_name = TRIM('%[-'' ]%' FROM dbo.REGEXP_REPLACE(cleaned_first_name, '[^a-z-'' ]', '')) WHERE cleaned_first_name IS NOT NULL ;")
cur.execute("UPDATE name_cleansing SET cleaned_last_name = TRIM('%[-'' ]%' FROM dbo.REGEXP_REPLACE(cleaned_last_name, '[^a-z-'' ]', '')) WHERE cleaned_last_name IS NOT NULL ;")
cur.execute("SELECT * FROM name_cleansing ;")

# Fetch all the records
result = cur.fetchall()
 
dfn = pd.DataFrame(result, columns = ['id', 'first_name', 'last_name', 'organization', 'linkedin_url', 'cleaned_first_name', 'cleaned_last_name', 'Remarks'])

date = "{:%Y_%m_%d_%H_%M_%S}".format(datetime.now())

dfn.to_csv('output_'+date+'.csv', index=False)


