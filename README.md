# H1B Analysis

### Unit 1 is in folder etl_code directory
### Unit 2 is in folder /profiling_code directory
### Unit 3, 5, 6 is in folder /code_iterations directory

### Unit 1 data clean - 2 source 
#### source 1 Minkang Yang - 2006 - h1b.txt
#### Step 1: Download data from http://www.flcdatacenter.com/CaseH1B.aspx -> Discontinued Systems -> H-1B Efile Data -> 2006(choose txt file) -> name it h1b.txt
#### Step 2: Run Mapreduce from code in folder step1-clean/(make file, run file, check file included) on h1b.txt. The result would be tab seperated 12-column file. The result is under hdfs dfs /user/my1843/project/clean/part-r-00000 -> rename to /user/my1843/project/clean/part1

#### source 2 Yubing Bai - H-1B_FY2018.xlsx
#### Step 1: Data source is from https://www.foreignlaborcert.doleta.gov/performancedata.cfm -> Disclosure Data -> H-1B_FY2018.xlsx
#### Step 2: Run Mapreduce from code in folder step1-clean/(make file, run file, check file included) on h1b.txt. The result would be tab seperated 12-column file. The result is under hdfs dfs /user/yb1059/project/cleanOutputUpdate -> rename and move to /user/my1843/project/clean/part2


### Unit 2 Profile
#### Minkang Yang - under folder unit2-profile-source1
#### result is resonable
#### Yubing Bai - under folder unit2-profile-source2
#### result is resonable


### Unit 3 Merge 2 source
#### step 1: put two file under the same folder, called hdfs dfs /user/my1843/project/clean/
#### step 2: run a hive command to create table merge the 2 sources into one table -> name it data20 . see command under folder unit3-merge2source/ for detail


### Unit 4 Industry Categorize
#### step 1: create table from data20, select only employer_name, job_title -> name it employer_name_job_title.
#### step 2: download employer_name_job_title -> dumbo -> hdfs /user/my1843/project/distinct_employer_name.tsv
#### step 3: Run Mapreduce from code in folder unit3-industry-categoty/key-word/(make file, run file, check file included) on distinct_employer_name.tsv -> result would be in /user/my1843/project/wordCount
#### step 4: create table from /user/my1843/project/wordCount -> table employer_name_count -> descending -> employer_name_count_order
#### step 5: from the employer_name_count_order, find the key word and put them into different category, namely Administrative, Architest And Engineer, Education, Medical, Computer, Finance and business, Entertainment.
#### step 6: Run MapReduce from code in unit3-industry-categoty/add-one-column/(make file, run file, check file included) ->result overwrite /user/my1843/project/clean
#### NOW /user/my1843/project/clean would be the newest tab separated file.
#### create table with 13-column, call it data30.


### Unit 5 Analyze in Hive
#### Step 1: analyze processing time
#### see code in HiveCommand under section analyze processing time

#### Step 2: analyze employing time
#### see code in HiveCommand under section analyze employing time

#### Step 3: analyze states distribution
#### see code in HiveCommand under section analyze states distribution

#### Step 4: analyze wage distribution
#### see code in HiveCommand under section analyze wage distribution

#### Step 5: analyze occupation distribution
#### see code in HiveCommand under section analyze occupation distribution


### Unit 6 Analyze in MLlib
#### step 1: generate vector [case_status, wage_rate, decision_time, employing_time, state, occupation], see code under section generate vector
#### step 2: export vector file from beeline to dumbo, then to hdfs
#### step 3: Run Map Reduce to create data vector for later trainning on MlLib.
#### step 4: export the data get from the last step to local folder unit6-AnalyzeInMlLib/ then open jupyter-notebook and run ML_Analysis.ipynb get the prediction model.
