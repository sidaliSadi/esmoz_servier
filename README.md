# esmoz_servier 
Pyspark pipline and SQL queries

## Prepare envirenemnt
1. Create virtual envirenement `python3 -m venv venv`
2. Activate venv `source venv/bin/activate`
3. Run `git clone https://github.com/sidaliSadi/esmoz_servier`
4. Run `cd esmoz_servier`
5. Run `pip install -r requirements.txt` to install dependencies

## Run python pipline 
1. `python main.py` This will run the entire pipline (read process files, save the result and print the ad_hoc also)
### the json file resulted
![result data](images/output_result.png?raw=true "json file as tree")

## Prepare mysql in docker container to run queries
1. Open new terminal
2. `docker run -it --name test -e MYSQL_ROOT_PASSWORD=root -d mysql`
3. `docker exec -it test bash`
4. `mysql -p` PASSWORD is `root` Now we can execute queries on our instance
5. Open new terminal this time in esmoz_servier directory
6. Run `docker cp data/sql_data/. test://var/lib/mysql-files/`
7. Run `docker cp queries.sql test://var/lib/mysql-files/`
8. in the terminal open at (1) `source /var/lib/mysql-files//queries.sql` to execute script in queries.sql 

## Run tests
`pytest` tape this in the terminal


### Code folder architecture
```
.
├── data
│   └── sql_data                    data folder containing data to populate transaction table
    └── clinical_trials.csv         
    └── pubmed.csv
    └── drugs.csv 
└── images                          folder containing the preview image of the result  
└── result                          data folder containing the output of the pipline 
└── src
    ├── common
        ├──  init_spark.py         pyspark initialization

    ├── ad_hoc.py                  
    └── graph.py                   represent the output (csv) to tree (json)
    └── spark_pipline.py     
└── tests                          folder containing test files      
```