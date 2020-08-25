# PyCon 2020 Demo

## Build ENV
``` 
conda create -n airflow-tutorials python=3.7 -y
conda activate airflow_tutorials
pip install "apache-airflow[sqlite]"
export AIRFLOW_HOME="$(pwd)"
airflow initdb
```

## Config 
``` 
Config file : vim /airflow/airflow.cfg
Change dags_folder : dags_folder = /name/airflow_practice/dags
```

## Create SQLite database and table
``` 
sqlite3 //tmp/sqlite_default.db

CREATE TABLE KEYWORD(
    ID INTEGER PRIMARY KEY AUTOINCREMENT,
    Forum VARCHAR(50),
    KeywordList TEXT,
    FinishTimestamp DATETIME DEFAULT CURRENT_TIMESTAMP
);

.quit
```

## Run
```
nohup airflow scheduler &
airflow webserver -p 8080
```
