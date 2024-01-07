# ETL airflow with postgresql

- wsl --update
- wsl --install -d ubuntu
- apt update
- sudo apt upgrade
- sudo apt install python3.10
- sudo apt install python3.10-distutils
- wget https://bootstrap.pypa.io/get-pip.py
- sudo python3.10 get-pip.py
- python3 --version
- apt install python3-pip
- export AIRFLOW_HOME=~ /airflow/
- pip3 install apache-airflow
- mkdir dags
- wsl --shutdown
- apt-get install postgresql
- sudo -u postgres psql
- postgres=# create database - airflow_metadata;
- postgres=# CREATE USER airflow WITH password 'password';
- postgres=# grant all privileges on database airflow_metadata to airflow;
- airflow db init
- chmod -R 777 ./
- load_examples = False
- executor = LocalExecutor
- sql_alchemy_conn = mysql://Airflow:1@localhost:33061/Airflow
или sql_alchemy_conn= postgresql+psycopg2://airflow:airflow@localhost:5432/airflow
- catchup_by_default = False
- web_server_host = 172.16.1.29
- pip install psycopg2-binary
- pip install --upgrade apache-airflow[postgres]
- pip install werkzeug==2.3
- sudo apt update
- airflow scheduler -D
- airflow webserver -p 8080 -D
- pkill -f "airflow webserver"
- rm /root/airflow/airflow-webserver.pid


help for install postgresql
https://ruvds.com/ru/helpcenter/postgresql-pgadmin-ubuntu/
