Jupyter Notebook Container:
``` console
$ docker run -p 8888:8888 --name capstone_udacity_notebook -v /home/matheus/projects/udacity/data_engineering_nanodegree/udacity_data_engineering_capstone_project/:/src/notebook -v /home/matheus/projects/udacity/data_engineering_nanodegree/udacity_data_engineering_capstone_project/data/:/src/data -d anjosma/jupyter_env
```

Airflow:
``` console
$ mkdir ./dags ./logs ./plugins
$ echo -e "AIRFLOW_UID=$(id -u)\nAIRFLOW_GID=0" > .env
$ docker-compose up airflow-init
$ docker-compose up
```