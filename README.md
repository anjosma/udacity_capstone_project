Jupyter Notebook Container:
``` console
$ docker run -p 8888:8888 --name capstone_udacity_notebook -v /home/matheus/projects/udacity/data_engineering_nanodegree/udacity_data_engineering_capstone_project/:/src/notebook -v /home/matheus/projects/udacity/data_engineering_nanodegree/udacity_data_engineering_capstone_project/data/:/src/data -d anjosma/jupyter_env
```