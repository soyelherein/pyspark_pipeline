# pyspark_pipeline
### Project Structure:
```
   |swadesh007/pyspark_pipeline
   |-- config/  #Prod and Dev Config files i.e. input and output path for different environment
   |   |-- config.ini
   |-- jobs/  #Contains the main ETL logics for the pipeline
   |   |-- pipeline.py
   |-- tests/  #Unittest cases w.r.t extract, transform, load and other functionality modules
   |   |-- fixtures/
   |   |-- | -- fixtures.json #unittest configuration such as input data, expected output etc
   |   |-- PySparkTest.py #unittest setup and teardown code
   |   |-- test_pipeline.py #pipeline unit testing code
   |-- utils/  #Pipeline modules,coupling/decoupling with app layer will be discussed
   |   |-- utilities.py #contains generic code to start spark session, configuration parsing etc.
   |   |-- schema.py #schema definition of input and output
   |-- .env
   |-- .gitignore
   |-- Makefile
   |-- Pipfile
   |-- Pipfile.lock
   |-- README.md
   ```


