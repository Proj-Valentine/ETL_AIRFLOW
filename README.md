# ETL_AIRFLOW
**ETL Project with DuckDB, Snowflake Model, and Airflow**

This project aims to perform an Extract, Transform, and Load (ETL) process using DuckDB as the extraction database, creating a Snowflake model, and inserting the data from CSV files into it. The project utilizes Airflow to orchestrate and schedule the various tasks involved in the ETL process, all within a Docker container.

**Project Structure:**

1. `etl_project/`: The root directory of the project.
2. `etl_project/docker-compose.yml`: The Docker Compose file for configuring and running the Airflow environment.
3. `etl_project/dags/`: The directory containing Airflow DAGs (Directed Acyclic Graphs) for defining the ETL workflow.
4. `etl_project/dags/my_mod/`: The directory containing utility files, such as helper functions, configurations, or custom operators.
5. `etl_project/dags/my_data/`: The directory to store CSV files used as the source data for the ETL process.
6. `etl_project/models/`: The directory containing the Snowflake model definitions, including tables and relationships.
7. `etl_project/reports/`: The directory to store generated reports or outputs of the ETL process.
8. `etl_project/config/`: The directory containing configuration files for Airflow, database connections, or other environment-specific settings.

**Setup Instructions:**

1. Install Docker and Docker Compose on your system.
2. Clone this repository to your local machine.
3. Place the CSV files in the `etl_project/dags/my_data/` directory.
4. Configure the necessary database connections in the Airflow configuration files located in `etl_project/config/`.
    - for this project duckdb was used which can be installed with pip (`pip install duckdb`)
5. Customize the Airflow DAGs in the `etl_project/dags/my_custom_dags` directory to define your ETL workflow and tasks.
6. Adjust the Snowflake model definitions in the `etl_project/models/` directory to match your desired schema and tables.
7. Run the command `docker-compose up` in the project's root directory to start the Airflow environment in a Docker container.
8. Access the Airflow web interface in your browser at `http://localhost:8080` (or the specified port).
9. Use the Airflow interface to trigger and monitor the ETL process.

**Running Docker:**
- Install docker engine
- Set up docker compose file: you can copy the default from airflow website and modify for your use case or create your own
- Run `docker-compose up -d` to run docker container in the background and shutdown `docker-compose down -v`
- Initialize airflow again - `docker-compose up airflow-init` 

**Notes:**

- DuckDB is used as the extraction database, but you can modify the connection and use another database if preferred.
- Ensure that the required Python packages, dependencies, and versions are specified in the `Dockerfile` or `requirements.txt` file.
- Customize the Docker Compose file, if needed, to adjust container configurations or add additional services.
- Refer to the Airflow documentation for more details on defining DAGs, operators, and scheduling tasks.

With this setup, you can perform the ETL process using DuckDB as the source and target , create a Snowflake model, and load the data from CSV files into it, all orchestrated by Airflow within the Docker environment.

Please feel free to reach out if you have any questions or need further assistance.