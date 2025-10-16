# Product Analytics Data Pipeline

A production-ready, modular data pipeline that simulates product analytics events, ingests them into Snowflake, transforms data with Snowpark Python, and visualizes insights through Streamlit. The stack is fully containerized with Docker, provisioned via Terraform, and orchestrated using Airflow or the CLI for flexibility across environments.

## Key Features

- Simulates realistic event and session data (CSV, JSON)
- Implements Snowflake’s multi-layer schema design (Bronze → Silver → Gold)
- Containerized transformation scripts using Snowpark inside Docker
- Interactive dashboards for KPIs and behavioral insights (Streamlit)
- Infrastructure as Code with Terraform for Snowflake resource setup
- Continuous Integration with GitHub Actions (linting, tests, workflow automation)
- DAG-based orchestration using Apache Airflow (Dockerized)


## Tech Stack

    Layer                   |  Technology / Tool               
    ------------------------+----------------------------------
    Data Simulation         |  Python                          
    Data Ingestion          |  Python, Snowflake Connector     
    Data Transformation     |  Snowpark for Python (in Docker) 
    Infrastructure          |  Terraform (Snowflake provider)  
    Orchestration           |  Apache Airflow (via Docker Compose)
    Data Warehouse          |  Snowflake                       
    Visualization           |  Streamlit                       
    CI/CD                   |  GitHub Actions                  
    Environment Management  |  .env,secrets.toml               


## Architecture Overview

- Data simulation produces CSV/JSON files with event and session data.
- Data is ingested into Snowflake’s Bronze layer (raw).
- Snowpark transformation scripts run inside Docker containers to move data from Bronze to Silver and Gold layers. 
> 💡 *Note: Dockerization was initially a workaround for Windows compatibility 
issues with Snowpark. In production, Airflow orchestrates the scripts directly.*
- Data is visualized via a Streamlit dashboard displaying KPIs and product insights.
- Apache Airflow orchestrates pipeline workflows, running in Docker containers with dependencies managed via Docker Compose.
- Terraform automates provisioning of Snowflake infrastructure, including roles, databases, and schemas.
- CI/CD pipelines automate code quality checks and deployment via GitHub Actions.


## Folder Structure

    product-analytics-pipeline/
    ├── airflow/                    # Airflow DAGs and Docker Compose setup
    │   ├── dags/
    │   ├── docker-compose.yaml
    │   ├── Dockerfile              # Airflow Dockerfile with Snowpark installed
    │   └── requirements.txt
    ├── dashboard/                  # Streamlit app source code
    │   ├── app.py
    │   ├── pages/
    │   └── utils/
    ├── data/raw/                   # Simulated raw data inputs (CSV, JSON)
    ├── docs/                      # Documentation, architecture diagrams, screenshots
    ├── scripts/                   # Data pipeline scripts and Dockerfiles
    │   ├── simulate_events.py
    │   ├── ingestion_to_snowflake.py
    │   ├── bronze_to_silver/       # Dockerized transformation scripts (initial workaround)
    │   └── gold_aggregation/       # Dockerized transformation scripts (initial workaround)
    ├── terraform/                 # Terraform configs for Snowflake infrastructure
    │   ├── main.tf
    │   ├── variables.tf
    │   ├── provider.tf
    │   └── terraform.tfvars       # Terraform variables file including Snowflake (excluded from git)
    ├── .github/workflows/         # GitHub Actions CI/CD pipelines
    │   └── ci.yml
    ├── .env                      # Environment variable file (excluded from git)
    └── README.md                  # Project documentation


## Prerequisites

Before running the project, ensure you have the following installed and configured on your system:

- Git (to clone the repository)
- Terraform (for provisioning Snowflake infrastructure)
- Docker and Docker Compose (to run Airflow and containerized scripts)
- Python (for simulation and ingestion scripts)
- A valid Snowflake account with the necessary permissions

## How to Run the Project

This section guides you through setting up and running the full product analytics data pipeline.

### 1. Clone the Repository

```bash
git clone https://github.com/Niave/product-analytics-pipeline.git
cd product-analytics-pipeline
```
### 2. Configure Snowflake Credentials

Create a .env file in the project root with your Snowflake credentials:

```text
SNOWFLAKE_ACCOUNT=your_account
SNOWFLAKE_USER=your_user
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_WAREHOUSE=your_warehouse
SNOWFLAKE_DATABASE=your_database
SNOWFLAKE_ROLE=your_role
```

In your terraform/ directory, create a file named terraform.tfvars with the following content to supply credentials securely to Terraform:

```text
snowflake_username = your_user
snowflake_password = your_password
snowflake_account  = your_account
snowflake_region   = your_region
```

### 3. Provision Snowflake Infrastructure Using Terraform

Navigate to the Terraform directory and run the following commands:
```bash
cd terraform
terraform init
terraform plan
terraform apply
```

- terraform init initializes the working directory.
- terraform plan shows the resources Terraform will create or update.
- terraform apply provisions the infrastructure on Snowflake.

> 💡*Note: Running Terraform requires that your Snowflake user has the right privileges to create databases, warehouses, roles, and users.*

### 4. Build and Run Airflow with Docker Compose

Ensure your Docker daemon is running, then build and start the Airflow environment:

```bash
cd airflow
docker-compose build
docker-compose run airflow-webserver airflow db init
docker-compose up -d airflow-scheduler airflow-webserver
docker-compose run --rm airflow-webserver airflow users create --username admin --firstname Admin --lastname User --role Admin --email admin@example.com --password your_password
```
Open your browser at http://localhost:8080 and log in with the username and password you set (default: airflow/airflow if not changed).

### 5. Trigger Pipeline in Airflow

- Find and trigger the DAG named product_analytics_pipeline.
- Monitor task progress and logs through the UI.

### 6. Launch the Streamlit Dashboard

In your project root directory, create a folder named `.streamlit` and inside it, create a file named `secrets.toml` with the following content to securely supply your Snowflake credentials to Streamlit:

```text
[snowflake]
user = your_user
password = your_password
account = your_account
warehouse = your_warehouse
database = your_database
schema = "GOLD"
role = your_role
```
In a separate terminal window, run the Streamlit dashboard:

```bash
streamlit run dashboard/app.py
```
Visit the URL output by Streamlit (usually http://localhost:8501) to explore product analytics dashboards.

### Notes and Considerations

- Dockerization of transformation scripts inside scripts/bronze_to_silver and scripts/gold_aggregation was an initial development workaround for Windows compatibility issues with Snowpark. The production Airflow DAGs run the original scripts mounted into the container.
- Ensure Docker containers and resources have sufficient memory and CPU allocated to avoid build or runtime failures.
- Keep all sensitive credentials outside of source control.

This stepwise setup reflects a typical production-style workflow starting from infrastructure provisioning with Terraform, containerized orchestration with Airflow, and data visualization with Streamlit.

If you want me to help write this section directly into your README file or suggest improvements, just ask!