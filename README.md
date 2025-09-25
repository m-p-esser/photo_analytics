# About
This Project is about collecting data from Unsplash using the Google Cloud Platform 
for Compute and Prefect as Orchestration Tool

# Prerequisites
- Setup a **Google Cloud Project**
- Setup a **Prefect Cloud** account
- Install **Task** (taskfile.dev)
- Install Python Package and Project Manager **UV**

# Project Structure
.
├── Dockerfile
├── explore.ipynb
├── pyproject.toml
├── README.md
├── setup.py
├── src
│   ├── gcs.py
│   ├── prefect
│   └── request.py
├── Taskfile.yml
└── uv.lock

# Technologies
- Prefect for Orchestration
- Google Cloud Runs for Execution
- Google Cloud Storage as Raw Data Layer
- Bigquery as Transformation Layer

# Statistics
- Metadata for XXX images