# SkillCorner Data Processing & Analytics

## Introduction

## Setup
1. Create a virtual environment `python3 -m venv .venv` and activated it `.venv\Scripts\Activate.ps1` (Windows) or `.venv/bin/activate` (Linux).
2. Install the dependencies `pip install -r requirements.txt`.
3. Create data folders 
    ```
    mkdir data
    mkdir data/raw
    mkdir data/processed
    mkdir data/metadata
    ```
4. Place the match data files (`{match_id}_tracking.txt`) in the `/data/raw` folder, while the metadata files (`{match_id}_metadata.json`) in the `data/metadata` folder.
    ```
    data
    ├───metadata
    │       10000_metadata.json
    │       100094_metadata.json
    │       10009_metadata.json
    │       10013_metadata.json
    │       10017_metadata.json
    ├───processed
    └───raw
            10000_tracking.txt
            100094_tracking.txt
            10009_tracking.txt
            10013_tracking.txt
            10017_tracking.txt
    ```
5. Run `python src/main.py` to process the data.


## Spark Jobs
For running the Spark jobs, we use the [Bitnami Spark Docker image](https://hub.docker.com/r/bitnami/spark).

The folders are mounted to the container volume.
The following commands are used to run the Spark jobs:
```bash
docker-compose up -d # Starts the Spark cluster
docker logs src-ftbl-spark-master-1 # Get the Spark master IP address, example: spark://172.20.0.2:7077
docker exec src-ftbl-spark-master-1 spark-submit --master spark://172.20.0.2:7077 /mounted-data/src/most_intense_5_minutes.py
docker exec src-ftbl-spark-master-1 spark-submit --master spark://172.20.0.2:7077 /mounted-data/src/highest_spread_2_minutes.py
```

