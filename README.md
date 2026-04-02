# Kedra Pipeline

This project scrapes WRC decisions, stores raw files and metadata in a landing zone, then transforms them into a curated zone.

## To run the wokflow

1. Install dependencies with `pip install -r requirements.txt`
2. Start storage with `docker compose up -d`
3. Verify storage services are up with `python check_storage_services.py`
4. Open MinIO at `http://localhost:19001`
5. Check MongoDB collections using `mongosh`
6. To trigger Dagster-ETL:
Run:

```powershell
dagster dev -f dags/dagster_job.py
```
```powershell
python -c "from dags.dagster_job import ingestion_pipeline; result = ingestion_pipeline.execute_in_process(); print(result.success)"
```

7. Visualize the data
### MinIO UI

Open:

```text
http://localhost:19001
```

Login using values from [\.env](c:/Users/user/Documents/Kedra/.env):

- username: `minio`
- password: `minio123`

You should see:

- `raw` : raw downloaded files
- `curated`: transformed files

### MongoDB

You can inspect MongoDB from the the container - MongoShell.

```powershell
python -c "from pymongo import MongoClient; c=MongoClient('mongodb://127.0.0.1:27017'); db=c['data']; print('collections=', db.list_collection_names()); print('raw_count=', db['raw_metadata'].count_documents({})); print('curated_count=', db['curated_metadata'].count_documents({})); print('raw_one=', db['raw_metadata'].find_one()); c.close()"
```