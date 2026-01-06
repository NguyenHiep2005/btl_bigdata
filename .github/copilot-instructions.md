# AI Coding Agent Instructions for btl_bigdata

## Project Overview
Traffic prediction system using Apache Spark ML, real-time data generation, and MongoDB. Three independent processes run in parallel:
1. **data_generator.py**: Fetches real traffic data from TomTom API (or simulates), stores in `traffic_db.traffic_data`
2. **prediction_pyspark.py**: Reads latest data per road, trains RandomForest model, predicts `congestion_level` (0-1), **adds `congestion_status` field** (THÔNG THOÁNG/TẮC/NGUY HIỂM), writes to `traffic_db.predictions`
3. **backend/app.py**: Flask REST API serving dashboard from `traffic_db.predictions`

## Critical Data Flow Architecture
```
TomTom API → traffic_data (raw) → Spark job → predictions (with status) → API → Dashboard
                                        ↓
                              ML model decision on congestion_level
```

**KEY REQUIREMENT**: Spark job MUST enrich predictions with `congestion_status` BEFORE MongoDB insert:
- `congestion_status = NGUY HIỂM` if prediction > 0.7
- `congestion_status = TẮC` if 0.4 < prediction ≤ 0.7  
- `congestion_status = THÔNG THOÁNG` if prediction ≤ 0.4

## Stack & Dependencies
- **Spark**: 3.2.4 with MongoDB connector (`mongo-spark-connector_2.12:3.0.1`)
- **MongoDB**: Container on `localhost:27017` (user: `root`, pass: `password`)
- **Flask**: 2.3.2 with CORS enabled
- **Python**: 3.10+ required

## Development Workflow

### Setup (one-time)
```bash
# Create venv & install
python3.10 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# Start MongoDB + Mongo Express (Terminal 1)
docker-compose up

# Terminal 2: Generate fake/real traffic data
python data_generator.py

# Terminal 3: Run Spark predictions (every 5 seconds)
python prediction_pyspark.py

# Terminal 4: Start Flask backend
python backend/app.py

# Access dashboard at http://localhost:5000
```

### API Endpoints
- `GET /api/current-predictions` → Latest status for all roads (from `predictions` collection)
- `GET /api/road-status/<road_id>` → Single road detail
- `GET /api/statistics` → Aggregate counts (NGUY HIỂM/TẮC/THÔNG THOÁNG)
- `GET /health` → Service health check

### Database Collections
- **traffic_data**: Raw input from API (speed, vehicle_count, timestamp, road_id, lat, lon)
- **predictions**: Spark output with ML enrichment (includes `congestion_status` from UDF)
- Mongo Express UI at `http://localhost:8081`

## Key Code Patterns

### Spark Data Flow (prediction_pyspark.py)
```python
# 1. Read from traffic_data (latest only per road)
# 2. Feature engineering (extract hour, is_peak for 7-9/17-19)
# 3. Transform with pipeline (VectorAssembler → RandomForestRegressor)
# 4. Convert prediction number to status text via UDF
# 5. Write enriched result to predictions collection
```

### MongoDB Aggregation (mongo_service.py)
Uses `$group` + `$first` to get **newest record per road_id** (not all records). If `congestion_status` missing, fallback logic applies (should not happen if Spark worked).

### Logging
- All processes log to `logs/` directory
- Levels set to INFO; check `logs/prediction_spark.log` for Spark job issues

## Common Issues & Solutions

| Issue | Cause | Fix |
|-------|-------|-----|
| No `congestion_status` in results | Spark UDF not applied | Verify `status_udf` registered before `.select()` in `make_predictions()` |
| MongoDB connection error | Container not running | Run `docker-compose up` first |
| Spark job hangs | Insufficient data in `traffic_data` | Check `data_generator.py` is running; Spark requires ≥10 rows to train |
| API returns empty predictions | Query reads from wrong collection | Confirm routes.py calls `mongo_service.get_latest_predictions()` which queries `predictions`, not `traffic_data` |

## When Modifying...

**Spark ML pipeline**: Update feature list in `VectorAssembler` inputCols + `feature_engineering()` consistently  
**Status thresholds**: Modify `determine_status()` UDF logic (currently 0.7/0.4 cutoffs)  
**MongoDB connection**: Must match all three files: `data_generator.py`, `prediction_pyspark.py`, `backend/config.py`  
**Collection names**: If renaming, sync across all three files + update routes  

## Testing
- Manual: Check `mongo-express` UI to verify collections and documents  
- Logs: Tail relevant log file (`logs/prediction_spark.log` for Spark issues)
- API: Use `curl http://localhost:5000/api/health` to verify Flask is running
