generate:
python3 ingestion/generate_transactions.py

upload:
python3 ingestion/upload_to_gcs.py

score:
python3 models/score_predictions.py

dashboard:
streamlit run dashboard/app.py

airflow-up:
docker compose up airflow-webserver airflow-scheduler -d

airflow-down:
docker compose down
