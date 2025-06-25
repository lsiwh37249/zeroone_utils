from google.cloud import bigquery
import pandas as pd
import os
class BigQueryDimLoader:
    def upload_append(self, project_id, dataset_id, df: pd.DataFrame, table_name: str):
        table_id = f"{project_id}.{dataset_id}.{table_name}"
        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_APPEND",  # 새로운 row만 append
            autodetect=True
        )
        print(f"⬆️ Appending {len(df)} rows to {table_name}")
        load_job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
        load_job.result()

    def load(self, project_id, dataset_id, dim_dir):
    
        # 각 파일에 대한 처리 정보 (keys 및 id 컬럼 정의)
        dim_table_configs = {
            "dim_member": {
                "keys": ["dl_member_id"],
                "id_column_name": "member_id"
            },
            "dim_event_type": {
                "keys": ["event"],
                "id_column_name": "event_type_id"
            },
            "dim_study": {
                "keys": ["dl_study_id"],
                "id_column_name": "study_id"
            },
            "dim_date": {
                "keys": ["date"],
                "id_column_name": "date_id"
            },
            "dim_time": {
                "keys": ["time"],
                "id_column_name": "time_id"
            }
        }

        # 실행
        for file_name in os.listdir(dim_dir):
            if not file_name.endswith(".csv"):
                continue

            dim_name = file_name.replace(".csv", "")
            config = dim_table_configs.get(dim_name)
            if not config:
                print(f"config for {dim_name} not found. Skipping.")
                continue

            file_path = os.path.join(dim_dir, file_name)
            df = pd.read_csv(file_path)
            upload_bigquery(df_processed, dim_name)

