from google.cloud import bigquery
import pandas as pd
import os

class BigQueryOlapModeling():

    def load_csv(self, csv_path, project_id, dataset_id, table_name, partition_field=None, write_mode="WRITE_TRUNCATE"):
        
        client = bigquery.Client(project=project_id)
        
        df = pd.read_csv(csv_path)
        df['event_timestamp'] = pd.to_datetime(df['event_timestamp']) 
        table_id = f"{project_id}.{dataset_id}.{table_name}"
        
        job_config = bigquery.LoadJobConfig(
            write_disposition=write_mode,
            autodetect=True,
            source_format=bigquery.SourceFormat.CSV
        )
        
        if partition_field:
            job_config.time_partitioning = bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field=partition_field
            )
        
        job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
        job.result()
        
        print(f" {csv_path} → {table_id} ({len(df)} rows) 업로드 완료")

    def merge_staging_to_fact(self, project_id, dataset_id, staging_table, fact_table):
        
        client = bigquery.Client(project=project_id)
        
        merge_sql = f"""
        MERGE `{project_id}.{dataset_id}.{fact_table}` T
        USING `{project_id}.{dataset_id}.{staging_table}` S
        ON T.event_timestamp = S.event_timestamp AND T.fact_event_id = S.fact_event_id
        WHEN MATCHED THEN
            UPDATE SET
                T.member_id = S.member_id,
                T.event_type_id = S.event_type_id,
                T.date_id = S.date_id,
                T.time_id = S.time_id,
                T.study_id = S.study_id
        WHEN NOT MATCHED THEN
            INSERT (member_id, event_type_id, date_id, time_id, study_id, event_timestamp, fact_event_id)
            VALUES(S.member_id, S.event_type_id, S.date_id, S.time_id, S.study_id, S.event_timestamp, S.fact_event_id)
        """

        job = client.query(merge_sql)
        job.result()
        print(f" MERGE 완료: {staging_table} → {fact_table}")


    def create_table(self, project_id, dataset_id, table_id,partition_field=None):
        client = bigquery.Client(project=project_id)

        table_ref = f"{project_id}.{dataset_id}.{table_id}"

        schema = [
            bigquery.SchemaField("member_id", "INT64", mode="REQUIRED"),
            bigquery.SchemaField("event_type_id", "INT64", mode="REQUIRED"),
            bigquery.SchemaField("date_id", "INT64", mode="REQUIRED"),
            bigquery.SchemaField("time_id", "INT64", mode="REQUIRED"),
            bigquery.SchemaField("study_id", "INT64", mode="REQUIRED"),
            bigquery.SchemaField("event_timestamp", "TIMESTAMP", mode="REQUIRED"),
            bigquery.SchemaField("fact_event_id", "STRING", mode="REQUIRED"),
        ]

        table = bigquery.Table(table_ref, schema=schema)
        # 하루 단위 파티셔닝 설정
        table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field=partition_field # 이 컬럼 기준으로 파티셔닝
        )   

        table = client.create_table(table, exists_ok=True)  # exists_ok=True: 이미 있으면 무시

        print(f"Created table {table.project}.{table.dataset_id}.{table.table_id}")
 
