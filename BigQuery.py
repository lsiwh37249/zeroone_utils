from google.cloud import bigquery
import os

class BigQuery:
    def __init__(self, dataset_id):
        self.project_id = os.getenv("PROJECT_ID")  # 환경변수 우선, fallback으로 인자 사용
        self.dataset_id = dataset_id
        self.client = bigquery.Client(project=self.project_id)

    def create_dataset(self, location="asia-northeast3", description=""):
        """데이터셋 생성"""
        dataset_ref = f"{self.project_id}.{self.dataset_id}"
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = location
        dataset.description = description

        dataset = self.client.create_dataset(dataset, exists_ok=True)
        print(f"📁 데이터셋 생성됨: {dataset.dataset_id} ({location})")

    def run_query(self, query):
        """SQL 쿼리 실행 후 DataFrame 반환"""
        query_job = self.client.query(query)
        result = query_job.result()
        return result.to_dataframe()

    def upload_dataframe(self, df, table_name, overwrite=False):
        """DataFrame을 테이블로 업로드"""
        table_id = f"{self.project_id}.{self.dataset_id}.{table_name}"
        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_APPEND",  # 기존에 같은 파티션 있으면 덮어쓰기 하려면 MERGE 필요
            time_partitioning=bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field="event_date",
            ),
        )
        job = self.client.load_table_from_dataframe(df, table_id, job_config=job_config)
        job.result()  # 완료될 때까지 대기
        print(f"✅ 업로드 완료: {table_id}")

    def get_table_schema(self, table_name):
        """테이블 스키마 확인"""
        table_id = f"{self.project_id}.{self.dataset_id}.{table_name}"
        table = self.client.get_table(table_id)
        return table.schema
