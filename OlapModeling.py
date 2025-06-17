import pandas as pd
import os

class OlapModeling:

    def load(self):
        # 파일 경로 지정
        
        AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow")
        DATE = 20250601
        file_path = f"{AIRFLOW_HOME}/data/temp/event_log_{DATE}.csv"
        
        log_df = pd.read_csv(file_path)
        print(log_df)
        return log_df
    
    def update_dimension_table(self, log_df_path, dim_file_path, keys, id_column_name):
        log_df = pd.read_csv(log_df_path)

        new_entries = log_df[keys].drop_duplicates()

        if os.path.exists(dim_file_path):
            dim_df = pd.read_csv(dim_file_path)
        else:
            dim_df = pd.DataFrame(columns=[id_column_name] + keys)

        merged = new_entries.merge(dim_df, on=keys, how='left', indicator=True)
        to_add = merged[merged['_merge'] == 'left_only'][keys]

        start_id = dim_df[id_column_name].max() + 1 if not dim_df.empty else 1
        to_add[id_column_name] = range(start_id, start_id + len(to_add))

        updated_dim = pd.concat([dim_df, to_add], ignore_index=True)

        # 저장
        updated_dim.to_csv(dim_file_path, index=False)

        return updated_dim

    def save(self, dim_customer_path, dim_product_path, fact_order_path):
        def save_file_if_exists(path, expected_columns):
            if os.path.exists(path):
                try:
                    df = pd.read_csv(path)
                    # 파일이 비어 있거나 컬럼이 일치하지 않으면 저장하지 않음
                    if df.empty:
                        print(f"⚠️ {path} is empty. Skipping save.")
                        return
                    if not all(col in df.columns for col in expected_columns):
                        print(f"⚠️ {path} has unexpected columns. Skipping save.")
                        return
                    df.to_csv(path, index=False)
                    print(f"✅ {path} 저장 완료")
                except Exception as e:
                    print(f"❌ {path} 저장 실패: {e}")
            else:
                print(f"⚠️ {path} 파일이 존재하지 않아 저장하지 않음")

        save_file_if_exists(dim_customer_path, ['customer_name', 'region', 'customer_id'])
        save_file_if_exists(dim_product_path, ['product_name', 'category', 'product_id'])
        save_file_if_exists(fact_order_path, ['order_id', 'order_date', 'customer_id', 'product_id', 'amount'])

    def send_slack_alert(self):
        print("send_slack")
