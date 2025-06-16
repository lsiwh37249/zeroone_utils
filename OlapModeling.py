import pandas as pd
import os

class OlapModeling:

    def load(self):
        # 파일 경로 지정
        
        AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow")
        DATE = 20240101
        file_path = f"{AIRFLOW_HOME}/data/temp/order_log_{DATE}.csv"
        
        log_df = pd.read_csv(file_path)
        print(log_df)
        return log_df

    def dimension_customer(self,log_df_path,dim_customer_file_path): 
        
        log_df = pd.read_csv(log_df_path)

        if os.path.exists(dim_customer_file_path):
            dim_customer = pd.read_csv(dim_customer_file_path)
        else:
            dim_customer = pd.DataFrame(columns=['customer_id', 'customer_name', 'region'])
        
        ### dim_customer 업데이트
    
        # 새로운 고객 정보 추출
        new_customers = log_df[['customer_name', 'region']].drop_duplicates()
        
        # 기존 고객과 비교하여 신규 고객만 추출
        merged_customers = new_customers.merge(dim_customer, on=['customer_name', 'region'], how='left', indicator=True)
        new_entries = merged_customers[merged_customers['_merge'] == 'left_only'][['customer_name', 'region']]

        # 새 customer_id 부여
        start_id = dim_customer['customer_id'].max() + 1 if not dim_customer.empty else 1
        new_entries['customer_id'] = range(start_id, start_id + len(new_entries))
        print(new_entries)

        # 업데이트
        dim_customer_updated = pd.concat([dim_customer, new_entries], ignore_index=True)
        os.makedirs(os.path.dirname(dim_customer_file_path), exist_ok=True)
        dim_customer_updated.to_csv(dim_customer_file_path, index=False)

        return dim_customer_updated

    def dimension_product(self, log_df_path ,dim_product_updated_file_path):
        
        log_df = pd.read_csv(log_df_path)
        
        if os.path.exists(dim_product_updated_file_path):
            dim_product = pd.read_csv(dim_product_updated_file_path)
        else:
            # 컬럼 정의도 같이 해주는 것이 좋음 (일관성 유지)
            dim_product = pd.DataFrame(columns=['product_id', 'product_name', 'category'])
        ### dim_product 업데이트
        new_products = log_df[['product_name', 'category']].drop_duplicates()

        merged_products = new_products.merge(dim_product, on=['product_name', 'category'], how='left', indicator=True)
        new_product_entries = merged_products[merged_products['_merge'] == 'left_only'][['product_name', 'category']]

        start_id = dim_product['product_id'].max() + 1 if not dim_product.empty else 1
        new_product_entries['product_id'] = range(start_id, start_id + len(new_product_entries))
        print(new_product_entries)

        dim_product_updated = pd.concat([dim_product, new_product_entries], ignore_index=True)
        print(dim_product_updated)
        os.makedirs(os.path.dirname(dim_product_updated_file_path), exist_ok=True)
        dim_product_updated.to_csv(dim_product_updated_file_path, index=False)
        return dim_product_updated

    def fact(self, log_df_path, dim_product_path, dim_customer_path, fact_file_path):

        log_df = pd.read_csv(log_df_path)
        dim_customer_df = pd.read_csv(dim_customer_path) if os.path.exists(dim_customer_path) else \
            pd.DataFrame(columns=['customer_name', 'region', 'customer_id'])
        dim_product_df = pd.read_csv(dim_product_path) if os.path.exists(dim_product_path) else \
            pd.DataFrame(columns=['product_name', 'category', 'product_id'])
        fact_order_df = pd.read_csv(fact_file_path) if os.path.exists(fact_file_path) else \
            pd.DataFrame(columns=['order_id', 'order_date', 'customer_id', 'product_id', 'amount'])
        
        # 컬럼 순서 맞춤 (필요시)
        dim_customer_df = dim_customer_df[['customer_name', 'region', 'customer_id']]
        dim_product_df = dim_product_df[['product_name', 'category', 'product_id']]

        log_with_ids = log_df.merge(dim_customer_df, on=['customer_name', 'region'], how='left') \
                         .merge(dim_product_df, on=['product_name', 'category'], how='left')

        fact_new = log_with_ids[['order_id', 'order_date', 'customer_id', 'product_id', 'amount']].drop_duplicates()
        fact_combined = pd.concat([fact_order_df, fact_new], ignore_index=True)
        fact_order_updated = fact_combined.drop_duplicates(subset=['order_id'])

        os.makedirs(os.path.dirname(fact_file_path), exist_ok=True)
        fact_order_updated.to_csv(fact_file_path, index=False)
        
        return fact_order_updated

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
