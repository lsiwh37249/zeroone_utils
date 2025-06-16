import pandas as pd
import os

class OlapModeling:

    def load(self):
        # 파일 경로 지정
        
        AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow")
        DATE = 20240101
        file_path = f"{AIRFLOW_HOME}/data/temp/order_log_{DATE}.csv"
        
        log_df = pd.read_csv(file_path)
        return log_df

    def dimension_customer(self,log_df):
        
        AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow")
        save_dir = os.path.join(AIRFLOW_HOME, "data/olap")
        
        os.makedirs(save_dir, exist_ok=True)
        os.makedirs(f'{save_dir}/dim', exist_ok=True)

        file_path = f'{save_dir}/dim/dim_customer.csv'
        if os.path.exists(file_path):
            dim_customer = pd.read_csv(file_path)
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
        
        return dim_customer_updated

    def dimension_product(self, log_df):
        
        AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow")
        save_dir = os.path.join(AIRFLOW_HOME, "data/olap")
        os.makedirs(save_dir, exist_ok=True)
        # 디렉토리 없으면 생성
        os.makedirs(f'{save_dir}/dim', exist_ok=True)
        
        file_path = f'{save_dir}/dim/dim_product.csv'
        
        if os.path.exists(file_path):
            dim_product = pd.read_csv(file_path)
        else:
            # 컬럼 정의도 같이 해주는 것이 좋음 (일관성 유지)
            dim_product = pd.DataFrame(columns=['product_name', 'category'])
        ### dim_product 업데이트
        new_products = log_df[['product_name', 'category']].drop_duplicates()

        merged_products = new_products.merge(dim_product, on=['product_name', 'category'], how='left', indicator=True)
        new_product_entries = merged_products[merged_products['_merge'] == 'left_only'][['product_name', 'category']]

        start_id = dim_product['product_id'].max() + 1 if not dim_product.empty else 1
        new_product_entries['product_id'] = range(start_id, start_id + len(new_product_entries))

        dim_product_updated = pd.concat([dim_product, new_product_entries], ignore_index=True)

        return dim_product_updated

    def fact(self,log_df,dim_customer_updated, dim_product_updated):
        

        AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow")
        save_dir = os.path.join(AIRFLOW_HOME, "data/olap")
        os.makedirs(save_dir, exist_ok=True)
        # 디렉토리 없으면 생성
        os.makedirs(f'{save_dir}/dim', exist_ok=True)

        file_path = f'{save_dir}/dim/fact_order.csv'

        if os.path.exists(file_path):
            fact_order = pd.read_csv(file_path)
        else:
            # 컬럼 정의도 같이 해주는 것이 좋음 (일관성 유지)
            fact_order = pd.DataFrame(columns=['order_id', 'order_date', 'customer_id', 'product_id', 'amount'])
        if dim_customer_updated is None:
            dim_customer_updated = pd.DataFrame(columns=['customer_name', 'region', 'customer_id'])

        if dim_product_updated is None:
            dim_product_updated = pd.DataFrame(columns=['product_name', 'category', 'product_id']) 
        
        ### dim_product 업데이트
        new_products = log_df[['product_name', 'category']].drop_duplicates()

       
        log_with_ids = log_df.merge(dim_customer_updated, on=['customer_name', 'region'], how='left') \
                             .merge(dim_product_updated, on=['product_name', 'category'], how='left')

        # Deduplication: 중복 주문 제거
        fact_new = log_with_ids[['order_id', 'order_date', 'customer_id', 'product_id', 'amount']].drop_duplicates()

        # 기존 팩트와 중복 제거 후 결합
        fact_combined = pd.concat([fact_order, fact_new], ignore_index=True)
        fact_order_updated = fact_combined.drop_duplicates(subset=['order_id'])
        
        return fact_order_updated

    def save(self, dim_customer_updated, dim_product_updated, fact_order_updated):
        ### 저장
        AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow")

        save_dir = os.path.join(AIRFLOW_HOME, "data/olap")

        os.makedirs(save_dir, exist_ok=True)
        
        # 디렉토리 없으면 생성
        os.makedirs(f'{save_dir}/dim', exist_ok=True)
        os.makedirs(f'{save_dir}/fact', exist_ok=True)

        if dim_customer_updated is None:
            dim_customer_updated = pd.DataFrame(columns=['customer_name', 'region', 'customer_id'])
        
        if dim_product_updated is None:
            dim_product_updated = pd.DataFrame(columns=['product_name', 'category', 'product_id'])
        
        dim_customer_updated.to_csv(f'{save_dir}/dim/dim_customer.csv', index=False)
        dim_product_updated.to_csv(f'{save_dir}/dim/dim_product.csv', index=False)
        fact_order_updated.to_csv(f'{save_dir}/fact/fact_order.csv', index=False)

        print("✅ dim_customer, dim_product, fact_order 업데이트 완료")
    def send_slack_alert(self):
        print("send_slack")
