class BigQueryFetcher:
    def make_temp_data(self):
        import pandas as pd
        import numpy as np
        import os

        # 고객 3명
        customers = [
            {'customer_name': 'Alice', 'region': 'Seoul'},
            {'customer_name': 'Bob', 'region': 'Busan'},
            {'customer_name': 'Charlie', 'region': 'Incheon'},
        ]

        # 제품 2개
        products = [
            {'product_name': 'Laptop', 'category': 'Electronics'},
            {'product_name': 'Shirt', 'category': 'Clothing'},
        ]

        # 주문 수 생성
        num_orders = 10000  # 실제는 500,000 이상으로 가정

        np.random.seed(0)
        log_data = []

        for i in range(1, num_orders + 1):
            customer = customers[np.random.randint(0, len(customers))]
            product = products[np.random.randint(0, len(products))]
            log_data.append({
                'order_id': i,
                'order_date': pd.Timestamp('2024-01-01') + pd.to_timedelta(np.random.randint(0, 150), unit='D'),
                'customer_name': customer['customer_name'],
                'region': customer['region'],
                'product_name': product['product_name'],
                'category': product['category'],
                'amount': np.random.randint(100, 2000)
            })

        log_df = pd.DataFrame(log_data)
        

        # 저장할 폴더 경로
        
        AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow")

        save_dir = os.path.join(AIRFLOW_HOME, "data/temp")
        
        os.makedirs(save_dir, exist_ok=True)

        # order_date를 문자열(YYYYMMDD)로 변환한 새로운 컬럼 생성
        log_df['order_date_str'] = log_df['order_date'].dt.strftime("%Y%m%d")

        # 날짜별로 그룹 나누기
        grouped = log_df.groupby('order_date_str')

        # 각 그룹을 개별 CSV 파일로 저장
        for date_str, group in grouped:
            file_path = f"{save_dir}/order_log_{date_str}.csv"
            group.drop(columns='order_date_str').to_csv(file_path, index=False)
            print(f"✅ 저장 완료: {file_path}")
    
    def get_data(self):
        print("bigquerfetcher")
