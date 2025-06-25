import pandas as pd
import os

class OlapModeling:

    def load(self, date, log_read_path, log_save_path):

        log_df = pd.read_csv(log_read_path)
        print(log_df)
        
        # timestamp → datetime 변환
        log_df['event_timestamp'] = pd.to_datetime(log_df['timestamp'])

        # date, time 컬럼 생성
        log_df['date'] = log_df['event_timestamp'].dt.date.astype(str)
        log_df['time'] = log_df['event_timestamp'].dt.time.astype(str)
        
        os.makedirs(log_save_path, exist_ok=True)
        log_df.to_csv(f"{log_save_path}/{date}")

        print(log_df)
        return log_df

    def update_dimension_table(self, log_df_path, dim_file_path, keys, id_column_name):
        log_df = pd.read_csv(log_df_path,index_col=0)
        
        new_entries = log_df[keys].drop_duplicates()
    
        if os.path.exists(dim_file_path):
            dim_df = pd.read_csv(dim_file_path)
        else:
            dim_df = pd.DataFrame(columns=[id_column_name] + keys)
    
        # update할 dim 간추리기
        merged = new_entries.merge(dim_df, on=keys, how='left', indicator=True)
        to_add = merged[merged['_merge'] == 'left_only'][keys]
        start_id = dim_df[id_column_name].max() + 1 if not dim_df.empty else 1
        to_add[id_column_name] = range(start_id, start_id + len(to_add))

        updated_dim = pd.concat([dim_df, to_add], ignore_index=True)
        print(updated_dim)
    
        # 저장
        os.makedirs(os.path.dirname(dim_file_path), exist_ok=True)
        updated_dim.to_csv(dim_file_path, index=False)
    
        return updated_dim
    
    def fact(self, log_df_path, dim_member_path, dim_event_type_path, dim_date_path, dim_time_path, dim_study_path, fact_file_path):
    
        log_df = pd.read_csv(log_df_path,index_col=0)
        print(f"log_df :{log_df}")
        #dim_event_type 테이블 가지고 오기
        dim_event_type_df = pd.read_csv(dim_event_type_path) if os.path.exists(dim_event_type_path) else \
            pd.DataFrame(columns=['event_type_id', 'event'])
        dim_member_df = pd.read_csv(dim_member_path) if os.path.exists(dim_member_path) else \
            pd.DataFrame(columns=['member_id', 'dl_member_id'])
        dim_study_df = pd.read_csv(dim_study_path) if os.path.exists(dim_study_path) else \
            pd.DataFrame(columns=['study_id', 'dl_study_id'])
        dim_date_df = pd.read_csv(dim_date_path) if os.path.exists(dim_date_path) else \
            pd.DataFrame(columns=['date_id', 'date'])
        dim_time_df = pd.read_csv(dim_time_path) if os.path.exists(dim_time_path) else \
            pd.DataFrame(columns=['time_id', 'time'])

        # log_df에 ID 매핑
        log_with_ids = log_df \
            .merge(dim_event_type_df, on='event', how='left') \
            .merge(dim_member_df, on='dl_member_id', how='left') \
            .merge(dim_study_df, on='dl_study_id', how='left') \
            .merge(dim_date_df, on='date', how='left') \
            .merge(dim_time_df, on='time', how='left')
        
        new_fact_event_df = log_with_ids[[
            'member_id',
            'event_type_id',
            'date_id',
            'time_id',
            'study_id',
            'event_timestamp'
        ]].copy()

        new_fact_event_df = new_fact_event_df[[
            'member_id',
            'event_type_id',
            'date_id',
            'time_id',
            'study_id',
            'event_timestamp'
        ]]
        print(new_fact_event_df)
        
        # datetime 변환
        new_fact_event_df['event_timestamp'] = pd.to_datetime(new_fact_event_df['event_timestamp'])
        new_fact_event_df['date'] = new_fact_event_df['event_timestamp'].dt.strftime('%Y%m%d')

        # 정렬 및 row_number 부여
        new_fact_event_df = new_fact_event_df.sort_values(by=['event_timestamp'])
        new_fact_event_df['row_number'] = range(1, len(new_fact_event_df) + 1)
            
        # fact_event_id 생성
        new_fact_event_df['fact_event_id'] = new_fact_event_df['date'] + '_' + new_fact_event_df['row_number'].astype(str).str.zfill(4)

        # 임시 컬럼 제거
        new_fact_event_df = new_fact_event_df.drop(columns=['date', 'row_number'])

        os.makedirs(os.path.dirname(fact_file_path), exist_ok=True)
        new_fact_event_df.to_csv(f"{fact_file_path}.csv", index=False)
        print(f"new_fact_event_df : {new_fact_event_df}") 
        return new_fact_event_df
    
    def send_slack_alert(self):
        print("send_slack")
