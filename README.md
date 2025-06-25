### 프로젝트 이름 및 한 줄 설명
```
목적 : 면접 스터디 활동 로그 기반으로 웹 사용률을 측정할 수 있는 운영 대시보드 구축
```
### 레포지토리 역할
```
역할 : Airflow의 Task에 들어가는 모듈로 구성
```
### 구성 요소
```
├── BigQueryDimLoader.py     # dimension 정보 BigQuery에 전송 모듈
├── BigQueryOlapModeling.py  # fact 정보 BigQuery에 전송 모듈
├── EventLogGenerator.py     # test용 event 발생기
├── OlapModeling.py          # ETL에 필요한 모듈 ( load, update_dimension_table, fact )
├── OlapModeling_back.py
├── __init__.py
└── __pycache__
```

### 문제 해결 방식
- ETL 버전 관리 방식 : 브랜치 당 1개의 ETL 버전을 할당하여 특정 버전을 선택할 수 있도록 설계
