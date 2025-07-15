import os
import json
import pandas as pd
from azure.cosmos import CosmosClient
from azure.storage.blob import BlobServiceClient
from datetime import datetime, timedelta, timezone
import argparse

# --- 1. Azure 리소스 연결 정보 설정 ---
COSMOS_ENDPOINT = "https://seoul-data-db.documents.azure.com:443"
COSMOS_KEY = "gdgCLQrX8omjZKrDkLRCyo41URDljVi7K8rdHzTUcUpRLg2k1BR8th6CmyKtUG3XS0wLB2hwe49oACDbxniaXQ=="
DATABASE_NAME = "seoul-data-db"
CONTAINER_NAME = "seoul-data-container"

BLOB_CONNECTION_STRING = "DefaultEndpointsProtocol=https;AccountName=cosmo2csv;AccountKey=jkUvA9iMfrJ8JHNSuuOs21uFYfM7g2Gu7D/CubSAEE6bknEtqp7x8woG3XGZSqviuth3t14oPvpk+AStKqz1qg==;EndpointSuffix=core.windows.net"
BLOB_CONTAINER_NAME = "parkingcsv" # 기존 컨테이너 이름으로 변경
OUTPUT_FILENAME = "parking_data.csv" # 저장될 CSV 파일 이름 (고정)

def main():
    # --- 스크립트 인자 파싱 (날짜 지정 기능) ---
    parser = argparse.ArgumentParser(description="Cosmos DB에서 특정 날짜의 주차 데이터를 가져와 Blob Storage에 저장합니다.")
    parser.add_argument('--date', type=str, help="'YYYY-MM-DD' 형식의 처리할 날짜. 지정하지 않으면 어제 날짜를 처리합니다.")
    args = parser.parse_args()

    # --- 2. 처리할 날짜 결정 ---
    if args.date:
        try:
            # 날짜 인자가 있으면 해당 날짜를 사용
            target_date = datetime.strptime(args.date, '%Y-%m-%d').replace(tzinfo=timezone.utc)
        except ValueError:
            print("오류: 날짜 형식이 잘못되었습니다. 'YYYY-MM-DD' 형식으로 입력해주세요.")
            return
    else:
        # 날짜 인자가 없으면 '어제'를 기본값으로 사용 (cron 작업용)
        target_date = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=1)

    # 해당 날짜의 시작(00:00:00)과 끝(23:59:59) 타임스탬프 계산
    start_of_day_utc = target_date
    end_of_day_utc = start_of_day_utc + timedelta(days=1) - timedelta(microseconds=1)

    start_timestamp = int(start_of_day_utc.timestamp())
    end_timestamp = int(end_of_day_utc.timestamp())
    
    processing_date_str = start_of_day_utc.strftime('%Y-%m-%d')
    print(f"스크립트 실행 시작: '{processing_date_str}' 날짜의 데이터를 처리합니다.")
    print(f"조회 시간 범위 (UTC): {start_of_day_utc.isoformat()} 부터 {end_of_day_utc.isoformat()} 까지")
    print(f"타임스탬프 범위: {start_timestamp} 부터 {end_timestamp} 까지")

    # --- 3. Cosmos DB에서 지정된 날짜의 데이터 가져오기 ---
    client = CosmosClient(COSMOS_ENDPOINT, credential=COSMOS_KEY)
    database = client.get_database_client(DATABASE_NAME)
    container = database.get_container_client(CONTAINER_NAME)

    query = f"SELECT * FROM c WHERE IS_DEFINED(c.PRK_STTS) AND c._ts >= {start_timestamp} AND c._ts <= {end_timestamp}"
    print(f"Cosmos DB 쿼리 실행: {query}")
    
    item_pager = container.query_items(query=query, enable_cross_partition_query=True)

    # --- 4. 데이터 파싱 및 펼치기 ---
    print("데이터 파싱 및 펼치기 작업 시작...")
    all_parking_data = []
    item_count = 0
    for item in item_pager:
        item_count += 1
        json_array_string = item.get('PRK_STTS')
        if json_array_string and isinstance(json_array_string, str):
            try:
                data_list = json.loads(json_array_string)
                all_parking_data.extend(data_list)
            except json.JSONDecodeError:
                print(f"경고: ID {item.get('id')}의 PRK_STTS 컬럼이 올바른 JSON 형식이 아닙니다.")
    
    if not all_parking_data:
        print(f"'{processing_date_str}'에 처리할 주차장 데이터가 없습니다. 스크립트를 종료합니다.")
        return
    
    print(f"총 {item_count}개의 Cosmos DB 문서를 처리하여 {len(all_parking_data)}개의 주차장 데이터를 추출했습니다.")
        
    df = pd.DataFrame(all_parking_data)
    print("Pandas DataFrame으로 변환 완료.")

    # --- 5. 날짜별 경로로 Blob Storage에 CSV 파일 업로드 ---
    blob_path = f"{start_of_day_utc.strftime('%Y/%m/%d')}/{OUTPUT_FILENAME}"
    
    print(f"'{BLOB_CONTAINER_NAME}/{blob_path}' 경로로 Blob Storage에 업로드 중...")
    blob_service_client = BlobServiceClient.from_connection_string(BLOB_CONNECTION_STRING)
    blob_client = blob_service_client.get_blob_client(container=BLOB_CONTAINER_NAME, blob=blob_path)

    output_csv = df.to_csv(index=False, encoding='utf-8-sig')
    blob_client.upload_blob(output_csv, overwrite=True)
    print("업로드 성공!")

    print("스크립트 실행 완료.")

if __name__ == '__main__':
    main()

