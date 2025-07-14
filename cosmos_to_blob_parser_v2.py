import os
import json
import pandas as pd
from azure.cosmos import CosmosClient

# --- 1. Azure 리소스 연결 정보 설정 ---
# (이 부분은 기존과 동일하게 올바른 값으로 유지)
COSMOS_ENDPOINT = "YOUR_COSMOS_DB_ENDPOINT"
COSMOS_KEY = "YOUR_COSMOS_DB_PRIMARY_KEY"
DATABASE_NAME = "seoul-data-db"
CONTAINER_NAME = "seoul-data-container"

BLOB_CONNECTION_STRING = "YOUR_BLOB_STORAGE_CONNECTION_STRING"
BLOB_CONTAINER_NAME = "parkingcsv"
OUTPUT_FILENAME = "parking_data_from_vm.csv" # 저장될 CSV 파일 이름

def main():
    print("스크립트 실행 시작...")

    # --- 2. Cosmos DB에서 데이터 가져오기 ---
    print(f"Cosmos DB '{DATABASE_NAME}/{CONTAINER_NAME}'에 연결 중...")
    client = CosmosClient(COSMOS_ENDPOINT, credential=COSMOS_KEY)
    database = client.get_database_client(DATABASE_NAME)
    container = database.get_container_client(CONTAINER_NAME)

    # =================================================================
    # === 로직 수정: 쿼리를 최적화하고, 결과를 스트리밍 방식으로 처리 ===
    # =================================================================
    print("데이터 쿼리 실행 (최근 1000개 문서만 가져오도록 제한)...")
    # 쿼리: 모든 문서 대신, 시스템 타임스탬프(_ts) 기준으로 내림차순 정렬 후 상위 1000개만 선택
    query = "SELECT * FROM c ORDER BY c._ts DESC OFFSET 0 LIMIT 1000"
    
    # 결과를 list()로 한 번에 감싸지 않고, item_pager를 그대로 사용 (스트리밍)
    item_pager = container.query_items(
        query=query,
        enable_cross_partition_query=True
    )

    # --- 3. 데이터 파싱 및 펼치기 (스트리밍 방식) ---
    print("데이터 파싱 및 펼치기 작업 시작...")
    all_parking_data = []
    processed_item_count = 0
    
    # item_pager를 for 루프로 돌면서 하나씩 처리
    for item in item_pager:
        processed_item_count += 1
        json_array_string = item.get('PRK_STTS')
        if json_array_string and isinstance(json_array_string, str):
            try:
                data_list = json.loads(json_array_string)
                all_parking_data.extend(data_list)
            except json.JSONDecodeError:
                print(f"경고: ID {item.get('id')}의 PRK_STTS 컬럼이 올바른 JSON 형식이 아닙니다.")
    
    print(f"총 {processed_item_count}개의 문서를 처리했습니다.")
    # =================================================================

    if not all_parking_data:
        print("파싱 후 처리할 주차장 데이터가 없습니다. 스크립트를 종료합니다.")
        return
        
    print(f"총 {len(all_parking_data)}개의 주차장 데이터로 펼쳤습니다.")

    # --- 4. Pandas DataFrame으로 변환 및 데이터 처리 ---
    # (이하 로직은 기존과 동일)
    df = pd.DataFrame(all_parking_data)
    print("Pandas DataFrame으로 변환 완료.")

    df['CPCTY'] = pd.to_numeric(df['CPCTY'], errors='coerce')
    df['CUR_PRK_CNT'] = pd.to_numeric(df['CUR_PRK_CNT'], errors='coerce')

    df['주차_점유율'] = 0.0
    mask = (df['CPCTY'] > 0) & (df['CPCTY'].notna()) & (df['CUR_PRK_CNT'].notna())
    df.loc[mask, '주차_점유율'] = round((df['CUR_PRK_CNT'] / df['CPCTY']) * 100, 2)
    print("주차 점유율 계산 완료.")

    # --- 5. Blob Storage에 CSV 파일로 업로드 ---
    # (이하 로직은 기존과 동일)
    print(f"'{BLOB_CONTAINER_NAME}/{OUTPUT_FILENAME}' 파일로 Blob Storage에 업로드 중...")
    # azure-storage-blob 라이브러리가 설치되어 있어야 합니다.
    from azure.storage.blob import BlobServiceClient
    blob_service_client = BlobServiceClient.from_connection_string(BLOB_CONNECTION_STRING)
    blob_client = blob_service_client.get_blob_client(container=BLOB_CONTAINER_NAME, blob=OUTPUT_FILENAME)

    output_csv = df.to_csv(index=False, encoding='utf-8-sig')
    blob_client.upload_blob(output_csv, overwrite=True)
    print("업로드 성공!")

    print("스크립트 실행 완료.")

if __name__ == '__main__':
    main()
