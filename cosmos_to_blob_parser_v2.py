import os
import json
import pandas as pd
from azure.cosmos import CosmosClient
from azure.storage.blob import BlobServiceClient

# --- 1. Azure 리소스 연결 정보 설정 ---
COSMOS_ENDPOINT = "https://seoul-data-db.documents.azure.com:443"  # 예: "https://your-account.documents.azure.com:443/"
COSMOS_KEY = "gdgCLQrX8omjZKrDkLRCyo41URDljVi7K8rdHzTUcUpRLg2k1BR8th6CmyKtUG3XS0wLB2hwe49oACDbxniaXQ=="
DATABASE_NAME = "seoul-data-db"
CONTAINER_NAME = "seoul-data-container"

BLOB_CONNECTION_STRING = "DefaultEndpointsProtocol=https;AccountName=cosmo2csv;AccountKey=jkUvA9iMfrJ8JHNSuuOs21uFYfM7g2Gu7D/CubSAEE6bknEtqp7x8woG3XGZSqviuth3t14oPvpk+AStKqz1qg==;EndpointSuffix=core.windows.net"
BLOB_CONTAINER_NAME = "parkingcsv" # 데이터를 저장할 Blob 컨테이너 이름
OUTPUT_FILENAME = "parking_data_from_vm.csv" # 저장될 CSV 파일 이름


def main():
    print("스크립트 실행 시작 (AS-IS 버전)...")

    # --- 2. Cosmos DB에서 데이터 가져오기 ---
    print(f"Cosmos DB '{DATABASE_NAME}/{CONTAINER_NAME}'에 연결 중...")
    client = CosmosClient(COSMOS_ENDPOINT, credential=COSMOS_KEY)
    database = client.get_database_client(DATABASE_NAME)
    container = database.get_container_client(CONTAINER_NAME)

    print("데이터 쿼리 실행 ('PRK_STTS' 필드가 있는 최근 1000개 문서만 선택)...")
    query = "SELECT * FROM c WHERE IS_DEFINED(c.PRK_STTS) ORDER BY c._ts DESC OFFSET 0 LIMIT 1000"
    item_pager = container.query_items(query=query, enable_cross_partition_query=True)

    # --- 3. 데이터 파싱 및 펼치기 ---
    print("데이터 파싱 및 펼치기 작업 시작...")
    all_parking_data = []
    for item in item_pager:
        json_array_string = item.get('PRK_STTS')
        if json_array_string and isinstance(json_array_string, str):
            try:
                data_list = json.loads(json_array_string)
                all_parking_data.extend(data_list)
            except json.JSONDecodeError:
                print(f"경고: ID {item.get('id')}의 PRK_STTS 컬럼이 올바른 JSON 형식이 아닙니다.")
    
    if not all_parking_data:
        print("처리할 주차장 데이터가 없습니다. 스크립트를 종료합니다.")
        return
        
    # =================================================================
    # === 4. Pandas DataFrame으로 변환 (AS-IS) ===
    # =================================================================
    # 어떠한 컬럼 이름 변경이나 선택도 하지 않고, 있는 그대로 DataFrame을 생성합니다.
    # 최종 CSV의 컬럼은 실제 데이터의 키와 순서를 그대로 따릅니다.
    df = pd.DataFrame(all_parking_data)
    print("Pandas DataFrame으로 변환 완료. 실제 데이터의 스키마를 그대로 유지합니다.")
    print("생성된 컬럼 목록:")
    print(df.columns)
    # =================================================================

    # --- 5. Blob Storage에 CSV 파일로 업로드 ---
    print(f"'{BLOB_CONTAINER_NAME}/{OUTPUT_FILENAME}' 파일로 Blob Storage에 업로드 중...")
    blob_service_client = BlobServiceClient.from_connection_string(BLOB_CONNECTION_STRING)
    blob_client = blob_service_client.get_blob_client(container=BLOB_CONTAINER_NAME, blob=OUTPUT_FILENAME)

    output_csv = df.to_csv(index=False, encoding='utf-8-sig')
    blob_client.upload_blob(output_csv, overwrite=True)
    print("업로드 성공!")

    print("스크립트 실행 완료.")

if __name__ == '__main__':
    main()
