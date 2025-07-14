import os
import json
import pandas as pd
from azure.cosmos import CosmosClient
from azure.storage.blob import BlobServiceClient

# --- 1. Azure 리소스 연결 정보 설정 ---
COSMOS_ENDPOINT = "YOUR_COSMOS_DB_ENDPOINT"
COSMOS_KEY = "YOUR_COSMOS_DB_PRIMARY_KEY"
DATABASE_NAME = "seoul-data-db"
CONTAINER_NAME = "seoul-data-container"

BLOB_CONNECTION_STRING = "YOUR_BLOB_STORAGE_CONNECTION_STRING"
BLOB_CONTAINER_NAME = "parkingcsv"
OUTPUT_FILENAME = "parking_data_from_vm.csv"

def main():
    print("스크립트 실행 시작...")

    # --- 2. Cosmos DB에서 데이터 가져오기 ---
    print(f"Cosmos DB '{DATABASE_NAME}/{CONTAINER_NAME}'에 연결 중...")
    client = CosmosClient(COSMOS_ENDPOINT, credential=COSMOS_KEY)
    database = client.get_database_client(DATABASE_NAME)
    container = database.get_container_client(CONTAINER_NAME)

    print("데이터 쿼리 실행 (최근 1000개 문서만 가져오도록 제한)...")
    query = "SELECT * FROM c ORDER BY c._ts DESC OFFSET 0 LIMIT 1000"
    item_pager = container.query_items(query=query, enable_cross_partition_query=True)

    # --- 3. 데이터 파싱 및 펼치기 ---
    print("데이터 파싱 및 펼치기 작업 시작...")
    all_parking_data = []
    processed_item_count = 0
    for item in item_pager:
        processed_item_count += 1
        # 'PRK_STTS'가 아닌 실제 배열 컬럼 이름으로 수정해야 할 수 있습니다.
        json_array_string = item.get('PRK_STTS') 
        if json_array_string and isinstance(json_array_string, str):
            try:
                data_list = json.loads(json_array_string)
                all_parking_data.extend(data_list)
            except json.JSONDecodeError:
                print(f"경고: ID {item.get('id')}의 컬럼이 올바른 JSON 형식이 아닙니다.")
    
    print(f"총 {processed_item_count}개의 문서를 처리했습니다.")

    if not all_parking_data:
        print("파싱 후 처리할 주차장 데이터가 없습니다. 스크립트를 종료합니다.")
        return
        
    print(f"총 {len(all_parking_data)}개의 주차장 데이터로 펼쳤습니다.")

    # --- 4. Pandas DataFrame으로 변환 ---
    # 이 단계에서 원본 필드 이름(PKLT_NM, TPKCT 등)이 그대로 컬럼이 됩니다.
    df = pd.DataFrame(all_parking_data)
    print("Pandas DataFrame으로 변환 완료. 원본 필드 이름이 유지됩니다.")

    # --- 5. '주차_점유율' 컬럼 추가 (원본 필드 이�� 사용) ---
    # 숫자형으로 변환 (오류 발생 시 숫자가 아닌 값으로 처리)
    # to_numeric을 사용하여 안전하게 숫자 타입으로 변경합니다.
    df['TPKCT_NUM'] = pd.to_numeric(df['TPKCT'], errors='coerce')
    df['NOW_PRK_VHCL_CNT_NUM'] = pd.to_numeric(df['NOW_PRK_VHCL_CNT'], errors='coerce')

    # '주차_점유율'이라는 새 컬럼을 추가합니다.
    df['주차_점유율'] = 0.0
    # 유효한 숫자일 경우에만 계산을 수행합니다.
    mask = (df['TPKCT_NUM'] > 0) & (df['TPKCT_NUM'].notna()) & (df['NOW_PRK_VHCL_CNT_NUM'].notna())
    df.loc[mask, '주차_점유율'] = round((df['NOW_PRK_VHCL_CNT_NUM'] / df['TPKCT_NUM']) * 100, 2)
    
    # 계산에 사용된 임시 숫자 컬럼은 삭제합니다.
    df = df.drop(columns=['TPKCT_NUM', 'NOW_PRK_VHCL_CNT_NUM'])
    print("'주차_점유율' 컬럼 추가 완료.")

    # --- 6. Blob Storage에 CSV 파일로 업로드 ---
    print(f"'{BLOB_CONTAINER_NAME}/{OUTPUT_FILENAME}' 파일로 Blob Storage에 업로드 중...")
    blob_service_client = BlobServiceClient.from_connection_string(BLOB_CONNECTION_STRING)
    blob_client = blob_service_client.get_blob_client(container=BLOB_CONTAINER_NAME, blob=OUTPUT_FILENAME)

    output_csv = df.to_csv(index=False, encoding='utf-8-sig')
    blob_client.upload_blob(output_csv, overwrite=True)
    print("업로드 성공!")

    print("스크립트 실행 완료.")

if __name__ == '__main__':
    main()
