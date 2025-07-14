import os
import json
import pandas as pd
from azure.cosmos import CosmosClient
from azure.storage.blob import BlobServiceClient

# --- 1. Azure 리소스 연결 정보 설정 ---
# (실제 값으로 변경하세요. 또는 환경 변수로 설정하는 것을 권장합니다.)
COSMOS_ENDPOINT = "https://seoul-data-db.documents.azure.com:443"  # 예: "https://your-account.documents.azure.com:443/"
COSMOS_KEY = "gdgCLQrX8omjZKrDkLRCyo41URDljVi7K8rdHzTUcUpRLg2k1BR8th6CmyKtUG3XS0wLB2hwe49oACDbxniaXQ=="
DATABASE_NAME = "seoul-data-db"
CONTAINER_NAME = "seoul-data-container"

BLOB_CONNECTION_STRING = "jkUvA9iMfrJ8JHNSuuOs21uFYfM7g2Gu7D/CubSAEE6bknEtqp7x8woG3XGZSqviuth3t14oPvpk+AStKqz1qg=="
BLOB_CONTAINER_NAME = "parkingcsv" # 데이터를 저장할 Blob 컨테이너 이름
OUTPUT_FILENAME = "parking_data_from_vm.csv" # 저장될 CSV 파일 이름

def main():
    print("스크립트 실행 시작...")

    # --- 2. Cosmos DB에서 데이터 가져오기 ---
    print(f"Cosmos DB '{DATABASE_NAME}/{CONTAINER_NAME}'에 연결 중...")
    client = CosmosClient(COSMOS_ENDPOINT, credential=COSMOS_KEY)
    database = client.get_database_client(DATABASE_NAME)
    container = database.get_container_client(CONTAINER_NAME)

    # 모든 문서를 가져옵니다.
    items = list(container.query_items(
        query="SELECT * FROM c",
        enable_cross_partition_query=True
    ))
    print(f"총 {len(items)}개의 문서를 Cosmos DB에서 가져왔습니다.")

    if not items:
        print("처리할 데이터가 없습니다. 스크립트를 종료합니다.")
        return

    # --- 3. 데이터 파싱 및 펼치기 (핵심 로직) ---
    print("데이터 파싱 및 펼치기 작업 시작...")
    all_parking_data = []
    for item in items:
        # 'PRK_STTS' 컬럼에 있는 배열 문자열을 가져옵니다.
        json_array_string = item.get('PRK_STTS')
        if json_array_string and isinstance(json_array_string, str):
            try:
                # 문자열을 Python 리스트(배열)로 변환합니다.
                data_list = json.loads(json_array_string)
                # 변환된 리스트를 전체 데이터 목록에 추가합니다.
                all_parking_data.extend(data_list)
            except json.JSONDecodeError:
                print(f"경고: ID {item.get('id')}의 PRK_STTS 컬럼이 올바른 JSON 형식이 아닙니다.")

    if not all_parking_data:
        print("파싱 후 처리할 주차장 데이터가 없습니다. 스크립트를 종료합니다.")
        return
        
    print(f"총 {len(all_parking_data)}개의 주차장 데이터로 펼쳤��니다.")

    # --- 4. Pandas DataFrame으로 변환 및 데이터 처리 ---
    df = pd.DataFrame(all_parking_data)
    print("Pandas DataFrame으로 변환 완료.")

    # (선택사항) ADF에서 하려던 추가 데이터 처리
    # - 숫자형으로 변환 (오류 발생 시 NaN으로 처리)
    df['CPCTY'] = pd.to_numeric(df['CPCTY'], errors='coerce')
    df['CUR_PRK_CNT'] = pd.to_numeric(df['CUR_PRK_CNT'], errors='coerce')

    # - 주차 점유율 계산
    # (CPCTY가 0보다 크고 NaN이 아닌 경우에만 계산)
    df['주차_점유율'] = 0.0
    mask = (df['CPCTY'] > 0) & (df['CPCTY'].notna()) & (df['CUR_PRK_CNT'].notna())
    df.loc[mask, '주차_점유율'] = round((df['CUR_PRK_CNT'] / df['CPCTY']) * 100, 2)
    print("주차 점유율 계산 완료.")

    # --- 5. Blob Storage에 CSV 파일로 업로드 ---
    print(f"'{BLOB_CONTAINER_NAME}/{OUTPUT_FILENAME}' 파일로 Blob Storage에 업로드 중...")
    blob_service_client = BlobServiceClient.from_connection_string(BLOB_CONNECTION_STRING)
    blob_client = blob_service_client.get_blob_client(container=BLOB_CONTAINER_NAME, blob=OUTPUT_FILENAME)

    # DataFrame을 CSV 형태의 문자열로 변환 (한글 깨짐 방지: utf-8-sig)
    output_csv = df.to_csv(index=False, encoding='utf-8-sig')

    # 업로드 (이미 파일이 있으면 덮어쓰기)
    blob_client.upload_blob(output_csv, overwrite=True)
    print("업로드 성공!")

    print("스크립트 실행 완료.")

if __name__ == '__main__':
    main()
