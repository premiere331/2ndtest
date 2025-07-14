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
    for item in item_pager:
        json_array_string = item.get('PRK_STTS')
        if json_array_string and isinstance(json_array_string, str):
            try:
                data_list = json.loads(json_array_string)
                all_parking_data.extend(data_list)
            except json.JSONDecodeError:
                print(f"경고: ID {item.get('id')}의 PRK_STTS 컬럼이 올바른 JSON 형식이 아닙니다.")
    
    if not all_parking_data:
        print("파싱 후 처리할 주차장 데이터가 없습니다. 스크립트를 종료합니다.")
        return
        
    df = pd.DataFrame(all_parking_data)
    print("Pandas DataFrame으로 변환 완료.")

    # =================================================================
    # === 4. 컬럼 이름 변경 및 최종 컬럼 선택/정렬 (핵심 요구사항 반영) ===
    # =================================================================
    print("실제 컬럼 이름을 원하시는 최종 필드 이름으로 변경하고, 최종 컬럼셋을 정의합니다...")

    # 실제 이름 -> 원하는 이름 매핑 정의
    column_mapping = {
        'PRK_CD': 'PKLT_CD',
        'PRK_NM': 'PKLT_NM',
        'ADDRESS': 'ADDR',
        'PRK_TYPE': 'PKLT_TYPE',
        'CPCTY': 'TPKCT',
        'CUR_PRK_CNT': 'NOW_PRK_VHCL_CNT',
        'CUR_PRK_TIME': 'NOW_PRK_VHCL_UPDT_TM'
    }
    
    # 컬럼 이름 변경
    df.rename(columns=column_mapping, inplace=True)

    # 원하는 최종 컬럼 목록 (제공해주신 JSON 순서 기반)
    desired_columns_in_order = [
        "PKLT_CD", "PKLT_NM", "ADDR", "PKLT_TYPE", "PRK_TYPE_NM", "OPER_SE",
        "OPER_SE_NM", "TELNO", "PRK_STTS_YN", "PRK_STTS_NM", "TPKCT",
        "NOW_PRK_VHCL_CNT", "NOW_PRK_VHCL_UPDT_TM", "PAY_YN", "PAY_YN_NM",
        "NGHT_PAY_YN", "NGHT_PAY_YN_NM", "WD_OPER_BGNG_TM", "WD_OPER_END_TM",
        "WE_OPER_BGNG_TM", "WE_OPER_END_TM", "LHLDY_OPER_BGNG_TM",
        "LHLDY_OPER_END_TM", "SAT_CHGD_FREE_SE", "SAT_CHGD_FREE_NM",
        "LHLDY_CHGD_FREE_SE", "LHLDY_CHGD_FREE_SE_NAME", "PRD_AMT",
        "STRT_PKLT_MNG_NO", "BSC_PRK_CRG", "BSC_PRK_HR", "ADD_PRK_CRG",
        "ADD_PRK_HR", "BUS_BSC_PRK_CRG", "BUS_BSC_PRK_HR", "BUS_ADD_PRK_HR",
        "BUS_ADD_PRK_CRG", "DAY_MAX_CRG", "SHRN_PKLT_MNG_NM",
        "SHRN_PKLT_MNG_URL", "SHRN_PKLT_YN", "SHRN_PKLT_ETC"
    ]

    # df에 존재하지 않는 컬럼이 desired_columns_in_order에 있을 경우를 대비하여,
    # 실제 존재하는 컬럼만으로 최종 목록을 다시 필터링합니다. (오류 방지)
    final_columns = [col for col in desired_columns_in_order if col in df.columns]
    
    # 최종 컬럼만 선택하고 순서를 맞춥니다.
    df = df[final_columns]
    print("컬럼 이름 변경 및 최종 컬럼 선택/정렬 완료.")
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
