import pandas as pd
from azure.cosmos import CosmosClient
from azure.storage.blob import BlobServiceClient, ContentSettings
from io import StringIO

# === 1. Cosmos DB 연결 ===
COSMOS_ENDPOINT = "https://seoul-data-db.documents.azure.com:443"  # 예: "https://your-account.documents.azure.com:443/"
COSMOS_KEY = "gdgCLQrX8omjZKrDkLRCyo41URDljVi7K8rdHzTUcUpRLg2k1BR8th6CmyKtUG3XS0wLB2hwe49oACDbxniaXQ=="
DATABASE_NAME = "seoul-data-db"
CONTAINER_NAME = "seoul-data-container"

client = CosmosClient(COSMOS_ENDPOINT, COSMOS_KEY)
db = client.get_database_client(DATABASE_NAME)
container = db.get_container_client(CONTAINER_NAME)

# === 2. 최대 1000개 행 조회 ===
query = "SELECT TOP 1000 * FROM c"
items = list(container.query_items(query=query, enable_cross_partition_query=True))
df_raw = pd.DataFrame(items)

# === 3. 50개 컬럼 정의 및 누락 채움 ===
columns = [
    "PKLT_CD", "PKLT_NM", "ADDR", "PKLT_TYPE", "PRK_TYPE_NM", "OPER_SE", "OPER_SE_NM", "TELNO",
    "PRK_STTS_YN", "PRK_STTS_NM", "TPKCT", "NOW_PRK_VHCL_CNT", "NOW_PRK_VHCL_UPDT_TM", "PAY_YN",
    "PAY_YN_NM", "NGHT_PAY_YN", "NGHT_PAY_YN_NM", "WD_OPER_BGNG_TM", "WD_OPER_END_TM",
    "WE_OPER_BGNG_TM", "WE_OPER_END_TM", "LHLDY_OPER_BGNG_TM", "LHLDY_OPER_END_TM",
    "SAT_CHGD_FREE_SE", "SAT_CHGD_FREE_NM", "LHLDY_CHGD_FREE_SE", "LHLDY_CHGD_FREE_SE_NAME",
    "PRD_AMT", "STRT_PKLT_MNG_NO", "BSC_PRK_CRG", "BSC_PRK_HR", "ADD_PRK_CRG", "ADD_PRK_HR",
    "BUS_BSC_PRK_CRG", "BUS_BSC_PRK_HR", "BUS_ADD_PRK_HR", "BUS_ADD_PRK_CRG", "DAY_MAX_CRG",
    "SHRN_PKLT_MNG_NM", "SHRN_PKLT_MNG_URL", "SHRN_PKLT_YN", "SHRN_PKLT_ETC"
]

for col in columns:
    if col not in df_raw.columns:
        df_raw[col] = None

df_final = df_raw[columns]

# === 4. CSV 파일 메모리에 생성 ===
csv_buffer = StringIO()
df_final.to_csv(csv_buffer, index=False, encoding="utf-8")

# === 5. Azure Blob Storage로 덮어쓰기 ===
BLOB_CONNECTION_STRING = "DefaultEndpointsProtocol=https;AccountName=cosmo2csv;AccountKey=jkUvA9iMfrJ8JHNSuuOs21uFYfM7g2Gu7D/CubSAEE6bknEtqp7x8woG3XGZSqviuth3t14oPvpk+AStKqz1qg==;EndpointSuffix=core.windows.net"
BLOB_CONTAINER_NAME = "parkingcsv"
BLOB_FILENAME = "parking_data_from_vm.csv"

blob_service = BlobServiceClient.from_connection_string(BLOB_CONNECTION_STRING)
blob_client = blob_service.get_blob_client(container=BLOB_CONTAINER_NAME, blob=BLOB_FILENAME)

blob_client.upload_blob(csv_buffer.getvalue(), overwrite=True,
                        content_settings=ContentSettings(content_type="text/csv"))

print(f"업로드 완료: {BLOB_FILENAME} (1000개 행)")