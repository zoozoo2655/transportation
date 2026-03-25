import requests
import pandas as pd
import time

# 1. 設定認證資訊 (請替換成你的 ID 與 Secret)
CLIENT_ID = '你的_CLIENT_ID'
CLIENT_SECRET = '你的_CLIENT_SECRET'

class TDX_ETL:
    def __init__(self, client_id, client_secret):
        self.client_id = client_id
        self.client_secret = client_secret
        self.token_url = "https://tdx.transportdata.tw/auth/realms/TDXConnect/protocol/openid-connect/token"
        self.access_token = self.get_token()

    def get_token(self):
        """取得 OAuth2 Access Token"""
        data = {
            'content-type': 'application/x-www-form-urlencoded',
            'grant_type': 'client_credentials',
            'client_id': self.client_id,
            'client_secret': self.client_secret
        }
        response = requests.post(self.token_url, data=data)
        if response.status_code == 200:
            return response.json().get('access_token')
        else:
            raise Exception(f"Token 取得失敗: {response.text}")

    def fetch_data(self, api_url):
        """抓取 API 資料"""
        headers = {
            'authorization': f'Bearer {self.access_token}',
            'Accept-Encoding': 'gzip'
        }
        response = requests.get(api_url, headers=headers)
        if response.status_code == 200:
            return response.json()
        else:
            print(f"資料抓取失敗碼: {response.status_code}")
            return None

# 2. 執行 ETL 流程
etl = TDX_ETL(CLIENT_ID, CLIENT_SECRET)

# 以「歷史交通事故」為例 (請根據 TDX 最新 API 路徑調整)
# 註：TDX 的事故資料通常在 OData 格式下，可使用 $filter 進行過濾
target_url = "https://tdx.transportdata.tw/api/basic/v2/Road/TrafficAbnormality/Taiwan?$format=JSON"

raw_data = etl.fetch_data(target_url)

if raw_data:
    # 3. Transform (資料轉換)
    df = pd.DataFrame(raw_data)
    
    # 範例：處理時間欄位與分類
    # 假設欄位包含發生時間與地點
    # df['OccurDate'] = pd.to_datetime(df['OccurTime'])
    # df['Hour'] = df['OccurDate'].dt.hour
    
    # 範例：針對你的專題標籤化 (例如簡單判斷道路型態)
    # df['Road_Type'] = df['Location'].apply(lambda x: '交叉路口' if '路口' in str(x) else '一般路段')

    # 4. Load (導出給 Power BI)
    df.to_csv('cleaned_traffic_data.csv', index=False, encoding='utf-8-sig')
    print("ETL 完成！已產生 cleaned_traffic_data.csv")