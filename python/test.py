import pandas as pd
import numpy as np
import random

# 設定隨機種子確保結果可重複
np.random.seed(42)

# 1. 設定參數
num_records = 1000
age_ranges = [18, 25, 45, 65, 85]
vehicles = ['機車', '自用小客車', '大貨車', '電動代步車', '行人']
road_patterns = ['十字路口', '丁字路口', '圓環', '五岔路口', '一般路段(巷弄)', '彎道']

# 2. 生成隨機資料
data = {
    '事故編號': [f'ACC2026{i:04d}' for i in range(num_records)],
    '發生時間': pd.to_datetime('2026-01-01') + pd.to_timedelta(np.random.randint(0, 80, num_records), unit='D'),
    '當事者年齡': np.random.randint(15, 90, num_records),
    '當事者車種': np.random.choice(vehicles, p=[0.5, 0.3, 0.05, 0.05, 0.1], size=num_records),
    '道路型態描述': np.random.choice(road_patterns, num_records),
    '經度': np.random.uniform(120.5, 121.5, num_records),
    '緯度': np.random.uniform(23.5, 25.1, num_records),
    '死亡人數': np.random.choice([0, 1], p=[0.98, 0.02], size=num_records),
    '受傷人數': np.random.randint(0, 3, num_records)
}

df = pd.DataFrame(data)

# 3. 進行初步 ETL (在 Python 裡先分好組)
def age_grouping(age):
    if age < 24: return '青少年(18-24)'
    elif age < 65: return '中壯年(25-64)'
    else: return '高齡者(65+)'

df['年齡層'] = df['當事者年齡'].apply(age_grouping)

# 4. 匯出 CSV
df.to_csv('mock_traffic_data.csv', index=False, encoding='utf-8-sig')
print("✅ 模擬資料已生成：mock_traffic_data.csv")