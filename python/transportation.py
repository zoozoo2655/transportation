import pandas as pd
import os
# 1. 指定檔案路徑 (請確保檔案在你的 Python 執行資料夾內，或者輸入絕對路徑)
file_path = r"C:\transportation\transportation\python\112年傷亡道路交通事故資料\112年度A1交通事故資料.csv" 

# 2.初始化 df 為 None，避免後續呼叫報錯
df = None

def smart_read_csv(path):
    # 嘗試順序：1. UTF-8 (含BOM) -> 2. Big5 (繁體中文) -> 3. cp950 (Windows 繁中)
    encodings = ['utf-8-sig', 'big5', 'cp950']
    
    for enc in encodings:
        try:
            df = pd.read_csv(path, encoding=enc)
            print(f"✅ 成功！使用編碼：{enc}")
            return df
        except UnicodeDecodeError:
            print(f"❌ {enc} 編碼讀取失敗，嘗試下一個...")
        except Exception as e:
            print(f"❌ 發生其他錯誤：{e}")
            break
    return None

# 3.執行讀取
df = smart_read_csv(file_path)

if df is not None:
    print("--- 成功讀取前 5 筆資料 ---")
    print(df.head())
    print("\n--- 實際欄位名稱 (請複製這些名稱到你的字典裡) ---")
    print(df.columns.tolist())
# 4. 定義欄位對應表 (根據你印出的結果進行精確對應)
mapping = {
    '發生年度': 'Year',
    '發生月份': 'Month',
    '發生日期': 'Date',
    '發生時間': 'Time',
    '發生地點': 'Location',
    '道路型態大類別名稱': 'Road_Type_Main',
    '道路型態子類別名稱': 'Road_Type_Sub',
    '當事者區分-類別-大類別名稱-車種': 'Vehicle_Type',
    '當事者事故發生時年齡': 'Age',
    '死亡受傷人數': 'Casualties',
    '經度': 'Longitude',
    '緯度': 'Latitude'
}

# 5. 篩選欄位並改名 (使用 try-except 避免欄位名稱微小差異導致失敗)
df_bi = df[list(mapping.keys())].rename(columns=mapping)

# 6. 資料清洗與特徵工程
# (A) 處理年齡：轉換為數字並分組
df_bi['Age'] = pd.to_numeric(df_bi['Age'], errors='coerce') # 處理非數字內容

def age_group(age):
    if pd.isna(age): return '不詳'
    if age < 18: return '1.未成年(<18)'
    elif age <= 24: return '2.青少年(18-24)'
    elif age <= 64: return '3.青壯年(25-64)'
    else: return '4.高齡者(65+)'

df_bi['Age_Group'] = df_bi['Age'].apply(age_group)

# (B) 道路型態簡化 (針對你的專題重點)
def simplify_road(road):
    road = str(road)
    if '交叉路口' in road or '叉路' in road: return '平面交叉路口'
    if '單路' in road: return '單路路段'
    if '圓環' in road: return '圓環/廣場'
    return '其他型態'

df_bi['Road_Pattern'] = df_bi['Road_Type_Main'].apply(simplify_road)

# (C) 座標清理：排除座標為 0 的資料
df_bi = df_bi[(df_bi['Longitude'] > 0) & (df_bi['Latitude'] > 0)]
# 假設 df 已經是讀取成功的 DataFrame
if 'df' in locals() and df is not None:
    # 1. 精選核心欄位 (對應你的專題：年齡、車種、道路型態、經緯度)
    mapping = {
        '發生日期': 'Date',
        '發生時間': 'Time',
        '道路型態大類別名稱': 'Road_Type_Main',
        '當事者區分-類別-大類別名稱-車種': 'Vehicle_Type',
        '當事者事故發生時年齡': 'Age',
        '肇因研判大類別名稱-主要': 'Primary_Cause',
        '經度': 'Longitude',
        '緯度': 'Latitude'
    }
    
    # 執行篩選與改名
    df_final = df[list(mapping.keys())].rename(columns=mapping).copy()

    # 2. 特徵工程 (A)：年齡層分類
    df_final['Age'] = pd.to_numeric(df_final['Age'], errors='coerce')
    def categorize_age(age):
        if pd.isna(age): return '不詳'
        if age < 18: return '1.未成年(<18)'
        elif age <= 24: return '2.青少年(18-24)'
        elif age <= 64: return '3.青壯年(25-64)'
        else: return '4.高齡者(65+)'
    df_final['Age_Group'] = df_final['Age'].apply(categorize_age)

    # 3. 特徵工程 (B)：時段分類 (時段是交通分析的關鍵)
    # 原始格式通常是 140800.0 (14:08:00)
    df_final['Hour'] = (df_final['Time'] // 10000).fillna(0).astype(int)
    def categorize_time(hour):
        if 6 <= hour < 9: return '早尖峰(06-09)'
        elif 9 <= hour < 17: return '日間(09-17)'
        elif 17 <= hour < 20: return '晚尖峰(17-20)'
        else: return '夜間/深夜(20-06)'
    df_final['Time_Slot'] = df_final['Hour'].apply(categorize_time)

    # 4. 特徵工程 (C)：道路型態群組化
    def group_road(road):
        road = str(road)
        if '交叉路口' in road: return '路口區域'
        if '單路' in road: return '直線路段'
        if '圓環' in road or '廣場' in road: return '圓環/特殊路段'
        return '其他'
    df_final['Road_Category'] = df_final['Road_Type_Main'].apply(group_road)

    # 5. 導出 CSV (utf-8-sig 確保 Excel 與 Power BI 不亂碼)
    df_final.to_csv('accident_analysis_ready.csv', index=False, encoding='utf-8-sig')
    
    print("✅ 恭喜！專題專用資料已產出。")
    print(f"總筆數：{len(df_final)}")
    print(df_final[['Age_Group', 'Time_Slot', 'Road_Category']].head())