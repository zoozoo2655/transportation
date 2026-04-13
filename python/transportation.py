import streamlit as st
import pandas as pd

# ==========================================
# 1. 頁面基礎設定 (輕量化配置)
# ==========================================
st.set_page_config(
    page_title="112年交通數據儀表板",
    page_icon="🚗",
    layout="wide"
)

# 修改後的 CSS：同時固定背景色與文字顏色
st.markdown("""
    <style>
    /* 強制設定 Metric 卡片的樣式 */
    [data-testid="stMetric"] {
        background-color: #ffffff !important; /* 強制背景為白 */
        padding: 15px !important;
        border-radius: 10px !important;
        border: 1px solid #e0e0e0 !important;
        box-shadow: 0 2px 4px rgba(0,0,0,0.05) !important;
    }
    /* 強制設定 Metric 內的文字顏色為深灰色，避免深色模式下變白字 */
    [data-testid="stMetric"] label, 
    [data-testid="stMetric"] div {
        color: #31333F !important; 
    }
    </style>
    """, unsafe_allow_html=True)

# ==========================================
# 2. 資料載入與快取 (針對慢速網路優化)
# ==========================================
# 這是 GitHub Gist 的 "Raw" 連結
DATA_URL = "https://gist.githubusercontent.com/zoozoo2655/fc7f38673253b84af35439a1ebd1aa17/raw/accident_analysis_ready"

@st.cache_data(ttl=3600) # 快取一小時，減少重複下載
def load_and_clean_data():
    try:
        # 讀取 CSV (處理編碼)
        df = pd.read_csv(DATA_URL, encoding='utf-8-sig')
        
        # 清除欄位空格並統一命名
        df.columns = df.columns.str.strip()
        
        # 建立欄位映射表
        mapping = {
            'Date': '日期',
            'Road_Type_Main': '道路型態',
            'Vehicle_Type': '車種',
            'Age_Group': '年齡分組',
            'Longitude': 'longitude',
            'Latitude': 'latitude',
            'Time_Slot': '時段'
        }
        
        # 僅提取存在的欄位，避免 KeyError
        present_cols = [c for c in mapping.keys() if c in df.columns]
        clean_df = df[present_cols].rename(columns=mapping)
        
        # 強制轉換經緯度為數字 (這是繪圖成功的關鍵)
        if 'latitude' in clean_df.columns and 'longitude' in clean_df.columns:
            clean_df['latitude'] = pd.to_numeric(clean_df['latitude'], errors='coerce')
            clean_df['longitude'] = pd.to_numeric(clean_df['longitude'], errors='coerce')
            # 剔除無法轉換或缺失的座標點
            clean_df = clean_df.dropna(subset=['latitude', 'longitude'])
            
        return clean_df
    except Exception as e:
        st.error(f"⚠️ 資料載入失敗: {e}")
        return pd.DataFrame()

# 執行載入
df = load_and_clean_data()

# ==========================================
# 3. 側邊欄互動篩選
# ==========================================
st.sidebar.header("📊 篩選條件")

if not df.empty:
    # 建立多選下拉選單
    road_list = df['道路型態'].unique().tolist() if '道路型態' in df.columns else []
    sel_roads = st.sidebar.multiselect("選擇道路類型", road_list, default=road_list)

    age_list = df['年齡分組'].unique().tolist() if '年齡分組' in df.columns else []
    sel_ages = st.sidebar.multiselect("選擇年齡分組", age_list, default=age_list)

    # 執行過濾邏輯
    filtered_df = df[
        (df['道路型態'].isin(sel_roads)) & 
        (df['年齡分組'].isin(sel_ages))
    ]
else:
    filtered_df = pd.DataFrame()

# ==========================================
# 4. 主畫面視覺化 (Dashboard)
# ==========================================
st.title("🚗 112年 A1 類道路交通事故分析")
st.info("💡資料來源:政府資料開放平台")

if not filtered_df.empty:
    # 第一列：核心指標 (KPIs)
    col_kpi1, col_kpi2, col_kpi3 = st.columns(3)
    col_kpi1.metric("總事故件數", f"{len(filtered_df)} 件")
    if '車種' in filtered_df.columns:
        col_kpi2.metric("主要肇事車種", filtered_df['車種'].mode()[0])
    if '時段' in filtered_df.columns:
        col_kpi3.metric("高發時段", filtered_df['時段'].mode()[0])

    st.markdown("---")

    # 第二列：地圖展示 (改用最輕量的 st.map)
    st.subheader("📍 事故地理位置分佈")
    # st.map 是 Streamlit 最穩定的地圖組件，不需額外 JS 庫
    st.map(filtered_df[['latitude', 'longitude']], use_container_width=True)

    st.markdown("---")

    # 第三列：統計圖表
    c1, c2 = st.columns(2)
    with c1:
        if '車種' in filtered_df.columns:
            st.subheader("🛵 車種分佈")
            st.bar_chart(filtered_df['車種'].value_counts())
    with c2:
        if '年齡分組' in filtered_df.columns:
            st.subheader("👥 年齡層比例")
            # 必須計算各組數量的次數 (value_counts)，否則折線圖會亂掉
            age_counts = filtered_df['年齡分組'].value_counts()
            st.line_chart(age_counts) # 確保有括號，且裡面有放數據