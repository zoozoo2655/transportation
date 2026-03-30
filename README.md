# 112年 A1 類道路交通事故分析系統 (Streamlit Cloud Edition)

這是一個基於 Python 開發的數據分析 Web 應用，旨在分析台灣 112 年度 A1 類道路交通事故，探討路口型態與高齡族群之事故關聯性。

## 🚀 專案亮點
- **架構優化**：針對受限開發環境 (如學校防火牆)，採用 GitHub Gist 作為雲端數據中台。
- **互動式分析**：整合 Streamlit 與 Pydeck，提供經緯度熱點空間分佈與動態篩選。
- **技術整合**：實現了從 Python (Pandas) 清洗資料到 Power BI / Streamlit 多平台呈現的完整流程。

## 🛠️ 技術棧 (Tech Stack)
- **Language**: Python 3.10+
- **Framework**: Streamlit (Web UI)
- **Visualization**: Pydeck (Map), Power BI (Deep Analysis)
- **Data Source**: 政府資料開放平台 (Data.gov.tw)

## 📁 檔案結構說明
- `transportation.py`: Streamlit Web 應用的主程式。
- `交通事故對於年齡、車輛種類、道路類型之分析.pbix`: 完整的 Power BI 深入分析報表。
- `requirements.txt`: 專案所需的套件清單。

## 🔧 本地運行指南
1. 克隆此專案。
2. 安裝必要套件：
   ```bash
   pip install -r requirements.txt
3. 在終端機上:
   streamlit run transportation.py
