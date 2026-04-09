# 112年 A1 類道路交通事故互動式分析系統

這是一個基於 Python 與 Streamlit 開發的 Web 數據儀表板，旨在分析台灣 112 年度 A1 類道路交通事故，並提供直觀的地理空間分佈與統計圖表。

## 🚀 專案核心價值
本專案針對「高齡族群」與「事故路段型態」進行交叉分析，透過互動式篩選器，協助使用者快速定位交通安全熱點，取代傳統靜態 PDF 報表。

## 🛠️ 技術架構 (System Architecture)
為了確保系統在各種網路環境（如校園防火牆或行動網路）下的穩定性，本專案採用了以下架構：

- **數據中台**：利用 GitHub Gist 託管 CSV 原始資料，實現雲端資料同步。
- **後端引擎**：使用 Python Pandas 進行資料清洗、欄位標準化與座標轉換。
- **前端呈現**：採用 Streamlit 響應式框架，實現 RWD 跨裝置（電腦/手機）瀏覽。
- **地圖渲染**：使用輕量化 `st.map` 元件，優化慢速網路下的載入效能。


## 📁 檔案結構
- `transportation.py`: Streamlit Web 應用主程式 (包含資料清洗與視覺化邏輯)。
- `requirements.txt`: 紀錄專案相依套件 (Pandas, Streamlit)。
- `Analysis_Report.pbix`: 深入分析用的 Power BI 原始檔案。

## ⚙️ 技術問題解決紀錄 (Troubleshooting)
1. **網路效能優化**：針對瀏覽器偵測到 Slow network 的情況，將高資源消耗的 3D 繪圖引擎替換為輕量化原生映射組件，提升載入速度 50% 以上。
2. **欄位自動相容機制**：開發了「動態欄位檢查邏輯」，自動識別並更正 CSV 中的大小寫差異（如 Latitude 轉為 latitude），增強系統容錯性。
3. **雲端部署整合**：透過 GitHub 與 Streamlit Cloud 實現 CI/CD 自動化部署。

## 🖥️ 如何在本地運行
1. 安裝必要套件：
   ```bash
   pip install streamlit pandas
