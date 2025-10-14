# MegaCRM_Streamlit.py
# CRM فقط + أرشيف — بدون أي كود مداخيل/مصاريف — مع زر يفتح MegaPay

import json, urllib.parse, time
import streamlit as st
import pandas as pd
import gspread
import gspread.exceptions as gse
from google.oauth2.service_account import Credentials
from datetime import datetime, date, timedelta, timezone
from PIL import Image

# ============ إعداد الصفحة ============
st.set_page_config(page_title="MegaCRM", layout="wide", initial_sidebar_state="expanded")
st.markdown("""
<div style='text-align:center'>
  <h1>📊 CRM MEGA FORMATION - إدارة العملاء</h1>
</div>
<hr/>
""", unsafe_allow_html=True)

# ============ روابط جانبية ============
with st.sidebar:
    st.markdown("### 💵 إدارة المداخيل والمصاريف")
    st.markdown("""
    <a href="https://megapay.streamlit.app/" target="_blank"
       style="display:inline-block;background:linear-gradient(90deg,#16a085,#1abc9c);
       color:#fff;padding:10px 18px;border-radius:10px;text-decoration:none;
       font-weight:600;font-size:15px;text-align:center;width:100%;
       box-shadow:0 4px 8px rgba(0,0,0,0.15);">
       🚀 فتح MegaPay
    </a>
    """, unsafe_allow_html=True)

    st.markdown("---")

    st.markdown("### 👨‍🏫 بوابة المكوّنين")
    st.markdown("""
    <a href="https://mega-formateur.streamlit.app/" target="_blank"
       style="display:inline-block;background:linear-gradient(90deg,#0078d7,#00b7ff);
       color:#fff;padding:10px 18px;border-radius:10px;text-decoration:none;
       font-weight:600;font-size:15px;text-align:center;width:100%;
       box-shadow:0 4px 8px rgba(0,0,0,0.15);">
       🔀 فتح Mega Formateur
    </a>
    """, unsafe_allow_html=True)

# ============ Google Auth ============
SCOPE = ["https://www.googleapis.com/auth/spreadsheets"]

def make_client_and_sheet_id():
    try:
        sa = st.secrets["gcp_service_account"]
        sa_info = dict(sa) if hasattr(sa, "keys") else (json.loads(sa) if isinstance(sa, str) else {})
        creds = Credentials.from_service_account_info(sa_info, scopes=SCOPE)
        client = gspread.authorize(creds)
        sheet_id = st.secrets["SPREADSHEET_ID"]
        return client, sheet_id
    except Exception:
        creds = Credentials.from_service_account_file("service_account.json", scopes=SCOPE)
        client = gspread.authorize(creds)
        return client, "PUT_YOUR_SHEET_ID_HERE"

client, SPREADSHEET_ID = make_client_and_sheet_id()

# ============ ثوابت الجداول ============
EXPECTED_HEADERS = [
    "Nom & Prénom","Téléphone","Type de contact","Formation",
    "Remarque","Date ajout","Date de suivi","Alerte",
    "Inscription","Employe","Tag"
]
REASSIGN_LOG_SHEET   = "Reassign_Log"
REASSIGN_LOG_HEADERS = ["timestamp","moved_by","src_employee","dst_employee","client_name","phone"]

# ============ Helpers ============
def fmt_date(d: date|None) -> str:
    return d.strftime("%d/%m/%Y") if isinstance(d, date) else ""

def normalize_tn_phone(s: str) -> str:
    digits = "".join(ch for ch in str(s) if ch.isdigit())
    if digits.startswith("216"): return digits
    if len(digits) == 8: return "216" + digits
    return digits

def format_display_phone(s: str) -> str:
    d = "".join(ch for ch in str(s) if ch.isdigit())
    return f"+{d}" if d else ""

def color_tag(val):
    if isinstance(val, str) and val.strip().startswith("#") and len(val.strip()) == 7:
        return f"background-color: {val}; color: white;"
    return ""

def mark_alert_cell(val: str):
    s = str(val).strip()
    if not s: return ''
    if "متأخر" in s: return 'background-color:#ffe6b3;color:#7a4e00'
    return 'background-color:#ffcccc;color:#7a0000'

def highlight_inscrit_row(row: pd.Series):
    insc = str(row.get("Inscription","")).strip().lower()
    return ['background-color:#d6f5e8' if insc in ("inscrit","oui") else '' for _ in row.index]

# ============ Sheets Utils ============
def get_spreadsheet():
    if st.session_state.get("sh_id") == SPREADSHEET_ID and "sh_obj" in st.session_state:
        return st.session_state["sh_obj"]
    for _ in range(5):
        try:
            sh = client.open_by_key(SPREADSHEET_ID)
            st.session_state["sh_obj"] = sh
            st.session_state["sh_id"] = SPREADSHEET_ID
            return sh
        except gse.APIError:
            time.sleep(1)
    st.error("❌ تعذّر فتح Google Sheet.")
    st.stop()

def ensure_ws(title: str, columns: list[str]):
    sh = get_spreadsheet()
    try:
        ws = sh.worksheet(title)
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title=title, rows="2000", cols=str(max(len(columns), 8)))
        ws.update("1:1", [columns])
        return ws
    header = ws.row_values(1)
    if not header or header[:len(columns)] != columns:
        ws.update("1:1", [columns])
    return ws

# ============ تحميل كل أوراق الموظفين ============
@st.cache_data(ttl=600)
def load_all_data():
    sh = get_spreadsheet()
    all_dfs, all_emps = [], []
    for ws in sh.worksheets():
        t = ws.title.strip()
        if t.endswith("_PAIEMENTS") or t.startswith("_") or t == REASSIGN_LOG_SHEET: continue
        all_emps.append(t)
        rows = ws.get_all_values()
        if not rows: ws.update("1:1", [EXPECTED_HEADERS]); rows = ws.get_all_values()
        data = rows[1:] if len(rows) > 1 else []
        fixed = [r + [""]*(len(EXPECTED_HEADERS)-len(r)) for r in data]
        df = pd.DataFrame(fixed, columns=EXPECTED_HEADERS)
        df["__sheet_name"] = t
        all_dfs.append(df)
    big = pd.concat(all_dfs, ignore_index=True) if all_dfs else pd.DataFrame(columns=EXPECTED_HEADERS+["__sheet_name"])
    return big, all_emps

df_all, all_employes = load_all_data()

# ============ Sidebar ============
try:
    st.sidebar.image(Image.open("logo.png"), use_container_width=True)
except Exception:
    pass

tab_choice = st.sidebar.radio("📑 اختر تبويب:", ["CRM", "أرشيف"], index=0)
role = st.sidebar.radio("الدور", ["موظف","أدمن"], horizontal=True)
employee = st.sidebar.selectbox("👨‍💼 اختر الموظّف", all_employes) if (role=="موظف" and all_employes) else None

# 🔐 الأقفال والإداري (مختصر)
def emp_pwd_for(emp): 
    try: return st.secrets["employee_passwords"].get(emp,"1234")
    except: return "1234"
def emp_unlocked(emp):
    ok = st.session_state.get(f"emp_ok::{emp}",False)
    ts = st.session_state.get(f"emp_ok_at::{emp}")
    return ok and ts and (datetime.now()-ts)<=timedelta(minutes=15)
def emp_lock_ui(emp):
    with st.expander(f"🔐 حماية ورقة {emp}", expanded=not emp_unlocked(emp)):
        if emp_unlocked(emp): 
            if st.button("قفل الآن", key=f"lock_{emp}"): 
                st.session_state[f"emp_ok::{emp}"]=False; st.rerun()
        else:
            pwd = st.text_input("كلمة السرّ", type="password")
            if st.button("فتح"):
                if pwd==emp_pwd_for(emp):
                    st.session_state[f"emp_ok::{emp}"]=True; st.session_state[f"emp_ok_at::{emp}"]=datetime.now(); st.rerun()
                else: st.error("❌ خطأ")

# ============ تبويب الموظف ============
if role=="موظف" and employee:
    emp_lock_ui(employee)
    if not emp_unlocked(employee): st.stop()
    st.subheader(f"📁 لوحة {employee}")

    df_emp = df_all[df_all["__sheet_name"]==employee].copy()
    if df_emp.empty: st.warning("⚠️ لا يوجد عملاء."); st.stop()

    df_emp["DateAjout_dt"]=pd.to_datetime(df_emp["Date ajout"], dayfirst=True, errors="coerce")
    df_emp=df_emp.dropna(subset=["DateAjout_dt"])
    df_emp["Mois"]=df_emp["DateAjout_dt"].dt.strftime("%m-%Y")
    month_opts=sorted(df_emp["Mois"].dropna().unique(),reverse=True)
    month_filter=st.selectbox("🗓️ اختر شهر الإضافة",month_opts)
    filtered_df=df_emp[df_emp["Mois"]==month_filter].copy()

    # ===== فلترة التكوين =====
    if not filtered_df.empty:
        formations_list=sorted(filtered_df["Formation"].fillna("").astype(str).str.strip().replace({"": "غير محدد"}).unique().tolist())
        chosen_forms=st.multiselect("🎓 اختر التكوين/ات", formations_list, default=formations_list)
        if chosen_forms:
            tmp=filtered_df.copy()
            tmp["Formation_norm"]=tmp["Formation"].fillna("").astype(str).str.strip().replace({"": "غير محدد"})
            tmp=tmp[tmp["Formation_norm"].isin(chosen_forms)]
            tmp=tmp.drop(columns=["Formation_norm"])
            filtered_df=tmp
        else:
            filtered_df=filtered_df.iloc[0:0]

    def render_table(df):
        if df.empty: st.info("لا توجد بيانات."); return
        _df=df.copy(); _df["Alerte"]=_df.get("Alerte_view","")
        st.dataframe(
            _df[EXPECTED_HEADERS].style
            .apply(highlight_inscrit_row,axis=1)
            .applymap(mark_alert_cell,subset=["Alerte"])
            .applymap(color_tag,subset=["Tag"]),
            use_container_width=True
        )

    st.markdown("### 📋 قائمة العملاء")
    render_table(filtered_df)

# باقي الكود متاع الأرشيف والأدمين يخلي كيما هو (ما تبدّلش)
