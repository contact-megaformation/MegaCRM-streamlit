# MegaCRM_Streamlit_App.py — Cloud + Local + Dashboard + Search/Filters + Dedup + Styling + WhatsApp + Hide Footer

import json
import streamlit as st
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime, date
from PIL import Image
# ========== Page config ==========
st.set_page_config(page_title="MegaCRM", layout="wide", initial_sidebar_state="expanded")

# ===== عنوان في الوسط =====
st.markdown(
    """
    <div style='text-align:center;'>
        <h1 style='color:#333; margin-top: 8px;'>📊 CRM MEGA FORMATION - إدارة العملاء ميقا للتكوين</h1>
    </div>
    <hr>
    """,
    unsafe_allow_html=True
)

# ===== Google Sheets Auth (Secrets أولاً ثم ملف محلي) =====
SCOPE = ["https://www.googleapis.com/auth/spreadsheets"]

def make_client_and_sheet_id():
    try:
        sa = st.secrets["gcp_service_account"]
        if hasattr(sa, "keys"):
            sa_info = dict(sa)
        elif isinstance(sa, str):
            sa_info = json.loads(sa)
        else:
            raise ValueError("Bad gcp_service_account format")

        creds = Credentials.from_service_account_info(sa_info, scopes=SCOPE)
        client = gspread.authorize(creds)
        sheet_id = st.secrets["SPREADSHEET_ID"]
        return client, sheet_id
    except Exception:
        creds = Credentials.from_service_account_file("service_account.json", scopes=SCOPE)
        client = gspread.authorize(creds)
        sheet_id = "1DV0KyDRYHofWR60zdx63a9BWBywTFhLavGAExPIa6LI"  # بدّلها إذا يلزم
        return client, sheet_id

client, SPREADSHEET_ID = make_client_and_sheet_id()

EXPECTED_HEADERS = [
    "Nom & Prénom","Téléphone","Type de contact","Formation",
    "Remarque","Date ajout","Date de suivi","Alerte",
    "Inscription","Employe","Tag"
]

# ===== Helpers =====
def fmt_date(d: date | None) -> str:
    return d.strftime("%d/%m/%Y") if isinstance(d, date) else ""

def normalize_tn_phone(s: str) -> str:
    """Digits only. If 8-digit Tunisian local -> prefix 216. If starts with 216 keep. Else return digits."""
    digits = "".join(ch for ch in str(s) if ch.isdigit())
    if digits.startswith("216"):
        return digits
    if len(digits) == 8:
        return "216" + digits
    return digits

def format_display_phone(s: str) -> str:
    d = "".join(ch for ch in str(s) if ch.isdigit())
    return f"+{d}" if d else ""

def find_row_by_phone(ws, phone_digits: str) -> int | None:
    values = ws.get_all_values()
    if not values:
        return None
    header = values[0]
    if "Téléphone" not in header:
        return None
    tel_idx = header.index("Téléphone")
    for i, r in enumerate(values[1:], start=2):
        if len(r) > tel_idx and normalize_tn_phone(r[tel_idx]) == phone_digits:
            return i
    return None

# ===== Styling helpers =====
def color_tag(val):
    # لون HEX في Tag
    if isinstance(val, str) and val.strip().startswith("#") and len(val.strip()) == 7:
        return f"background-color: {val}; color: white;"
    return ""

def mark_alert_cell(val: str):
    """تلوين خلية التنبيه حسب الحالة"""
    s = str(val).strip()
    if not s:
        return ''
    if "متأخرة" in s:
        # أصفر للمتأخر
        return 'background-color: #ffe6b3; color: #7a4e00'
    # أحمر لليوم أو أي نص تنبيه
    return 'background-color: #ffcccc; color: #7a0000'

def highlight_inscrit_row(row: pd.Series):
    # تلوين الصف كامل بالأخضر إذا مسجّل
    insc = str(row.get("Inscription", "")).strip().lower()
    is_inscrit = insc in ("inscrit", "oui")
    return ['background-color: #d6f5e8' if is_inscrit else '' for _ in row.index]

# ===== تحميل كل أوراق الموظفين =====
@st.cache_data(ttl=600)
def load_all_data():
    sh = client.open_by_key(SPREADSHEET_ID)
    worksheets = sh.worksheets()
    all_dfs, all_employes = [], []

    for ws in worksheets:
        all_employes.append(ws.title)

        rows = ws.get_all_values()
        if not rows:
            ws.update("1:1", [EXPECTED_HEADERS])
            rows = ws.get_all_values()

        # تأكيد الهيدر
        try:
            ws.update("1:1", [EXPECTED_HEADERS])
            rows = ws.get_all_values()
        except Exception:
            pass

        data_rows = rows[1:] if len(rows) > 1 else []
        fixed_rows = []
        for r in data_rows:
            r = list(r) if r is not None else []
            if len(r) < len(EXPECTED_HEADERS):
                r = r + [""] * (len(EXPECTED_HEADERS) - len(r))
            else:
                r = r[:len(EXPECTED_HEADERS)]
            fixed_rows.append(r)

        df = pd.DataFrame(fixed_rows, columns=EXPECTED_HEADERS)
        df["__sheet_name"] = ws.title
        all_dfs.append(df)

    big = pd.concat(all_dfs, ignore_index=True) if all_dfs else pd.DataFrame(columns=EXPECTED_HEADERS + ["__sheet_name"])
    return big, all_employes

df_all, all_employes = load_all_data()
df_emp = pd.DataFrame()   # حماية عامة
filtered_df = pd.DataFrame()

# ===== أعمدة مشتقّة + إعدادات =====
if not df_all.empty:
    df_all["DateAjout_dt"] = pd.to_datetime(df_all["Date ajout"], dayfirst=True, errors="coerce")
    df_all["DateSuivi_dt"] = pd.to_datetime(df_all["Date de suivi"], dayfirst=True, errors="coerce")
    df_all["Mois"] = df_all["DateAjout_dt"].dt.strftime("%m-%Y")

    today = datetime.now().date()
    base_alert = df_all["Alerte"].fillna("").astype(str).str.strip()

    dsv_date = df_all["DateSuivi_dt"].dt.date
    due_today = dsv_date.eq(today).fillna(False)
    overdue = dsv_date.lt(today).fillna(False)

    # عرض التنبيه
    df_all["Alerte_view"] = base_alert
    df_all.loc[base_alert.eq("") & overdue, "Alerte_view"] = "⚠️ متابعة متأخرة"
    df_all.loc[base_alert.eq("") & due_today, "Alerte_view"] = "⏰ متابعة اليوم"

    df_all["Téléphone_norm"] = df_all["Téléphone"].apply(normalize_tn_phone)
    ALL_PHONES = set(df_all["Téléphone_norm"].dropna().astype(str))

    # تنظيف المسجّلين: تفريغ متابعة/تنبيه
    df_all["Inscription_norm"] = df_all["Inscription"].fillna("").astype(str).str.strip().str.lower()
    inscrit_mask = df_all["Inscription_norm"].isin(["oui", "inscrit"])
    df_all.loc[inscrit_mask, "Date de suivi"] = ""
    df_all.loc[inscrit_mask, "Alerte_view"] = ""
else:
    df_all["Alerte_view"] = ""
    df_all["Mois"] = ""
    df_all["Téléphone_norm"] = ""
    ALL_PHONES = set()
