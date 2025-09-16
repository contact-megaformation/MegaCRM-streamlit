# MegaCRM_Streamlit_App.py — CRM + Finance (MB/Bizerte)
# ================================================
# - واجهة موظف: عملاء + إضافة + ملاحظات/Tag + تعديل + نقل بين الموظفين
# - حذف قسم "الدفوعات" القديم
# - إضافة تبويب Finance: Revenus (Prix, Admin, Structure, Total) + Dépenses بسيطة
# - ملخص شهري (Admin/Structure/Total Revenus + Total Dépenses)
# - حماية كل ورقة موظف بكلمة سر

import json, time
import streamlit as st
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime, date, timedelta
from PIL import Image

# ========== Page config ==========
st.set_page_config(page_title="MegaCRM", layout="wide", initial_sidebar_state="expanded")
st.markdown(
    """
    <div style='text-align:center;'>
        <h1 style='color:#333; margin-top: 8px;'>📊 CRM MEGA FORMATION - إدارة العملاء</h1>
    </div>
    <hr>
    """,
    unsafe_allow_html=True
)

# ===== Google Sheets Auth =====
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
        sheet_id = "1DV0KyDRYHofWR60zdx63a9BWBywTFhLavGAExPIa6LI"  # بدّلها إذا يلزم
        return client, sheet_id

client, SPREADSHEET_ID = make_client_and_sheet_id()

# ===== CRM schema =====
EXPECTED_HEADERS = [
    "Nom & Prénom","Téléphone","Type de contact","Formation",
    "Remarque","Date ajout","Date de suivi","Alerte",
    "Inscription","Employe","Tag"
]

# ===== Finance =====
FIN_REV_COLUMNS = [
    "Date", "Libellé", "Prix",
    "Montant_Admin", "Montant_Structure", "Montant_Total",
    "Mode", "Employé", "Catégorie", "Note"
]
FIN_DEP_COLUMNS = ["Date","Libellé","Montant","Mode","Employé","Catégorie","Note"]
FIN_MONTHS_FR = ["Janvier","Février","Mars","Avril","Mai","Juin","Juillet","Aout","Septembre","Octobre","Novembre","Décembre"]

def _branch_passwords():
    try:
        b = st.secrets["branch_passwords"]
        return {"Menzel Bourguiba": str(b.get("MB", "MB_2025!")), "Bizerte": str(b.get("BZ", "BZ_2025!"))}
    except Exception:
        return {"Menzel Bourguiba": "MB_2025!", "Bizerte": "BZ_2025!"}

def fin_month_title(mois: str, kind: str, branch: str):
    prefix = "Revenue " if kind == "Revenus" else "Dépense "
    short = "MB" if "Menzel" in branch else "BZ"
    return f"{prefix}{mois} ({short})"

def fin_ensure_ws(client, sheet_id: str, title: str, columns: list[str]):
    sh = client.open_by_key(sheet_id)
    try:
        ws = sh.worksheet(title)
    except Exception:
        ws = sh.add_worksheet(title=title, rows="2000", cols=str(max(len(columns), 8)))
        ws.update("1:1", [columns])
        return ws
    rows = ws.get_all_values()
    if not rows:
        ws.update("1:1", [columns])
    else:
        header = rows[0]
        if not header or header[:len(columns)] != columns:
            ws.update("1:1", [columns])
    return ws

def fin_read_df(client, sheet_id: str, title: str, kind: str) -> pd.DataFrame:
    cols = FIN_REV_COLUMNS if kind=="Revenus" else FIN_DEP_COLUMNS
    ws = fin_ensure_ws(client, sheet_id, title, cols)
    values = ws.get_all_values()
    if not values:
        return pd.DataFrame(columns=cols)
    df = pd.DataFrame(values[1:], columns=values[0])
    if "Date" in df.columns:
        def _p(x):
            for fmt in ("%d/%m/%Y","%Y-%m-%d","%d-%m-%Y","%m/%d/%Y"):
                try: return datetime.strptime(str(x), fmt).date()
                except: pass
            return pd.NaT
        df["Date"] = df["Date"].apply(_p)
    if kind=="Revenus":
        for c in ["Prix","Montant_Admin","Montant_Structure","Montant_Total"]:
            if c in df.columns:
                df[c] = (
                    df[c].astype(str)
                    .str.replace(",", ".", regex=False)
                    .str.replace(" ", "", regex=False)
                    .apply(lambda x: pd.to_numeric(x, errors="coerce"))
                )
    else:
        if "Montant" in df.columns:
            df["Montant"] = (
                df["Montant"].astype(str)
                .str.replace(",", ".", regex=False)
                .str.replace(" ", "", regex=False)
                .apply(lambda x: pd.to_numeric(x, errors="coerce"))
            )
    return df

def fin_append_row(client, sheet_id: str, title: str, row: dict, kind: str):
    cols = FIN_REV_COLUMNS if kind=="Revenus" else FIN_DEP_COLUMNS
    ws = fin_ensure_ws(client, sheet_id, title, cols)
    header = ws.row_values(1)
    vals = [str(row.get(col, "")) for col in header]
    ws.append_row(vals)

# ===== Helpers =====
def fmt_date(d: date | None) -> str:
    return d.strftime("%d/%m/%Y") if isinstance(d, date) else ""

def normalize_tn_phone(s: str) -> str:
    digits = "".join(ch for ch in str(s) if ch.isdigit())
    if digits.startswith("216"): return digits
    if len(digits) == 8: return "216" + digits
    return digits

def format_display_phone(s: str) -> str:
    d = "".join(ch for ch in str(s) if ch.isdigit())
    return f"+{d}" if d else ""

def highlight_inscrit_row(row: pd.Series):
    insc = str(row.get("Inscription", "")).strip().lower()
    return ['background-color: #d6f5e8' if insc in ("inscrit","oui") else '' for _ in row.index]

# ================== Sidebar ==================
st.sidebar.title("🔧 الإعدادات")
tab_choice = st.sidebar.radio("اختر تبويب:", ["CRM","Finance (MB/Bizerte)"])
role = st.sidebar.radio("الدور", ["موظف", "أدمن"], horizontal=True)
employee = None
if role=="موظف":
    employee = st.sidebar.text_input("👤 اسم الموظف (ورقة Google Sheets)")
    emp_pwd = st.sidebar.text_input("🔒 كلمة سر الموظف", type="password")
    if not emp_pwd or emp_pwd != "1234":  # بدّل كلمة السر لكل موظف
        st.sidebar.error("أدخل كلمة السرّ الصحيحة")
        st.stop()

# ================== تبويب Finance ==================
if tab_choice=="Finance (MB/Bizerte)":
    st.title("💸 المالية — مداخيل/مصاريف")

    branch = st.selectbox("الفرع", ["Menzel Bourguiba","Bizerte"])
    kind = st.radio("النوع", ["Revenus","Dépenses"], horizontal=True)
    mois = st.selectbox("الشهر", FIN_MONTHS_FR, index=datetime.now().month-1)

    fin_title = fin_month_title(mois, kind, branch)

    df_fin = fin_read_df(client, SPREADSHEET_ID, fin_title, kind)
    st.subheader(f"📄 {fin_title}")
    st.dataframe(df_fin, use_container_width=True)

    # ملخص
    st.markdown("### 📊 ملخص")
    rev_df = fin_read_df(client, SPREADSHEET_ID, fin_month_title(mois,"Revenus",branch),"Revenus")
    dep_df = fin_read_df(client, SPREADSHEET_ID, fin_month_title(mois,"Dépenses",branch),"Dépenses")
    sum_admin = rev_df["Montant_Admin"].sum() if "Montant_Admin" in rev_df else 0
    sum_struct = rev_df["Montant_Structure"].sum() if "Montant_Structure" in rev_df else 0
    sum_total = rev_df["Montant_Total"].sum() if "Montant_Total" in rev_df else 0
    sum_dep = dep_df["Montant"].sum() if "Montant" in dep_df else 0
    c1,c2,c3,c4 = st.columns(4)
    c1.metric("Admin",f"{sum_admin:.2f}")
    c2.metric("Structure",f"{sum_struct:.2f}")
    c3.metric("Total Revenus",f"{sum_total:.2f}")
    c4.metric("Total Dépenses",f"{sum_dep:.2f}")

    # إضافة عملية جديدة
    st.markdown("### ➕ إضافة عملية جديدة")
    with st.form("add_fin"):
        d = st.date_input("📅 التاريخ", value=date.today())
        lib = st.text_input("Libellé")
        if kind=="Revenus":
            prix = st.number_input("Prix",0.0)
            adm = st.number_input("Montant Admin",0.0)
            struct = st.number_input("Montant Structure",0.0)
            total = adm+struct
            st.write(f"💰 Total = {total}")
            mode = st.selectbox("Mode",["Espèces","Virement","Chèque"])
            note = st.text_area("Note")
            if st.form_submit_button("حفظ"):
                row = {
                    "Date":fmt_date(d),"Libellé":lib,"Prix":prix,
                    "Montant_Admin":adm,"Montant_Structure":struct,"Montant_Total":total,
                    "Mode":mode,"Employé":employee or "","Catégorie":"Revenus","Note":note
                }
                fin_append_row(client, SPREADSHEET_ID, fin_title, row, "Revenus")
                st.success("تمت الإضافة ✅")
        else:
            mont = st.number_input("Montant",0.0)
            mode = st.selectbox("Mode",["Espèces","Virement","Chèque"])
            note = st.text_area("Note")
            if st.form_submit_button("حفظ"):
                row = {
                    "Date":fmt_date(d),"Libellé":lib,"Montant":mont,
                    "Mode":mode,"Employé":employee or "","Catégorie":"Dépenses","Note":note
                }
                fin_append_row(client, SPREADSHEET_ID, fin_title, row, "Dépenses")
                st.success("تمت الإضافة ✅")
