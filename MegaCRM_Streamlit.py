# MegaCRM_Streamlit_App.py — Cloud + Local + Dashboard + Search/Filters + Dedup + Styling + WhatsApp + Paiements

import json
import streamlit as st
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime, date
from PIL import Image
import math

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

# ===== Google Sheets Auth =====
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
        sheet_id = "1DV0KyDRYHofWR60zdx63a9BWBywTFhLavGAExPIa6LI"
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
    if isinstance(val, str) and val.strip().startswith("#") and len(val.strip()) == 7:
        return f"background-color: {val}; color: white;"
    return ""

def mark_alert_cell(val: str):
    s = str(val).strip()
    if not s:
        return ''
    if "متأخرة" in s:
        return 'background-color: #ffe6b3; color: #7a4e00'
    return 'background-color: #ffcccc; color: #7a0000'

def highlight_inscrit_row(row: pd.Series):
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
df_emp = pd.DataFrame()
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

    df_all["Alerte_view"] = base_alert
    df_all.loc[base_alert.eq("") & overdue, "Alerte_view"] = "⚠️ متابعة متأخرة"
    df_all.loc[base_alert.eq("") & due_today, "Alerte_view"] = "⏰ متابعة اليوم"

    df_all["Téléphone_norm"] = df_all["Téléphone"].apply(normalize_tn_phone)
    ALL_PHONES = set(df_all["Téléphone_norm"].dropna().astype(str))

    df_all["Inscription_norm"] = df_all["Inscription"].fillna("").astype(str).str.strip().str.lower()
    inscrit_mask = df_all["Inscription_norm"].isin(["oui", "inscrit"])
    df_all.loc[inscrit_mask, "Date de suivi"] = ""
    df_all.loc[inscrit_mask, "Alerte_view"] = ""
else:
    df_all["Alerte_view"] = ""
    df_all["Mois"] = ""
    df_all["Téléphone_norm"] = ""
    ALL_PHONES = set()

# ===== اختيار الدور =====
role = st.sidebar.selectbox("الدور", ["موظف", "أدمن"])
employee = st.sidebar.selectbox("اختر اسمك", all_employes) if role == "موظف" else None

# ================== واجهة الموظف ==================
if role == "موظف" and employee:
    st.subheader(f"📁 لوحة {employee}")
    df_emp = df_all[df_all["__sheet_name"] == employee].copy()

# ===== ✏️ تعديل بيانات عميل =====
if not df_emp.empty:
    st.markdown("### ✏️ تعديل بيانات عميل")
    df_emp_edit = df_emp.copy()
    df_emp_edit["Téléphone_norm"] = df_emp_edit["Téléphone"].apply(normalize_tn_phone)

    phone_choices = {
        f"[{i}] {row['Nom & Prénom']} — {format_display_phone(row['Téléphone_norm'])}": row["Téléphone_norm"]
        for i, row in df_emp_edit.iterrows()
        if str(row["Téléphone"]).strip() != ""
    }

    if phone_choices:
        chosen_key = st.selectbox("اختر العميل (بالاسم/الهاتف)", list(phone_choices.keys()), key="edit_pick")
        chosen_phone = phone_choices.get(chosen_key, "")

        # باقي الأكواد متاع التعديل (الاسم، الهاتف، الملاحظات...)

        # ======================= 💳 إدارة المدفوعات =======================
        def _to_float(x):
            try:
                return float(str(x).replace(",", ".").strip())
            except:
                return 0.0

        def _ensure_paiements_sheet(sh):
            try:
                return sh.worksheet("Paiements")
            except gspread.exceptions.WorksheetNotFound:
                ws = sh.add_worksheet(title="Paiements", rows="2000", cols="10")
                ws.update("1:1", [["Employe","Téléphonique","Nom","Date paiement","Montant"]])
                return ws

        def _read_payments_for(sh, phone_norm, employe):
            ws_pay = _ensure_paiements_sheet(sh)
            vals = ws_pay.get_all_values()
            if len(vals) <= 1:
                return pd.DataFrame(columns=["Employe","Téléphonique","Nom","Date paiement","Montant"])
            df = pd.DataFrame(vals[1:], columns=vals[0])
            if "Téléphonique" not in df.columns and "Téléphone" in df.columns:
                df = df.rename(columns={"Téléphone": "Téléphonique"})
            df["Montant"] = df["Montant"].apply(_to_float)
            df["_tel_norm"] = df["Téléphonique"].apply(normalize_tn_phone)
            return df[(df["Employe"] == employe) & (df["_tel_norm"] == phone_norm)].copy()

        def _ensure_price_column(ws_emp):
            header = ws_emp.row_values(1) or []
            if "Prix inscription" not in header:
                header.append("Prix inscription")
                ws_emp.update("1:1", [header])

        def _get_set_price(ws_emp, row_idx, new_price=None):
            _ensure_price_column(ws_emp)
            header = ws_emp.row_values(1)
            col = header.index("Prix inscription") + 1
            if new_price is None:
                val = ws_emp.cell(row_idx, col).value or "0"
                return _to_float(val)
            else:
                ws_emp.update_cell(row_idx, col, str(new_price))
                return float(new_price)

        # Bloc المدفوعات
        if chosen_phone:
            sh = client.open_by_key(SPREADSHEET_ID)
            ws_emp = sh.worksheet(employee)
            row_idx = find_row_by_phone(ws_emp, chosen_phone)
            if row_idx:
                cur_name_for_pay = ws_emp.cell(row_idx, EXPECTED_HEADERS.index("Nom & Prénom")+1).value or ""
                current_price = _get_set_price(ws_emp, row_idx, new_price=None)
                df_payments = _read_payments_for(sh, chosen_phone, employee)
                total_paid_before = float(df_payments["Montant"].sum()) if not df_payments.empty else 0.0
                remain_before = max(current_price - total_paid_before, 0.0)

                with st.expander("💳 المدفوعات — اضغط للفتح", expanded=False):
                    c1, c2, c3 = st.columns(3)
                    with c1:
                        price_input = st.number_input("💵 سعر التكوين", min_value=0.0, value=float(current_price), step=10.0, key="price_input")
                    with c2:
                        st.metric("إجمالي مدفوع (قديم)", f"{total_paid_before:,.0f}")
                    with c3:
                        st.metric("المتبقي (قبل الإضافة)", f"{remain_before:,.0f}")

                    st.markdown("#### ➕ إضافة دفعة جديدة")
                    d1, d2 = st.columns(2)
                    with d1:
                        pay_amount = st.number_input("المبلغ المدفوع اليوم", min_value=0.0, value=0.0, step=10.0, key="pay_amount")
                    with d2:
                        pay_date = st.date_input("تاريخ الدفع", value=date.today(), key="pay_date")

                    if st.button("💾 حفظ السعر + إضافة الدفعة", type="primary", key="save_pay_btn"):
                        try:
                            new_price = float(price_input)
                            if not math.isclose(new_price, current_price):
                                current_price = _get_set_price(ws_emp, row_idx, new_price=new_price)

                            if pay_amount > 0:
                                ws_pay = _ensure_paiements_sheet(sh)
                                ws_pay.append_row([employee, chosen_phone, cur_name_for_pay, fmt_date(pay_date), str(pay_amount)])

                            df_after = _read_payments_for(sh, chosen_phone, employee)
                            total_paid_after = float(df_after["Montant"].sum()) if not df_after.empty else 0.0
                            remain_after = max(current_price - total_paid_after, 0.0)

                            st.success("✅ تم الحفظ.")
                            st.info(f"إجمالي المدفوع: **{total_paid_after:,.0f}** — المتبقي: **{remain_after:,.0f}**")

                            if not df_after.empty:
                                st.dataframe(df_after[["Date paiement","Montant"]], use_container_width=True, hide_index=True)

                            st.cache_data.clear()
                        except Exception as e:
                            st.error(f"❌ خطأ أثناء الحفظ: {e}")
