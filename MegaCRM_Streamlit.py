# MegaCRM_Streamlit_App.py — CRM + "مداخيل (MB/Bizerte)" مع مصاريف + Pré-Inscription منفصلة
# =================================================================================================
# - CRM كامل: موظفين (قفل بكلمة سر)، قائمة العملاء، بحث، ملاحظات/Tag، تعديل، إضافة، نقل
# - Admin: إضافة/حذف موظف، إضافة عميل لأي موظّف (قفل 30 دقيقة)
# - تبويب "مداخيل (MB/Bizerte)":
#     Revenus: Prix + Montant_Admin + Montant_Structure + Montant_PreInscription (منفصل) + Montant_Total=(Admin+Structure)
#              + Echeance + Reste (على أساس Admin+Structure فقط) + Alert تلقائي
#     Dépenses: Montant + Caisse_Source (Admin/Structure/Inscription) + Mode/Employé/Note...
# - ملخّص شهري تفصيلي (لكل صنف): مداخيل / مصاريف / Reste
# - إخفاء أوراق *_PAIEMENTS و "_" و أوراق المالية من قائمة الموظفين
# - ✅ المداخيل محمية بباسورد الموظّف (ما عادش branch_passwords)
# - ✅ زرّ WhatsApp في واجهة الموظّفين

import json, time
import streamlit as st
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime, date, timedelta
from PIL import Image

# ---------------- Page config ----------------
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

# ---------------- Google Sheets Auth ----------------
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

# ---------------- Schemas ----------------
EXPECTED_HEADERS = [
    "Nom & Prénom","Téléphone","Type de contact","Formation",
    "Remarque","Date ajout","Date de suivi","Alerte",
    "Inscription","Employe","Tag"
]

# Revenus (Pré-Inscription منفصلة، و Montant_Total = Admin + Structure فقط)
FIN_REV_COLUMNS = [
    "Date", "Libellé", "Prix",
    "Montant_Admin", "Montant_Structure", "Montant_PreInscription", "Montant_Total",
    "Echeance", "Reste",
    "Mode", "Employé", "Catégorie", "Note"
]
# Dépenses (مصدر الصندوق)
FIN_DEP_COLUMNS = ["Date","Libellé","Montant","Caisse_Source","Mode","Employé","Catégorie","Note"]
FIN_MONTHS_FR = ["Janvier","Février","Mars","Avril","Mai","Juin","Juillet","Aout","Septembre","Octobre","Novembre","Décembre"]

# ---------------- Finance helpers ----------------
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
        ws.update("1:1", [columns]); return ws
    rows = ws.get_all_values()
    if not rows:
        ws.update("1:1", [columns])
    else:
        header = rows[0]
        if not header or header[:len(columns)] != columns:
            ws.update("1:1", [columns])
    return ws

def _parse_date_any(x):
    for fmt in ("%d/%m/%Y","%Y-%m-%d","%d-%m-%Y","%m/%d/%Y"):
        try: return datetime.strptime(str(x), fmt).date()
        except: pass
    return pd.NaT

def _to_num(s):
    return pd.to_numeric(str(s).replace(" ", "").replace(",", "."), errors="coerce")

def fin_read_df(client, sheet_id: str, title: str, kind: str) -> pd.DataFrame:
    cols = FIN_REV_COLUMNS if kind == "Revenus" else FIN_DEP_COLUMNS
    ws = fin_ensure_ws(client, sheet_id, title, cols)
    values = ws.get_all_values()
    if not values:
        return pd.DataFrame(columns=cols)

    df = pd.DataFrame(values[1:], columns=values[0])
    for c in cols:
        if c not in df.columns:
            df[c] = None

    if "Date" in df.columns:
        df["Date"] = pd.to_datetime(df["Date"], dayfirst=True, errors="coerce")
    if kind == "Revenus" and "Echeance" in df.columns:
        df["Echeance"] = pd.to_datetime(df["Echeance"], dayfirst=True, errors="coerce")

    def _num_series(s):
        return (
            s.astype(str)
             .str.replace(" ", "", regex=False)
             .str.replace(",", ".", regex=False)
             .str.replace("DT", "", regex=False)
             .str.replace("TND", "", regex=False)
             .str.replace("د.", "", regex=False)
             .str.replace("د", "", regex=False)
             .str.replace("€", "", regex=False)
             .str.replace("$", "", regex=False)
        )

    if kind == "Revenus":
        for c in ["Prix", "Montant_Admin", "Montant_Structure", "Montant_PreInscription", "Montant_Total", "Reste"]:
            if c in df.columns:
                df[c] = pd.to_numeric(_num_series(df[c]), errors="coerce").fillna(0.0)
    else:
        if "Montant" in df.columns:
            df["Montant"] = pd.to_numeric(_num_series(df["Montant"]), errors="coerce").fillna(0.0)

    if kind == "Revenus":
        today_ts = pd.Timestamp.now().normalize()
        df["Alert"] = ""
        if "Echeance" in df.columns and "Reste" in df.columns:
            late_mask  = df["Echeance"].notna() & (df["Echeance"] < today_ts) & (df["Reste"] > 0)
            today_mask = df["Echeance"].notna() & (df["Echeance"] == today_ts) & (df["Reste"] > 0)
            df.loc[late_mask,  "Alert"] = "⚠️ متأخر"
            df.loc[today_mask, "Alert"] = "⏰ اليوم"

    return df

def fin_append_row(client, sheet_id: str, title: str, row: dict, kind: str):
    cols = FIN_REV_COLUMNS if kind=="Revenus" else FIN_DEP_COLUMNS
    ws = fin_ensure_ws(client, sheet_id, title, cols)
    header = ws.row_values(1)
    vals = [str(row.get(col, "")) for col in header]
    ws.append_row(vals)

# ---------------- Common helpers ----------------
def fmt_date(d: date | None) -> str:
    return d.strftime("%d/%m/%Y") if isinstance(d, date) else ""

def normalize_tn_phone(s: str) -> str:
    digits = "".join(ch for ch in str(s) if ch.isdigit())
    if digits.startswith("216"): return digits
    if len(digits) == 8: return "216" + digits
    return digits

def format_display_phone(s: str) -> str:
    d = "".join(ch for ch in str(s).strip() if ch.isdigit())
    return f"+{d}" if d else ""

def color_tag(val):
    if isinstance(val, str) and val.strip().startswith("#") and len(val.strip()) == 7:
        return f"background-color: {val}; color: white;"
    return ""

def mark_alert_cell(val: str):
    s = str(val).strip()
    if not s: return ''
    if "متأخر" in s: return 'background-color: #ffe6b3; color: #7a4e00'
    return 'background-color: #ffcccc; color: #7a0000'

def highlight_inscrit_row(row: pd.Series):
    insc = str(row.get("Inscription", "")).strip().lower()
    return ['background-color: #d6f5e8' if insc in ("inscrit","oui") else '' for _ in row.index]

# ---------------- Employee Password Locks ----------------
def _get_emp_password(emp_name: str) -> str:
    try:
        mp = st.secrets["employee_passwords"]
        return str(mp.get(emp_name, mp.get("_default", "1234")))
    except Exception:
        return "1234"

def _emp_unlocked(emp_name: str) -> bool:
    ok = st.session_state.get(f"emp_ok::{emp_name}", False)
    ts = st.session_state.get(f"emp_ok_at::{emp_name}")
    return bool(ok and ts and (datetime.now() - ts) <= timedelta(minutes=15))

def _emp_lock_ui(emp_name: str):
    with st.expander(f"🔐 حماية ورقة الموظّف: {emp_name}", expanded=not _emp_unlocked(emp_name)):
        if _emp_unlocked(emp_name):
            c1, c2 = st.columns(2)
            with c1: st.success("مفتوح (15 دقيقة).")
            with c2:
                if st.button("قفل الآن"):
                    st.session_state[f"emp_ok::{emp_name}"] = False
                    st.session_state[f"emp_ok_at::{emp_name}"] = None
                    st.info("تم القفل.")
        else:
            pwd_try = st.text_input("أدخل كلمة السرّ", type="password", key=f"emp_pwd_{emp_name}")
            if st.button("فتح"):
                if pwd_try and pwd_try == _get_emp_password(emp_name):
                    st.session_state[f"emp_ok::{emp_name}"] = True
                    st.session_state[f"emp_ok_at::{emp_name}"] = datetime.now()
                    st.success("تم الفتح لمدة 15 دقيقة.")
                else:
                    st.error("كلمة سرّ غير صحيحة.")

# ---------------- Load all CRM data (hide non-employee sheets) ----------------
@st.cache_data(ttl=600)
def load_all_data():
    sh = client.open_by_key(SPREADSHEET_ID)
    worksheets = sh.worksheets()
    all_dfs, all_employes = [], []

    for ws in worksheets:
        title = ws.title.strip()
        if title.endswith("_PAIEMENTS"):            # دفوعات قديمة
            continue
        if title.startswith("_"):                   # أوراق نظام
            continue
        if title.startswith("Revenue ") or title.startswith("Dépense "):  # أوراق مالية
            continue

        all_employes.append(title)

        rows = ws.get_all_values()
        if not rows:
            ws.update("1:1", [EXPECTED_HEADERS]); rows = ws.get_all_values()
        try:
            ws.update("1:1", [EXPECTED_HEADERS]); rows = ws.get_all_values()
        except Exception:
            pass

        data_rows = rows[1:] if len(rows) > 1 else []
        fixed_rows = []
        for r in data_rows:
            r = list(r or [])
            if len(r) < len(EXPECTED_HEADERS): r += [""] * (len(EXPECTED_HEADERS) - len(r))
            else: r = r[:len(EXPECTED_HEADERS)]
            fixed_rows.append(r)

        df = pd.DataFrame(fixed_rows, columns=EXPECTED_HEADERS)
        df["__sheet_name"] = title
        all_dfs.append(df)

    big = pd.concat(all_dfs, ignore_index=True) if all_dfs else pd.DataFrame(columns=EXPECTED_HEADERS + ["__sheet_name"])
    return big, all_employes

df_all, all_employes = load_all_data()

# ================== Table renderer with WhatsApp link (ضعها قبل استعمالها) ==================
def render_table(df_disp: pd.DataFrame):
    if df_disp.empty:
        st.info("لا توجد بيانات.")
        return

    _df = df_disp.copy()
    _df["Alerte"] = _df.get("Alerte_view", "")

    # نحضّر رابط واتساب (wa.me) لكل سطر
    def _wa_link(row):
        tel = normalize_tn_phone(row.get("Téléphone", ""))
        if not tel:
            return ""
        name = str(row.get("Nom & Prénom", "")).strip().replace(" ", "%20")
        txt = f"السلام%20عليكم%20{name}"
        return f"https://wa.me/{tel}?text={txt}"

    _df["WhatsApp"] = _df.apply(_wa_link, axis=1)

    display_cols = [c for c in EXPECTED_HEADERS if c in _df.columns] + ["WhatsApp"]

    st.dataframe(
        _df[display_cols],
        use_container_width=True,
        column_config={
            "WhatsApp": st.column_config.LinkColumn(
                "📲 WhatsApp",
                help="افتح محادثة واتساب مع العميل",
                display_text="فتح"
            )
        }
    )

# ---------------- Sidebar ----------------
try:
    st.sidebar.image(Image.open("logo.png"), use_container_width=True)
except Exception:
    pass

tab_choice = st.sidebar.radio("📑 اختر تبويب:", ["CRM", "مداخيل (MB/Bizerte)"], index=0)
role = st.sidebar.radio("الدور", ["موظف", "أدمن"], horizontal=True)
employee = None
if role == "موظف":
    employee = st.sidebar.selectbox("👨‍💼 اختر الموظّف (ورقة Google Sheets)", all_employes) if all_employes else None

# ---------------- Access guards for Finance ----------------
def admin_unlocked() -> bool:
    ok = st.session_state.get("admin_ok", False)
    ts = st.session_state.get("admin_ok_at", None)
    return bool(ok and ts and (datetime.now() - ts) <= timedelta(minutes=30))

def admin_lock_ui():
    with st.sidebar.expander("🔐 إدارة (Admin)", expanded=(role=="أدمن" and not admin_unlocked())):
        if admin_unlocked():
            if st.button("قفل صفحة الأدمِن"):
                st.session_state["admin_ok"] = False
                st.session_state["admin_ok_at"] = None
                st.rerun()
        else:
            admin_pwd = st.text_input("كلمة سرّ الأدمِن", type="password", key="admin_pwd_inp")
            if st.button("فتح صفحة الأدمِن"):
                conf = str(st.secrets.get("admin_password", "admin123"))
                if admin_pwd and admin_pwd == conf:
                    st.session_state["admin_ok"] = True
                    st.session_state["admin_ok_at"] = datetime.now()
                    st.success("تم فتح صفحة الأدمِن لمدة 30 دقيقة.")
                else:
                    st.error("كلمة سرّ غير صحيحة.")

if role == "أدمن":
    admin_lock_ui()

# ---------------- "مداخيل (MB/Bizerte)" Tab ----------------
if tab_choice == "مداخيل (MB/Bizerte)":
    st.title("💸 المداخيل والمصاريف — (منزل بورقيبة & بنزرت)")

    # 🔒 الحماية: الموظف يفتح بورقته أو الأدمِن يفتح بلوحته
    if role == "موظف":
        if not employee:
            st.info("⬅️ إختر الموظّف من اليسار."); st.stop()
        _emp_lock_ui(employee)
        if not _emp_unlocked(employee):
            st.info("🔒 أدخل كلمة سرّ الموظّف في أعلى هذا القسم لفتح المداخيل."); st.stop()
    elif role == "أدمن":
        if not admin_unlocked():
            st.info("🔐 أدخل كلمة سرّ الأدمِن من اليسار لفتح المداخيل."); st.stop()

    with st.sidebar:
        st.markdown("---")
        st.subheader("🔧 إعدادات المداخيل/المصاريف")
        branch = st.selectbox("الفرع", ["Menzel Bourguiba", "Bizerte"], key="fin_branch")
        kind_ar = st.radio("النوع", ["مداخيل","مصاريف"], horizontal=True, key="fin_kind_ar")
        kind = "Revenus" if kind_ar == "مداخيل" else "Dépenses"
        mois   = st.selectbox("الشهر", FIN_MONTHS_FR, index=datetime.now().month-1, key="fin_month")

    fin_title = fin_month_title(mois, kind, branch)

    # قراءة + فلاتر
    df_fin = fin_read_df(client, SPREADSHEET_ID, fin_title, kind)
    df_view = df_fin.copy()

    if role == "موظف" and employee and "Employé" in df_view.columns:
        df_view = df_view[df_view["Employé"].fillna("").str.strip().str.lower() == employee.strip().lower()]

    with st.expander("🔎 فلاتر"):
        c1, c2, c3 = st.columns(3)
        date_from = c1.date_input("من تاريخ", value=None, key="fin_from")
        date_to   = c2.date_input("إلى تاريخ", value=None, key="fin_to")
        search    = c3.text_input("بحث (Libellé/Catégorie/Mode/Note)", key="fin_search")
        if "Date" in df_view.columns:
            if date_from: df_view = df_view[df_view["Date"] >= pd.to_datetime(date_from)]
            if date_to:   df_view = df_view[df_view["Date"] <= pd.to_datetime(date_to)]
        if search:
            m = pd.Series([False]*len(df_view))
            for col in [c for c in ["Libellé","Catégorie","Mode","Employé","Note","Caisse_Source",
                                    "Montant_PreInscription"] if c in df_view.columns]:
                m |= df_view[col].fillna("").astype(str).str.contains(search, case=False, na=False)
            df_view = df_view[m]

    st.subheader(f"📄 {fin_title}")
    if kind == "Revenus":
        cols_show = [c for c in ["Date","Libellé","Prix","Montant_Admin","Montant_Structure","Montant_PreInscription","Montant_Total","Echeance","Reste","Alert","Mode","Employé","Catégorie","Note"] if c in df_view.columns]
    else:
        cols_show = [c for c in ["Date","Libellé","Montant","Caisse_Source","Mode","Employé","Catégorie","Note"] if c in df_view.columns]
    st.dataframe(df_view[cols_show] if not df_view.empty else pd.DataFrame(columns=cols_show), use_container_width=True)

    # ====================== ملخص شهري تفصيلي ======================
    with st.expander("📊 ملخّص الفرع للشهر (حسب الصنف)"):
        rev_df = fin_read_df(client, SPREADSHEET_ID, fin_month_title(mois, "Revenus", branch), "Revenus")
        dep_df = fin_read_df(client, SPREADSHEET_ID, fin_month_title(mois, "Dépenses", branch), "Dépenses")

        # Revenus
        sum_admin    = rev_df["Montant_Admin"].sum()           if ("Montant_Admin" in rev_df.columns and not rev_df.empty) else 0.0
        sum_struct   = rev_df["Montant_Structure"].sum()       if ("Montant_Structure" in rev_df.columns and not rev_df.empty) else 0.0
        sum_preins   = rev_df["Montant_PreInscription"].sum()  if ("Montant_PreInscription" in rev_df.columns and not rev_df.empty) else 0.0
        sum_total_as = rev_df["Montant_Total"].sum()           if ("Montant_Total" in rev_df.columns and not rev_df.empty) else (sum_admin + sum_struct)
        sum_reste_due= rev_df["Reste"].sum()                   if ("Reste" in rev_df.columns and not rev_df.empty) else 0.0

        # Dépenses per caisse
        if not dep_df.empty and "Caisse_Source" in dep_df.columns and "Montant" in dep_df.columns:
            dep_admin  = dep_df.loc[dep_df["Caisse_Source"]=="Caisse_Admin",        "Montant"].sum()
            dep_struct = dep_df.loc[dep_df["Caisse_Source"]=="Caisse_Structure",    "Montant"].sum()
            dep_inscr  = dep_df.loc[dep_df["Caisse_Source"]=="Caisse_Inscription",  "Montant"].sum()
        else:
            dep_admin = dep_struct = dep_inscr = 0.0

        # Reste per category
        reste_admin    = float(sum_admin)  - float(dep_admin)
        reste_struct   = float(sum_struct) - float(dep_struct)
        reste_inscr    = float(sum_preins) - float(dep_inscr)

        # بطاقات مختصرة
        st.markdown("#### 🔹 Admin")
        a1, a2, a3 = st.columns(3)
        a1.metric("مداخيل Admin",   f"{sum_admin:,.2f}")
        a2.metric("مصاريف Admin",   f"{dep_admin:,.2f}")
        a3.metric("Reste Admin",     f"{reste_admin:,.2f}")

        st.markdown("#### 🔹 Structure")
        s1, s2, s3 = st.columns(3)
        s1.metric("مداخيل Structure", f"{sum_struct:,.2f}")
        s2.metric("مصاريف Structure", f"{dep_struct:,.2f}")
        s3.metric("Reste Structure",   f"{reste_struct:,.2f}")

        st.markdown("#### 🔹 Inscription (Pré-Inscription)")
        i1, i2, i3 = st.columns(3)
        i1.metric("مداخيل Inscription", f"{sum_preins:,.2f}")
        i2.metric("مصاريف Inscription", f"{dep_inscr:,.2f}")
        i3.metric("Reste Inscription",   f"{reste_inscr:,.2f}")

        st.markdown("#### 🔸 معلومات إضافية")
        x1, x2, x3 = st.columns(3)
        x1.metric("Total Admin+Structure (مداخيل فقط)", f"{sum_total_as:,.2f}")
        x2.metric("Total مصاريف", f"{(dep_admin + dep_struct + dep_inscr):,.2f}")
        x3.metric("إجمالي المتبقّي بالدروس (Reste Due)", f"{sum_reste_due:,.2f}")

    # ---------------- إضافة عملية جديدة ----------------
    st.markdown("---")
    st.markdown("### ➕ إضافة عملية جديدة")

    selected_client_info = None
    client_default_lib = ""
    client_default_emp = (employee or "") if role == "موظف" else ""

    # (اختياري) اقتراح تلقائي من عميل مُسجّل للمداخيل
    if kind == "Revenus":
        st.markdown("#### 👤 اربط الدفعة بعميل مُسجَّل (اختياري)")
        reg_df = df_all.copy()
        reg_df["Inscription_norm"] = reg_df["Inscription"].fillna("").astype(str).str.strip().str.lower()
        reg_df = reg_df[reg_df["Inscription_norm"].isin(["oui","inscrit"])]
        if role == "موظف" and employee:
            reg_df = reg_df[reg_df["__sheet_name"] == employee]
        if not reg_df.empty:
            def _opt(row):
                phone = format_display_phone(row.get("Téléphone",""))
                return f"{row.get('Nom & Prénom','')} — {phone} — {row.get('Formation','')}  [{row.get('__sheet_name','')}]"
            options = [_opt(r) for _, r in reg_df.iterrows()]
            pick = st.selectbox("اختر عميلًا مُسجَّلًا", ["— بدون اختيار —"] + options, key="fin_client_pick")
            if pick and pick != "— بدون اختيار —":
                idx = options.index(pick); row = reg_df.iloc[idx]
                selected_client_info = {
                    "name": str(row.get("Nom & Prénom","")).strip(),
                    "tel":  str(row.get("Téléphone","")).strip(),
                    "formation": str(row.get("Formation","")).strip(),
                    "emp": str(row.get("__sheet_name","")).strip()
                }
                client_default_lib = f"Paiement {selected_client_info['formation']} - {selected_client_info['name']}".strip()
                if not client_default_emp: client_default_emp = selected_client_info["emp"]

    with st.form("fin_add_row"):
        d1, d2, d3 = st.columns(3)
        date_val = d1.date_input("Date", value=datetime.today())
        libelle  = d2.text_input("Libellé", value=(client_default_lib if kind=="Revenus" else ""))
        employe  = d3.text_input("Employé", value=client_default_emp)

        if kind == "Revenus":
            r1, r2, r3 = st.columns(3)
            prix            = r1.number_input("💰 Prix (سعر التكوين)", min_value=0.0, step=10.0)
            montant_admin   = r2.number_input("🏢 Montant Admin", min_value=0.0, step=10.0)
            montant_struct  = r3.numbe_
