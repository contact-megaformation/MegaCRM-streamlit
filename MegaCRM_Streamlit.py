# MegaCRM_Streamlit_App.py — CRM + Revenus/Dépenses + Pré-Inscription + InterNotes + Transfers Log
# =================================================================================================
# - CRM: موظفين، بحث، تعديل، إضافة، نقل + زر WhatsApp
# - Admin: إضافة/حذف موظف، إضافة عميل لأي موظّف (قفل 30 دقيقة)
# - تبويب "مداخيل (MB/Bizerte)": Revenus + Dépenses + Pré-Inscription منفصل
# - ملخّص شهري + إحصائيات شهرية (شهر بشهر)
# - لوج نقل عميل: _TransfersLog
# - تحديث دفع سريع: اختيار عميل مُسجَّل وتعديل المبالغ على نفس Libellé
# - 📝 نوط داخلية بين الموظفين

import json, urllib.parse, base64, uuid
import streamlit as st
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime, date, timedelta, timezone
from PIL import Image

# =============== Page config ===============
st.set_page_config(page_title="MegaCRM", layout="wide", initial_sidebar_state="expanded")
st.markdown(
    "<div style='text-align:center;'><h1>📊 CRM MEGA FORMATION - إدارة العملاء</h1></div><hr>",
    unsafe_allow_html=True
)

# =============== Google Sheets Auth ===============
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

# =============== Constants/Schemas ===============
EXPECTED_HEADERS = [
    "Nom & Prénom","Téléphone","Type de contact","Formation",
    "Remarque","Date ajout","Date de suivi","Alerte",
    "Inscription","Employe","Tag"
]

FIN_REV_COLUMNS = [
    "Date","Libellé","Prix",
    "Montant_Admin","Montant_Structure","Montant_PreInscription","Montant_Total",
    "Echeance","Reste","Mode","Employé","Catégorie","Note"
]
FIN_DEP_COLUMNS = ["Date","Libellé","Montant","Caisse_Source","Mode","Employé","Catégorie","Note"]
FIN_MONTHS_FR = ["Janvier","Février","Mars","Avril","Mai","Juin","Juillet","Aout","Septembre","Octobre","Novembre","Décembre"]

TRANSFERS_SHEET = "_TransfersLog"
TRANSFERS_HEADERS = ["timestamp","by_user","src","dst","client_name","phone_norm"]

INTER_NOTES_SHEET = "InterNotes"
INTER_NOTES_HEADERS = ["timestamp","sender","receiver","message","status","note_id"]

# =============== Helpers ===============
def safe_unique_columns(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty: return df
    df = df.copy(); df.columns = pd.Index(df.columns).astype(str)
    return df.loc[:, ~df.columns.duplicated(keep="first")]

def _to_num_series(s):
    return (pd.Series(s).astype(str).str.replace(" ", "", regex=False)
            .str.replace(",", ".", regex=False)
            .pipe(pd.to_numeric, errors="coerce").fillna(0.0))

def fmt_date(d: date | None) -> str:
    return d.strftime("%d/%m/%Y") if isinstance(d, date) else ""

def normalize_tn_phone(s: str) -> str:
    digits = "".join(ch for ch in str(s) if ch.isdigit())
    if digits.startswith("216"): return digits
    if len(digits) == 8: return "216" + digits
    return digits

def format_display_phone(s: str) -> str:
    d = "".join(ch for ch in str(s) if s is not None and ch.isdigit())
    return f"+{d}" if d else ""

def color_tag(val):
    if isinstance(val, str) and val.strip().startswith("#") and len(val.strip()) == 7:
        return f"background-color:{val};color:white;"
    return ""

def mark_alert_cell(val: str):
    s = str(val).strip()
    if not s: return ""
    if "متأخر" in s: return "background-color:#ffe6b3;color:#7a4e00"
    return "background-color:#ffcccc;color:#7a0000"

def highlight_inscrit_row(row: pd.Series):
    insc = str(row.get("Inscription","")).strip().lower()
    return ['background-color:#d6f5e8' if insc in ("inscrit","oui") else '' for _ in row.index]

def fin_month_title(mois: str, kind: str, branch: str):
    prefix = "Revenue " if kind == "Revenus" else "Dépense "
    short = "MB" if "Menzel" in branch else "BZ"
    return f"{prefix}{mois} ({short})"

def month_order_idx(mois: str) -> int:
    return FIN_MONTHS_FR.index(mois) if mois in FIN_MONTHS_FR else 0

# =============== Sheets ensure/open ===============
def ensure_sheet(title: str, headers: list[str]):
    sh = client.open_by_key(SPREADSHEET_ID)
    try:
        ws = sh.worksheet(title)
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title=title, rows="2000", cols=str(max(len(headers),8)))
        ws.update("1:1", [headers])
    rows = ws.get_all_values()
    if not rows:
        ws.update("1:1", [headers])
    return ws

def fin_ensure_ws(title: str, columns: list[str]):
    return ensure_sheet(title, columns)

# =============== Robust read ===============
def fin_read_df(title: str, kind: str) -> pd.DataFrame:
    cols = FIN_REV_COLUMNS if kind == "Revenus" else FIN_DEP_COLUMNS
    try:
        ws = fin_ensure_ws(title, cols)
        values = ws.get_all_values()
    except Exception as e:
        st.warning(f"⚠️ تعذّر قراءة الورقة: {title} — {e}")
        return pd.DataFrame(columns=cols)

    if not values:
        return pd.DataFrame(columns=cols)
    header = values[0] if values else []
    data   = values[1:] if len(values) > 1 else []
    if not header: header = cols

    fixed = []
    for r in data:
        row = list(r)
        if len(row) < len(header): row += [""]*(len(header)-len(row))
        else: row = row[:len(header)]
        fixed.append(row)

    df = pd.DataFrame(fixed, columns=header)
    for c in cols:
        if c not in df.columns: df[c] = ""

    if "Date" in df.columns:
        df["Date"] = pd.to_datetime(df["Date"], errors="coerce", dayfirst=True)

    if kind == "Revenus":
        if "Echeance" in df.columns:
            df["Echeance"] = pd.to_datetime(df["Echeance"], errors="coerce", dayfirst=True)
        for c in ["Prix","Montant_Admin","Montant_Structure","Montant_PreInscription","Montant_Total","Reste"]:
            df[c] = _to_num_series(df[c])
        if "Alert" not in df.columns: df["Alert"] = ""
        if "Echeance" in df.columns and "Reste" in df.columns:
            today_ts = pd.Timestamp.now().normalize()
            ech = pd.to_datetime(df["Echeance"], errors="coerce")
            reste = pd.to_numeric(df["Reste"], errors="coerce").fillna(0.0)
            df.loc[ech.notna() & (ech < today_ts) & (reste > 0), "Alert"] = "⚠️ متأخر"
            df.loc[ech.notna() & (ech.dt.normalize() == today_ts) & (reste > 0), "Alert"] = "⏰ اليوم"
        return safe_unique_columns(df[FIN_REV_COLUMNS])
    else:
        df["Montant"] = _to_num_series(df["Montant"])
        return safe_unique_columns(df[FIN_DEP_COLUMNS])

def fin_append_row(title: str, row: dict, kind: str):
    cols = FIN_REV_COLUMNS if kind=="Revenus" else FIN_DEP_COLUMNS
    ws = fin_ensure_ws(title, cols)
    header = ws.row_values(1)
    vals = [str(row.get(col, "")) for col in header]
    ws.append_row(vals)

def fin_update_row(title: str, row_index: int, updates: dict, kind: str):
    cols = FIN_REV_COLUMNS if kind=="Revenus" else FIN_DEP_COLUMNS
    ws = fin_ensure_ws(title, cols)
    header = ws.row_values(1)
    # Google Sheets: 1-based rows/cols
    for k, v in updates.items():
        if k in header:
            ws.update_cell(row_index, header.index(k) + 1, str(v))

# =============== InterNotes ===============
def inter_notes_open_ws():
    return ensure_sheet(INTER_NOTES_SHEET, INTER_NOTES_HEADERS)

def inter_notes_append(sender: str, receiver: str, message: str):
    if not message.strip(): return False, "النص فارغ"
    ws = inter_notes_open_ws()
    ts = datetime.now(timezone.utc).isoformat()
    note_id = str(uuid.uuid4())
    ws.append_row([ts, sender, receiver, message.strip(), "unread", note_id])
    return True, note_id

def inter_notes_fetch_all_df():
    ws = inter_notes_open_ws()
    values = ws.get_all_values()
    if not values or len(values) <= 1:
        return pd.DataFrame(columns=INTER_NOTES_HEADERS)
    df = pd.DataFrame(values[1:], columns=values[0])
    for c in INTER_NOTES_HEADERS:
        if c not in df.columns: df[c] = ""
    return df

def inter_notes_fetch_unread(receiver: str):
    df = inter_notes_fetch_all_df()
    return df[(df["receiver"] == receiver) & (df["status"] == "unread")].copy()

def inter_notes_mark_read(note_ids: list[str]):
    if not note_ids: return
    ws = inter_notes_open_ws()
    values = ws.get_all_values()
    if not values or len(values) <= 1: return
    header = values[0]; idx_note = header.index("note_id"); idx_status = header.index("status")
    for r, row in enumerate(values[1:], start=2):
        if len(row) > idx_note and row[idx_note] in note_ids:
            ws.update_cell(r, idx_status + 1, "read")

# =============== Transfers Log ===============
def transfers_ws():
    return ensure_sheet(TRANSFERS_SHEET, TRANSFERS_HEADERS)

def log_transfer(by_user: str, src: str, dst: str, client_name: str, phone_norm: str):
    ws = transfers_ws()
    ts = datetime.now(timezone.utc).isoformat()
    ws.append_row([ts, by_user, src, dst, client_name, phone_norm])

# =============== Cache: load all CRM data ===============
@st.cache_data(ttl=600)
def load_all_data():
    sh = client.open_by_key(SPREADSHEET_ID)
    worksheets = sh.worksheets()
    all_dfs, all_employes = [], []
    for ws in worksheets:
        title = ws.title.strip()
        if title.endswith("_PAIEMENTS"): continue
        if title.startswith("_"): continue
        if title.startswith("Revenue ") or title.startswith("Dépense "): continue
        if title == INTER_NOTES_SHEET: continue
        all_employes.append(title)
        rows = ws.get_all_values()
        if not rows:
            ws.update("1:1", [EXPECTED_HEADERS]); rows = ws.get_all_values()
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

# =============== Sidebar ===============
try:
    st.sidebar.image(Image.open("logo.png"), use_container_width=True)
except Exception:
    pass

tab_choice = st.sidebar.radio("📑 اختر تبويب:", ["CRM", "مداخيل (MB/Bizerte)", "📝 نوط داخلية"], index=0)
role = st.sidebar.radio("الدور", ["موظف", "أدمن"], horizontal=True)
employee = None
if role == "موظف":
    employee = st.sidebar.selectbox("👨‍💼 الموظّف (ورقة)", all_employes) if all_employes else None

# =============== Admin lock ===============
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
                    st.success("تم فتح صفحة الأدمِن لمدّة 30 دقيقة.")
                else:
                    st.error("كلمة سرّ غير صحيحة.")

if role == "أدمن":
    admin_lock_ui()

# =============== Branch PWs ===============
def _branch_passwords():
    try:
        b = st.secrets["branch_passwords"]
        return {"Menzel Bourguiba": str(b.get("MB", "MB_2025!")), "Bizerte": str(b.get("BZ", "BZ_2025!"))}
    except Exception:
        return {"Menzel Bourguiba": "MB_2025!", "Bizerte": "BZ_2025!"}

# =============== Finance Tab ===============
if tab_choice == "مداخيل (MB/Bizerte)":
    st.title("💸 المداخيل والمصاريف — (منزل بورقيبة & بنزرت)")
    with st.sidebar:
        st.markdown("---")
        st.subheader("🔧 إعدادات المداخيل/المصاريف")
        branch = st.selectbox("الفرع", ["Menzel Bourguiba", "Bizerte"], key="fin_branch")
        kind_ar = st.radio("النوع", ["مداخيل","مصاريف"], horizontal=True, key="fin_kind_ar")
        kind = "Revenus" if kind_ar == "مداخيل" else "Dépenses"
        mois   = st.selectbox("الشهر", FIN_MONTHS_FR, index=datetime.now().month-1, key="fin_month")

        BRANCH_PASSWORDS = _branch_passwords()
        key_pw = f"finance_pw_ok::{branch}"
        if key_pw not in st.session_state: st.session_state[key_pw] = False
        if not st.session_state[key_pw]:
            pw_try = st.text_input("كلمة سرّ الفرع", type="password", key=f"fin_pw_{branch}")
            if st.button("دخول الفرع", key=f"fin_enter_{branch}"):
                if pw_try and pw_try == BRANCH_PASSWORDS.get(branch, ""):
                    st.session_state[key_pw] = True; st.success("تم الدخول ✅")
                else:
                    st.error("كلمة سرّ غير صحيحة ❌")

    if not st.session_state.get(f"finance_pw_ok::{branch}", False):
        st.info("⬅️ أدخل كلمة السرّ من اليسار للمتابعة."); st.stop()

    fin_title = fin_month_title(mois, kind, branch)
    df_fin = fin_read_df(fin_title, kind)
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
        if search and not df_view.empty:
            m = pd.Series([False]*len(df_view))
            for col in [c for c in ["Libellé","Catégorie","Mode","Employé","Note","Caisse_Source","Montant_PreInscription"] if c in df_view.columns]:
                m |= df_view[col].fillna("").astype(str).str.contains(search, case=False, na=False)
            df_view = df_view[m]

    st.subheader(f"📄 {fin_title}")
    df_view = safe_unique_columns(df_view)
    if kind == "Revenus":
        cols_show = [c for c in ["Date","Libellé","Prix","Montant_Admin","Montant_Structure","Montant_PreInscription","Montant_Total","Echeance","Reste","Alert","Mode","Employé","Catégorie","Note"] if c in df_view.columns]
    else:
        cols_show = [c for c in ["Date","Libellé","Montant","Caisse_Source","Mode","Employé","Catégorie","Note"] if c in df_view.columns]
    st.dataframe(df_view[cols_show] if not df_view.empty else pd.DataFrame(columns=cols_show), use_container_width=True)

    # ---------- ملخص شهري + cumulative Reste ----------
    rev_df_month = fin_read_df(fin_month_title(mois, "Revenus", branch), "Revenus")
    dep_df_month = fin_read_df(fin_month_title(mois, "Dépenses", branch), "Dépenses")

    sum_admin  = rev_df_month["Montant_Admin"].sum() if "Montant_Admin" in rev_df_month else 0.0
    sum_struct = rev_df_month["Montant_Structure"].sum() if "Montant_Structure" in rev_df_month else 0.0
    sum_preins = rev_df_month["Montant_PreInscription"].sum() if "Montant_PreInscription" in rev_df_month else 0.0
    sum_reste_month = rev_df_month["Reste"].sum() if "Reste" in rev_df_month else 0.0

    dep_admin = dep_df_month.loc[dep_df_month["Caisse_Source"]=="Caisse_Admin","Montant"].sum() if not dep_df_month.empty else 0.0
    dep_struct= dep_df_month.loc[dep_df_month["Caisse_Source"]=="Caisse_Structure","Montant"].sum() if not dep_df_month.empty else 0.0
    dep_inscr = dep_df_month.loc[dep_df_month["Caisse_Source"]=="Caisse_Inscription","Montant"].sum() if not dep_df_month.empty else 0.0

    # Reste cumulative: هذا الشهر + ما قبله
    m_idx = month_order_idx(mois)
    reste_cum = 0.0
    for i in range(0, m_idx+1):
        mname = FIN_MONTHS_FR[i]
        rdf = fin_read_df(fin_month_title(mname,"Revenus",branch), "Revenus")
        if "Reste" in rdf: reste_cum += float(rdf["Reste"].sum())

    st.markdown("### 📊 ملخّص الشهر")
    a1, a2, a3, a4 = st.columns(4)
    a1.metric("Admin (مداخيل)", f"{sum_admin:,.2f}")
    a2.metric("Structure (مداخيل)", f"{sum_struct:,.2f}")
    a3.metric("Pré-Inscription (مداخيل)", f"{sum_preins:,.2f}")
    a4.metric("مجموع المصاريف", f"{(dep_admin+dep_struct+dep_inscr):,.2f}")

    b1, b2, b3, b4 = st.columns(4)
    b1.metric("مصروف Admin", f"{dep_admin:,.2f}")
    b2.metric("مصروف Structure", f"{dep_struct:,.2f}")
    b3.metric("مصروف Inscription", f"{dep_inscr:,.2f}")
    b4.metric("Reste (هذا الشهر فقط)", f"{sum_reste_month:,.2f}")

    st.info(f"🧮 **Reste cumulative (هذا الشهر + الأشهر السابقة)**: **{reste_cum:,.2f}**")

    # ---------- إحصائيات شهرية (شهر بشهر) ----------
    st.markdown("## 📈 إحصائيات شهرية (شهر بشهر)")
    stats = []
    for mname in FIN_MONTHS_FR:
        rev_t = fin_month_title(mname,"Revenus",branch)
        dep_t = fin_month_title(mname,"Dépenses",branch)
        rdf = fin_read_df(rev_t,"Revenus")
        ddf = fin_read_df(dep_t,"Dépenses")
        stats.append({
            "Mois": mname,
            "Admin": float(rdf["Montant_Admin"].sum()) if "Montant_Admin" in rdf else 0.0,
            "Structure": float(rdf["Montant_Structure"].sum()) if "Montant_Structure" in rdf else 0.0,
            "Inscription": float(rdf["Montant_PreInscription"].sum()) if "Montant_PreInscription" in rdf else 0.0,
            "Dépenses": float(ddf["Montant"].sum()) if "Montant" in ddf else 0.0,
            "Reste": float(rdf["Reste"].sum()) if "Reste" in rdf else 0.0
        })
    df_stats = pd.DataFrame(stats)
    st.dataframe(df_stats, use_container_width=True)

    # ---------- إضافة/تحديث عملية ----------
    st.markdown("---")
    st.markdown("### ➕ إضافة عملية جديدة / تحديث")

    selected_client_info = None
    client_default_lib = ""
    emp_default = (employee or "")

    # اختيار عميل مُسجَّل
    reg_df = df_all.copy()
    if not reg_df.empty:
        reg_df["Inscription_norm"] = reg_df["Inscription"].fillna("").astype(str).str.strip().str.lower()
        reg_df = reg_df[reg_df["Inscription_norm"].isin(["oui","inscrit"])]
        if role == "موظف" and employee:
            reg_df = reg_df[reg_df["__sheet_name"] == employee]
    else:
        reg_df = pd.DataFrame(columns=df_all.columns)

    if kind == "Revenus":
        st.markdown("#### 👤 اربط/حدّث دفع عميل مُسجَّل")
        pick = None
        if not reg_df.empty:
            def _opt(row):
                phone = format_display_phone(row.get("Téléphone",""))
                return f"{row.get('Nom & Prénom','')} — {phone} — {row.get('Formation','')}  [{row.get('__sheet_name','')}]"
            options = [_opt(r) for _, r in reg_df.iterrows()]
            pick = st.selectbox("اختر عميلًا", ["— بدون اختيار —"] + options, key="fin_client_pick")
            if pick and pick != "— بدون اختيار —":
                idx = options.index(pick); row = reg_df.iloc[idx]
                selected_client_info = {
                    "name": str(row.get("Nom & Prénom","")).strip(),
                    "tel":  str(row.get("Téléphone","")).strip(),
                    "formation": str(row.get("Formation","")).strip(),
                    "emp": str(row.get("__sheet_name","")).strip()
                }
                client_default_lib = f"Paiement {selected_client_info['formation']} - {selected_client_info['name']}".strip()
                if not emp_default: emp_default = selected_client_info["emp"]

        with st.form("fin_add_or_update"):
            d1, d2, d3 = st.columns(3)
            date_val = d1.date_input("Date", value=datetime.today())
            libelle  = d2.text_input("Libellé", value=client_default_lib)
            employe  = d3.selectbox("Employé", all_employes if all_employes else [""],
                                    index=(all_employes.index(emp_default) if (emp_default in all_employes) else 0) if all_employes else 0)

            r1, r2, r3 = st.columns(3)
            prix            = r1.number_input("💰 Prix (سعر التكوين)", min_value=0.0, step=10.0)
            montant_admin   = r2.number_input("🏢 Montant Admin", min_value=0.0, step=10.0)
            montant_struct  = r3.number_input("🏫 Montant Structure", min_value=0.0, step=10.0)
            r4, r5 = st.columns(2)
            montant_preins  = r4.number_input("📝 Montant Pré-Inscription", min_value=0.0, step=10.0, help="اختياري")
            montant_total   = float(montant_admin) + float(montant_struct)
            e1, e2, e3 = st.columns(3)
            echeance   = e1.date_input("⏰ تاريخ الاستحقاق", value=date.today())
            mode       = e2.selectbox("Mode", ["Espèces","Virement","Carte","Chèque","Autre"])
            categorie  = e3.text_input("Catégorie", value="Revenus")
            note_default = f"Client: {selected_client_info['name']} / {selected_client_info['formation']}" if selected_client_info else ""
            note = st.text_area("Note", value=note_default)

            # اقتراح Reste: من نفس Libellé في نفس الشهر
            rev_current = fin_read_df(fin_title, "Revenus")
            paid_so_far = 0.0
            if not rev_current.empty:
                same = rev_current[rev_current["Libellé"].fillna("").str.strip().str.lower() == libelle.strip().lower()]
                paid_so_far = float(same["Montant_Total"].sum()) if not same.empty else 0.0
            reste_calc = max(float(prix) - (paid_so_far + float(montant_total)), 0.0)
            reste_input = st.number_input("💳 Reste", min_value=0.0, value=float(round(reste_calc,2)), step=10.0)

            st.caption(f"💡 Total(Admin+Structure): {montant_total:.2f} — مدفوع سابقًا لنفس Libellé: {paid_so_far:.2f} — Reste مقترح: {reste_calc:.2f} — Pré-Inscription: {montant_preins:.2f}")

            # زر حفظ (يحدّث إذا لقى صف بنفس Libellé)
            if st.form_submit_button("✅ حفظ / تحديث"):
                if not libelle.strip(): st.error("Libellé مطلوب."); st.stop()
                if prix <= 0: st.error("Prix مطلوب."); st.stop()
                if montant_total <= 0 and montant_preins <= 0:
                    st.error("المبلغ لازم > 0 (Admin/Structure أو Pré-Inscription)."); st.stop()

                # هل يوجد صف بنفس Libellé في هذا الشهر؟
                row_index = None
                if not rev_current.empty:
                    same = rev_current[rev_current["Libellé"].fillna("").str.strip().str.lower() == libelle.strip().lower()]
                    if not same.empty:
                        # خذ آخر صف مطابق
                        last_idx_in_df = same.index[-1]
                        # +2 لأن 1 للـ header و 1 لأن index DataFrame يبدأ من 0
                        row_index = int(last_idx_in_df) + 2

                if row_index:  # تحديث
                    fin_update_row(
                        fin_title, row_index,
                        {
                            "Date": fmt_date(date_val),
                            "Prix": f"{float(prix):.2f}",
                            "Montant_Admin": f"{float(montant_admin):.2f}",
                            "Montant_Structure": f"{float(montant_struct):.2f}",
                            "Montant_PreInscription": f"{float(montant_preins):.2f}",
                            "Montant_Total": f"{float(montant_total):.2f}",
                            "Echeance": fmt_date(echeance),
                            "Reste": f"{float(reste_input):.2f}",
                            "Mode": mode, "Employé": employe, "Catégorie": categorie, "Note": note
                        },
                        "Revenus"
                    )
                    st.success("تمّ **تحديث** الدفعة على نفس Libellé ✅")
                else:  # إضافة
                    fin_append_row(
                        fin_title,
                        {
                            "Date": fmt_date(date_val), "Libellé": libelle.strip(),
                            "Prix": f"{float(prix):.2f}",
                            "Montant_Admin": f"{float(montant_admin):.2f}",
                            "Montant_Structure": f"{float(montant_struct):.2f}",
                            "Montant_PreInscription": f"{float(montant_preins):.2f}",
                            "Montant_Total": f"{float(montant_total):.2f}",
                            "Echeance": fmt_date(echeance),
                            "Reste": f"{float(reste_input):.2f}",
                            "Mode": mode, "Employé": employe, "Catégorie": categorie, "Note": note
                        },
                        "Revenus"
                    )
                    st.success("تمّت **إضافة** الدفعة ✅")
                st.cache_data.clear()
                st.rerun()

    else:
        # Dépenses form
        with st.form("fin_add_dep"):
            d1, d2, d3 = st.columns(3)
            date_val = d1.date_input("Date", value=datetime.today())
            libelle  = d2.text_input("Libellé")
            employe  = d3.selectbox("Employé", all_employes if all_employes else [""])
            r1, r2, r3 = st.columns(3)
            montant   = r1.number_input("Montant", min_value=0.0, step=10.0)
            caisse    = r2.selectbox("Caisse_Source", ["Caisse_Admin","Caisse_Structure","Caisse_Inscription"])
            mode      = r3.selectbox("Mode", ["Espèces","Virement","Carte","Chèque","Autre"])
            c2, c3 = st.columns(2)
            categorie = c2.text_input("Catégorie", value="Achat")
            note      = c3.text_area("Note (اختياري)")
            if st.form_submit_button("✅ حفظ العملية"):
                if not libelle.strip(): st.error("Libellé مطلوب."); st.stop()
                if montant <= 0: st.error("المبلغ لازم > 0."); st.stop()
                fin_append_row(
                    fin_title,
                    {
                        "Date": fmt_date(date_val), "Libellé": libelle.strip(),
                        "Montant": f"{float(montant):.2f}",
                        "Caisse_Source": caisse, "Mode": mode,
                        "Employé": employe.strip(), "Catégorie": categorie.strip(), "Note": note.strip(),
                    },
                    "Dépenses"
                )
                st.success("تمّ الحفظ ✅"); st.cache_data.clear(); st.rerun()

# =============== CRM مشتقات/تنبيهات ===============
df_all = df_all.copy()
if not df_all.empty:
    df_all["DateAjout_dt"] = pd.to_datetime(df_all["Date ajout"], dayfirst=True, errors="coerce")
    df_all["DateSuivi_dt"] = pd.to_datetime(df_all["Date de suivi"], dayfirst=True, errors="coerce")
    df_all["Mois"] = df_all["DateAjout_dt"].dt.strftime("%m-%Y")
    today = datetime.now().date()
    base_alert = df_all["Alerte"].fillna("").astype(str).str.strip()
    dsv_date = df_all["DateSuivi_dt"].dt.date
    due_today = dsv_date.eq(today).fillna(False)
    overdue  = dsv_date.lt(today).fillna(False)
    df_all["Alerte_view"] = base_alert
    df_all.loc[base_alert.eq("") & overdue, "Alerte_view"] = "⚠️ متابعة متأخرة"
    df_all.loc[base_alert.eq("") & due_today, "Alerte_view"] = "⏰ متابعة اليوم"
    df_all["Téléphone_norm"] = df_all["Téléphone"].apply(normalize_tn_phone)
    ALL_PHONES = set(df_all["Téléphone_norm"].dropna().astype(str))
    df_all["Inscription_norm"] = df_all["Inscription"].fillna("").astype(str).str.strip().str.lower()
    inscrit_mask = df_all["Inscription_norm"].isin(["oui","inscrit"])
    df_all.loc[inscrit_mask, "Date de suivi"] = ""
    df_all.loc[inscrit_mask, "Alerte_view"] = ""
else:
    df_all["Alerte_view"] = ""; df_all["Mois"] = ""; df_all["Téléphone_norm"] = ""; ALL_PHONES = set()

# =============== Dashboard سريع ===============
st.subheader("لوحة إحصائيات سريعة")
df_dash = df_all.copy()
if df_dash.empty:
    st.info("ما فماش داتا للعرض.")
else:
    df_dash["DateAjout_dt"] = pd.to_datetime(df_dash.get("Date ajout"), dayfirst=True, errors="coerce")
    df_dash["DateSuivi_dt"] = pd.to_datetime(df_dash.get("Date de suivi"), dayfirst=True, errors="coerce")
    today = datetime.now().date()
    df_dash["Inscription_norm"] = df_dash["Inscription"].fillna("").astype(str).str.strip().str.lower()
    df_dash["Alerte_norm"]      = df_dash["Alerte_view"].fillna("").astype(str).str.strip()
    added_today_mask      = df_dash["DateAjout_dt"].dt.date.eq(today)
    registered_today_mask = df_dash["Inscription_norm"].isin(["oui","inscrit"]) & added_today_mask
    alert_now_mask        = df_dash["Alerte_norm"].ne("")
    total_clients    = int(len(df_dash))
    added_today      = int(added_today_mask.sum())
    registered_today = int(registered_today_mask.sum())
    alerts_now       = int(alert_now_mask.sum())
    registered_total = int((df_dash["Inscription_norm"] == "oui").sum())
    rate = round((registered_total / total_clients) * 100, 2) if total_clients else 0.0

    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("👥 إجمالي العملاء", f"{total_clients}")
    c2.metric("🆕 المضافون اليوم", f"{added_today}")
    c3.metric("✅ المسجّلون اليوم", f"{registered_today}")
    c4.metric("🚨 التنبيهات الحالية", f"{alerts_now}")
    c5.metric("📈 نسبة التسجيل الإجمالية", f"{rate}%")

# =============== قسم الموظّف: نقل + لوج + واتساب + تعديل ===============
if role == "موظف" and employee:
    st.markdown(f"## 📁 لوحة {employee}")
    df_emp = df_all[df_all["__sheet_name"] == employee].copy()

    # نقل عميل + لوج
    st.markdown("### 🔁 نقل عميل بين الموظفين (مع تسجيل العملية)")
    if all_employes:
        colRA, colRB = st.columns(2)
        with colRA: src_emp = st.selectbox("من موظّف", all_employes, key="reassign_src")
        with colRB: dst_emp = st.selectbox("إلى موظّف", [e for e in all_employes if e != src_emp], key="reassign_dst")
        df_src = df_all[df_all["__sheet_name"] == src_emp].copy()
        if df_src.empty:
            st.info("❕ لا يوجد عملاء عند هذا الموظّف.")
        else:
            pick = st.selectbox(
                "اختر العميل للنقل",
                [f"{r['Nom & Prénom']} — {format_display_phone(r['Téléphone'])}" for _, r in df_src.iterrows()],
                key="reassign_pick"
            )
            phone_pick = normalize_tn_phone(pick.split("—")[-1])
            if st.button("🚚 نقل الآن"):
                try:
                    sh = client.open_by_key(SPREADSHEET_ID)
                    ws_src, ws_dst = sh.worksheet(src_emp), sh.worksheet(dst_emp)
                    values = ws_src.get_all_values(); header = values[0] if values else []
                    row_idx = None
                    if "Téléphone" in header:
                        tel_idx = header.index("Téléphone")
                        for i, r in enumerate(values[1:], start=2):
                            if len(r) > tel_idx and normalize_tn_phone(r[tel_idx]) == phone_pick:
                                row_idx = i; break
                    if not row_idx:
                        st.error("❌ لم يتم العثور على هذا العميل.")
                    else:
                        row_values = ws_src.row_values(row_idx)
                        if len(row_values) < len(EXPECTED_HEADERS):
                            row_values += [""] * (len(EXPECTED_HEADERS) - len(row_values))
                        row_values = row_values[:len(EXPECTED_HEADERS)]
                        client_name = row_values[0]
                        row_values[EXPECTED_HEADERS.index("Employe")] = dst_emp
                        ws_dst.append_row(row_values); ws_src.delete_rows(row_idx)
                        # Log
                        log_transfer(by_user=employee or "Unknown", src=src_emp, dst=dst_emp, client_name=client_name, phone_norm=phone_pick)
                        st.success(f"✅ نقل ({client_name}) من {src_emp} إلى {dst_emp} وتمّ تسجيل العملية.")
                        st.cache_data.clear()
                except Exception as e:
                    st.error(f"❌ خطأ أثناء النقل: {e}")

    # عرض اللوج
    st.markdown("#### 🧾 سجلّ النقل")
    try:
        wslog = transfers_ws()
        logs = wslog.get_all_values()
        if logs and len(logs) > 1:
            df_log = pd.DataFrame(logs[1:], columns=logs[0])
            st.dataframe(df_log.sort_values("timestamp", ascending=False), use_container_width=True, height=200)
        else:
            st.caption("لا توجد عمليات نقل مسجلة بعد.")
    except Exception:
        st.caption("لا توجد عمليات نقل مسجلة بعد.")

    # واتساب
    st.markdown("### 💬 تواصل WhatsApp")
    if not df_emp.empty:
        wa_pick = st.selectbox("اختر العميل", [f"{r['Nom & Prénom']} — {format_display_phone(r['Téléphone'])}" for _, r in df_emp.iterrows()])
        default_msg = "سلام! معاك Mega Formation. بخصوص التكوين، نحبّوا ننسّقو معاك موعد المتابعة. 👍"
        wa_msg = st.text_area("نص الرسالة", value=default_msg)
        if st.button("📲 فتح WhatsApp"):
            raw_tel = wa_pick.split("—")[-1]
            tel_norm = normalize_tn_phone(raw_tel)
            url = f"https://wa.me/{tel_norm}?text={urllib.parse.quote(wa_msg)}"
            st.markdown(f"[افتح المحادثة الآن]({url})")
            st.info("اضغط على الرابط لفتح واتساب.")

# =============== 📝 نوط داخلية ===============
if tab_choice == "📝 نوط داخلية":
    current_emp_name = (employee if (role == "موظف" and employee) else "Admin")
    st.subheader("📝 النوط الداخلية")
    receivers = [e for e in all_employes if e != current_emp_name] if all_employes else []
    with st.expander("✍️ إرسال نوط لموظف آخر", expanded=True):
        receiver = st.selectbox("الموظّف المستلم", receivers)
        message = st.text_area("الملاحظة", placeholder="اكتب ملاحظة قصيرة...")
        if st.button("إرسال ✅"):
            ok, info = inter_notes_append(current_emp_name, receiver, message)
            st.success("تم الإرسال 👌") if ok else st.error(f"تعذّر الإرسال: {info}")

    unread_df = inter_notes_fetch_unread(current_emp_name)
    st.markdown(f"### 📥 غير المقروء: **{len(unread_df)}**")
    if len(unread_df)==0:
        st.caption("ما فماش نوط غير مقروءة حاليا.")
    else:
        st.dataframe(unread_df[["timestamp","sender","message","note_id"]].sort_values("timestamp", ascending=False),
                     use_container_width=True, height=220)
        sel = st.multiselect("اختار رسائل لتعليمها كمقروء", options=unread_df["note_id"].tolist())
        if st.button("تعليم المحدد كمقروء"):
            inter_notes_mark_read(sel); st.success("تم التعليم كمقروء."); st.rerun()

    df_all_notes = inter_notes_fetch_all_df()
    mine = df_all_notes[(df_all_notes["receiver"] == current_emp_name) | (df_all_notes["sender"] == current_emp_name)].copy()
    st.markdown("### 🗂️ مراسلاتي")
    if mine.empty:
        st.caption("ما عندكش مراسلات.")
    else:
        st.dataframe(mine.sort_values("timestamp", ascending=False), use_container_width=True, height=260)

# =============== Admin Page ===============
if role == "أدمن":
    st.markdown("## 👑 لوحة الأدمِن")
    if not admin_unlocked():
        st.info("🔐 أدخل كلمة سرّ الأدمِن من اليسار لفتح الصفحة.")
    else:
        sh = client.open_by_key(SPREADSHEET_ID)
        colA, colB, colC = st.columns(3)

        with colA:
            st.subheader("➕ إضافة موظّف")
            new_emp = st.text_input("اسم الموظّف الجديد")
            if st.button("إنشاء ورقة"):
                try:
                    titles = [w.title for w in sh.worksheets()]
                    if not new_emp or new_emp in titles:
                        st.warning("⚠️ الاسم فارغ أو موجود.")
                    else:
                        sh.add_worksheet(title=new_emp, rows="1000", cols="20")
                        sh.worksheet(new_emp).update("1:1", [EXPECTED_HEADERS])
                        st.success("✔️ تم الإنشاء"); st.cache_data.clear()
                except Exception as e:
                    st.error(f"❌ خطأ: {e}")

        with colB:
            st.subheader("➕ إضافة عميل (لأي موظّف)")
            target_emp = st.selectbox("اختر الموظّف", all_employes, key="admin_add_emp")
            nom_a = st.text_input("👤 الاسم و اللقب", key="admin_nom")
            tel_a_raw = st.text_input("📞 الهاتف", key="admin_tel")
            formation_a = st.text_input("📚 التكوين", key="admin_form")
            type_contact_a = st.selectbox("نوع التواصل", ["Visiteur","Appel téléphonique","WhatsApp","Social media"], key="admin_type")
            inscription_a = st.selectbox("التسجيل", ["Pas encore","Inscrit"], key="admin_insc")
            date_ajout_a = st.date_input("تاريخ الإضافة", value=date.today(), key="admin_dt_add")
            suivi_date_a = st.date_input("تاريخ المتابعة", value=date.today(), key="admin_dt_suivi")
            if st.button("📥 أضف"):
                try:
                    if not (nom_a and tel_a_raw and formation_a and target_emp): st.error("❌ حقول ناقصة."); st.stop()
                    tel_a = normalize_tn_phone(tel_a_raw)
                    if tel_a in set(df_all["Téléphone_norm"]): st.warning("⚠️ الرقم موجود.")
                    else:
                        insc_val = "Oui" if inscription_a=="Inscrit" else "Pas encore"
                        ws = sh.worksheet(target_emp)
                        ws.append_row([nom_a, tel_a, type_contact_a, formation_a, "", fmt_date(date_ajout_a), fmt_date(suivi_date_a), "", insc_val, target_emp, ""])
                        st.success("✅ تمت الإضافة"); st.cache_data.clear()
                except Exception as e:
                    st.error(f"❌ خطأ: {e}")

        with colC:
            st.subheader("🗑️ حذف موظّف")
            if all_employes:
                emp_to_delete = st.selectbox("اختر الموظّف", all_employes, key="admin_del_emp")
                if st.button("❗ حذف الورقة كاملة"):
                    try:
                        sh.del_worksheet(sh.worksheet(emp_to_delete))
                        st.success("تم الحذف"); st.cache_data.clear()
                    except Exception as e:
                        st.error(f"❌ خطأ: {e}")

        st.caption("صفحة الأدمِن مفتوحة لمدّة 30 دقيقة من وقت الفتح.")
