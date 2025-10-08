# MegaCRM_Streamlit.py
# CRM + Finance (MB/Bizerte) — نسخة مُحسّنة مع Backoff/Cache + كل الميزات المطلوبة

import json, urllib.parse, time
import streamlit as st
import pandas as pd
import gspread
import gspread.exceptions as gse
from google.oauth2.service_account import Credentials
from datetime import datetime, date, timedelta, timezone
from PIL import Image

# ================= Page & Header =================
st.set_page_config(page_title="MegaCRM", layout="wide", initial_sidebar_state="expanded")
st.markdown(
    """
    <div style='text-align:center'>
      <h1>📊 CRM MEGA FORMATION - إدارة العملاء</h1>
    </div>
    <hr/>
    """,
    unsafe_allow_html=True
)

# ================= Google Auth =================
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
        sheet_id = "PUT_YOUR_SHEET_ID_HERE"  # غيّرها إذا تحتاج
        return client, sheet_id

client, SPREADSHEET_ID = make_client_and_sheet_id()

# ================= Schemas =================
EXPECTED_HEADERS = [
    "Nom & Prénom","Téléphone","Type de contact","Formation",
    "Remarque","Date ajout","Date de suivi","Alerte",
    "Inscription","Employe","Tag"
]

FIN_MONTHS_FR = ["Janvier","Février","Mars","Avril","Mai","Juin","Juillet","Aout","Septembre","Octobre","Novembre","Décembre"]

FIN_REV_COLUMNS = [
    "Date","Libellé","Prix",
    "Montant_Admin","Montant_Structure","Montant_PreInscription","Montant_Total",
    "Echeance","Reste",
    "Mode","Employé","Catégorie","Note"
]
FIN_DEP_COLUMNS = ["Date","Libellé","Montant","Caisse_Source","Mode","Employé","Catégorie","Note"]

REASSIGN_LOG_SHEET   = "Reassign_Log"
REASSIGN_LOG_HEADERS = ["timestamp","moved_by","src_employee","dst_employee","client_name","phone"]

# ================= Helpers =================
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

def _branch_passwords():
    try:
        b = st.secrets["branch_passwords"]
        return {"Menzel Bourguiba": str(b.get("MB","MB_2025!")), "Bizerte": str(b.get("BZ","BZ_2025!"))}
    except Exception:
        return {"Menzel Bourguiba":"MB_2025!","Bizerte":"BZ_2025!"}

def fin_month_title(mois: str, kind: str, branch: str):
    prefix = "Revenue " if kind=="Revenus" else "Dépense "
    short  = "MB" if "Menzel" in branch else "BZ"
    return f"{prefix}{mois} ({short})"

def _to_num_series_any(s):
    return (
        pd.Series(s).astype(str)
        .str.replace(" ","",regex=False)
        .str.replace(",",".",regex=False)
        .pipe(pd.to_numeric, errors="coerce")
        .fillna(0.0)
    )

# ===== Sheets Utils (Backoff + Cache) =====
def get_spreadsheet():
    """افتح الـ Spreadsheet مرة واحدة مع backoff، وأعد استعماله من session_state."""
    if st.session_state.get("sh_id") == SPREADSHEET_ID and "sh_obj" in st.session_state:
        return st.session_state["sh_obj"]
    last_err = None
    for i in range(5):  # 5 محاولات مع backoff أُسّي
        try:
            sh = client.open_by_key(SPREADSHEET_ID)
            st.session_state["sh_obj"] = sh
            st.session_state["sh_id"]  = SPREADSHEET_ID
            return sh
        except gse.APIError as e:
            last_err = e
            time.sleep(0.5 * (2**i))  # 0.5s,1s,2s,4s,8s
    st.error("تعذر فتح Google Sheet (ربما الكوتا تعدّت). خفّض عدد القراءات أو أعد المحاولة لاحقًا.")
    raise last_err

def ensure_ws(title: str, columns: list[str]):
    sh = get_spreadsheet()
    try:
        ws = sh.worksheet(title)
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title=title, rows="2000", cols=str(max(len(columns), 8)))
        ws.update("1:1", [columns])
        return ws
    header = ws.row_values(1)  # نقرأ صف الهيدر فقط
    if not header or header[:len(columns)] != columns:
        ws.update("1:1", [columns])
    return ws

@st.cache_data(ttl=120, show_spinner=False)
def _read_ws_all_values_cached(title: str, kind: str, cols: tuple) -> list[list[str]]:
    ws = ensure_ws(title, list(cols))
    return ws.get_all_values()

def fin_read_df(title: str, kind: str) -> pd.DataFrame:
    """قراءات مالية (Revenus/Dépenses) مع توحيد الأنواع + Alert. (مع كاش 120 ثانية)"""
    cols = FIN_REV_COLUMNS if kind == "Revenus" else FIN_DEP_COLUMNS
    values = _read_ws_all_values_cached(title, kind, tuple(cols))
    if not values:
        return pd.DataFrame(columns=cols)

    df = pd.DataFrame(values[1:], columns=values[0] if values else cols)

    # Dates
    if "Date" in df.columns:
        df["Date"] = pd.to_datetime(df["Date"], errors="coerce", dayfirst=True)
    if kind == "Revenus" and "Echeance" in df.columns:
        df["Echeance"] = pd.to_datetime(df["Echeance"], errors="coerce", dayfirst=True)

    # Numbers
    if kind == "Revenus":
        for c in ["Prix","Montant_Admin","Montant_Structure","Montant_PreInscription","Montant_Total","Reste"]:
            if c in df.columns: df[c] = _to_num_series_any(df[c])
        if "Alert" not in df.columns:
            df["Alert"] = ""
        if "Echeance" in df.columns and "Reste" in df.columns:
            today_ts = pd.Timestamp.now().normalize()
            ech = pd.to_datetime(df["Echeance"], errors="coerce")
            reste = pd.to_numeric(df["Reste"], errors="coerce").fillna(0.0)
            df.loc[ech.notna() & (ech < today_ts) & (reste > 0), "Alert"] = "⚠️ متأخر"
            df.loc[ech.notna() & (ech.dt.normalize() == today_ts) & (reste > 0), "Alert"] = "⏰ اليوم"
    else:
        if "Montant" in df.columns:
            df["Montant"] = _to_num_series_any(df["Montant"])
        # احذف الأعمدة المكرّرة إن وجدت (نحتفظ بالأول فقط)
    df = df.loc[:, ~df.columns.duplicated()]
    return df


def fin_append_row(title: str, row: dict, kind: str):
    cols = FIN_REV_COLUMNS if kind=="Revenus" else FIN_DEP_COLUMNS
    ws = ensure_ws(title, cols)
    header = ws.row_values(1)
    vals = [str(row.get(col, "")) for col in header]
    ws.append_row(vals)
    _read_ws_all_values_cached.clear()

# ============ Load all employee sheets ============
@st.cache_data(ttl=600)
def load_all_data():
    sh = get_spreadsheet()
    all_dfs, all_emps = [], []
    for ws in sh.worksheets():
        title = ws.title.strip()
        # إخفاء الأوراق غير الخاصة بالموظفين:
        if title.endswith("_PAIEMENTS"): continue
        if title.startswith("_"): continue
        if title.startswith("Revenue ") or title.startswith("Dépense "): continue
        if title in (REASSIGN_LOG_SHEET,): continue

        all_emps.append(title)
        rows = ws.get_all_values()
        if not rows:
            ws.update("1:1",[EXPECTED_HEADERS]); rows = ws.get_all_values()
        data_rows = rows[1:] if len(rows)>1 else []
        fixed = []
        for r in data_rows:
            r = list(r or [])
            if len(r)<len(EXPECTED_HEADERS): r += [""]*(len(EXPECTED_HEADERS)-len(r))
            else: r = r[:len(EXPECTED_HEADERS)]
            fixed.append(r)
        df = pd.DataFrame(fixed, columns=EXPECTED_HEADERS)
        df["__sheet_name"] = title
        all_dfs.append(df)
    big = pd.concat(all_dfs, ignore_index=True) if all_dfs else pd.DataFrame(columns=EXPECTED_HEADERS+["__sheet_name"])
    return big, all_emps

df_all, all_employes = load_all_data()

# ================= Sidebar =================
try:
    st.sidebar.image(Image.open("logo.png"), use_container_width=True)
except Exception:
    pass

tab_choice = st.sidebar.radio("📑 اختر تبويب:", ["CRM", "مداخيل (MB/Bizerte)"], index=0)
role = st.sidebar.radio("الدور", ["موظف","أدمن"], horizontal=True)
employee = st.sidebar.selectbox("👨‍💼 اختر الموظّف (ورقة Google Sheets)", all_employes) if (role=="موظف" and all_employes) else None

# ================= Locks =================
def admin_unlocked() -> bool:
    ok = st.session_state.get("admin_ok", False)
    ts = st.session_state.get("admin_ok_at")
    return bool(ok and ts and (datetime.now()-ts)<=timedelta(minutes=30))

def admin_lock_ui():
    with st.sidebar.expander("🔐 إدارة (Admin)", expanded=(role=="أدمن" and not admin_unlocked())):
        if admin_unlocked():
            if st.button("قفل صفحة الأدمِن"):
                st.session_state["admin_ok"]=False; st.session_state["admin_ok_at"]=None; st.rerun()
        else:
            admin_pwd = st.text_input("كلمة سرّ الأدمِن", type="password")
            if st.button("فتح صفحة الأدمِن"):
                conf = str(st.secrets.get("admin_password","admin123"))
                if admin_pwd and admin_pwd==conf:
                    st.session_state["admin_ok"]=True; st.session_state["admin_ok_at"]=datetime.now()
                    st.success("تم فتح صفحة الأدمِن لمدة 30 دقيقة.")
                else:
                    st.error("كلمة سرّ غير صحيحة.")

if role=="أدمن": admin_lock_ui()

def emp_pwd_for(emp_name:str)->str:
    try:
        mp = st.secrets["employee_passwords"]
        return str(mp.get(emp_name, mp.get("_default","1234")))
    except Exception: return "1234"

def emp_unlocked(emp_name:str)->bool:
    ok = st.session_state.get(f"emp_ok::{emp_name}", False)
    ts = st.session_state.get(f"emp_ok_at::{emp_name}")
    return bool(ok and ts and (datetime.now()-ts)<=timedelta(minutes=15))

def emp_lock_ui(emp_name:str):
    with st.expander(f"🔐 حماية ورقة الموظّف: {emp_name}", expanded=not emp_unlocked(emp_name)):
        if emp_unlocked(emp_name):
            c1, c2 = st.columns(2)
            c1.success("مفتوح (15 دقيقة).")
            if c2.button("قفل الآن"):
                st.session_state[f"emp_ok::{emp_name}"]=False
                st.session_state[f"emp_ok_at::{emp_name}"]=None
        else:
            pwd_try = st.text_input("أدخل كلمة السرّ", type="password", key=f"emp_pwd_{emp_name}")
            if st.button("فتح", key=f"emp_open_{emp_name}"):
                if pwd_try==emp_pwd_for(emp_name):
                    st.session_state[f"emp_ok::{emp_name}"]=True
                    st.session_state[f"emp_ok_at::{emp_name}"]=datetime.now()
                    st.success("تم الفتح لمدة 15 دقيقة.")
                else: st.error("كلمة سرّ غير صحيحة.")

# ================= Global derived =================
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
    df_all["Alerte_view"] = ""; df_all["Mois"] = ""; df_all["Téléphone_norm"] = ""; ALL_PHONES=set()

# ================= Finance Tab =================
if tab_choice=="مداخيل (MB/Bizerte)":
    st.header("💸 المداخيل والمصاريف — (منزل بورقيبة & بنزرت)")

    with st.sidebar:
        st.markdown("---"); st.subheader("🔧 إعدادات المداخيل/المصاريف")
        branch  = st.selectbox("الفرع", ["Menzel Bourguiba","Bizerte"], key="fin_branch")
        kind_ar = st.radio("النوع", ["مداخيل","مصاريف"], horizontal=True, key="fin_kind_ar")
        kind    = "Revenus" if kind_ar=="مداخيل" else "Dépenses"
        mois    = st.selectbox("الشهر", FIN_MONTHS_FR, index=datetime.now().month-1, key="fin_month")
        BRANCH_PASSWORDS = _branch_passwords()
        key_pw = f"finance_pw_ok::{branch}"
        if key_pw not in st.session_state: st.session_state[key_pw]=False
        if not st.session_state[key_pw]:
            pw_try = st.text_input("كلمة سرّ الفرع", type="password", key=f"fin_pw_{branch}")
            if st.button("دخول الفرع", key=f"fin_enter_{branch}"):
                if pw_try == BRANCH_PASSWORDS.get(branch,""): st.session_state[key_pw]=True; st.success("تم الدخول ✅")
                else: st.error("كلمة سرّ غير صحيحة ❌")

    if not st.session_state.get(f"finance_pw_ok::{branch}", False):
        st.info("⬅️ أدخل كلمة السرّ من اليسار للمتابعة."); st.stop()

    fin_title = fin_month_title(mois, kind, branch)
    df_fin = fin_read_df(fin_title, kind)
    df_view = df_fin.copy()

    if role=="موظف" and employee and "Employé" in df_view.columns:
        df_view = df_view[df_view["Employé"].fillna("").str.strip().str.lower() == employee.strip().lower()]

    with st.expander("🔎 فلاتر"):
        c1,c2,c3 = st.columns(3)
        date_from = c1.date_input("من تاريخ", value=None)
        date_to   = c2.date_input("إلى تاريخ", value=None)
        search    = c3.text_input("بحث (Libellé/Catégorie/Mode/Note)")
        if "Date" in df_view.columns:
            if date_from: df_view = df_view[df_view["Date"] >= pd.to_datetime(date_from)]
            if date_to:   df_view = df_view[df_view["Date"] <= pd.to_datetime(date_to)]
        if search and not df_view.empty:
            m = pd.Series([False]*len(df_view))
            cols_search = ["Libellé","Catégorie","Mode","Employé","Note","Caisse_Source","Montant_PreInscription"]
            cols_search = [c for c in cols_search if c in df_view.columns]
            for c in cols_search:
                m |= df_view[c].fillna("").astype(str).str.contains(search, case=False, na=False)
            df_view = df_view[m]

    st.subheader(f"📄 {fin_title}")
    if kind=="Revenus":
        cols_show = [c for c in ["Date","Libellé","Prix","Montant_Admin","Montant_Structure","Montant_PreInscription","Montant_Total","Echeance","Reste","Alert","Mode","Employé","Catégorie","Note"] if c in df_view.columns]
    else:
        cols_show = [c for c in ["Date","Libellé","Montant","Caisse_Source","Mode","Employé","Catégorie","Note"] if c in df_view.columns]
    st.dataframe(df_view[cols_show] if not df_view.empty else pd.DataFrame(columns=cols_show), use_container_width=True)

    # ---- ملخص شهري (Admin فقط) ----
    if role=="أدمن" and admin_unlocked():
        with st.expander("📊 ملخّص الفرع للشهر (حسب الصنف) — Admin Only"):
            rev_df = fin_read_df(fin_month_title(mois,"Revenus",branch), "Revenus")
            dep_df = fin_read_df(fin_month_title(mois,"Dépenses",branch), "Dépenses")
            sum_admin  = rev_df["Montant_Admin"].sum() if ("Montant_Admin" in rev_df) else 0.0
            sum_struct = rev_df["Montant_Structure"].sum() if ("Montant_Structure" in rev_df) else 0.0
            sum_preins = rev_df["Montant_PreInscription"].sum() if ("Montant_PreInscription" in rev_df) else 0.0
            sum_total_as = rev_df["Montant_Total"].sum() if ("Montant_Total" in rev_df) else (sum_admin+sum_struct)
            sum_reste_due= rev_df["Reste"].sum() if ("Reste" in rev_df) else 0.0
            if not dep_df.empty and "Caisse_Source" in dep_df and "Montant" in dep_df:
                dep_admin  = dep_df.loc[dep_df["Caisse_Source"]=="Caisse_Admin","Montant"].sum()
                dep_struct = dep_df.loc[dep_df["Caisse_Source"]=="Caisse_Structure","Montant"].sum()
                dep_inscr  = dep_df.loc[dep_df["Caisse_Source"]=="Caisse_Inscription","Montant"].sum()
            else: dep_admin=dep_struct=dep_inscr=0.0
            reste_admin  = float(sum_admin)  - float(dep_admin)
            reste_struct = float(sum_struct) - float(dep_struct)
            reste_inscr  = float(sum_preins) - float(dep_inscr)
            a1,a2,a3 = st.columns(3)
            a1.metric("مداخيل Admin", f"{sum_admin:,.2f}")
            a2.metric("مصاريف Admin", f"{dep_admin:,.2f}")
            a3.metric("Reste Admin", f"{reste_admin:,.2f}")
            s1,s2,s3 = st.columns(3)
            s1.metric("مداخيل Structure", f"{sum_struct:,.2f}")
            s2.metric("مصاريف Structure", f"{dep_struct:,.2f}")
            s3.metric("Reste Structure", f"{reste_struct:,.2f}")
            i1,i2,i3 = st.columns(3)
            i1.metric("مداخيل Inscription", f"{sum_preins:,.2f}")
            i2.metric("مصاريف Inscription", f"{dep_inscr:,.2f}")
            i3.metric("Reste Inscription", f"{reste_inscr:,.2f}")
            x1,x2,x3 = st.columns(3)
            x1.metric("Total Admin+Structure", f"{sum_total_as:,.2f}")
            x2.metric("Total مصاريف", f"{(dep_admin+dep_struct+dep_inscr):,.2f}")
            x3.metric("إجمالي Reste Due", f"{sum_reste_due:,.2f}")

    # ---- إضافة عملية جديدة + ربط بعميل ----
    st.markdown("---"); st.subheader("➕ إضافة عملية جديدة")
    selected_client_info = None
    client_default_lib, client_default_emp = "", (employee or "")
    if kind=="Revenus":
        st.markdown("#### 👤 اربط الدفعة بعميل مُسجَّل (اختياري)")
        reg = df_all.copy()
        reg = reg[reg["Inscription_norm"].isin(["oui","inscrit"])]
        if role=="موظف" and employee: reg = reg[reg["__sheet_name"]==employee]
        options=[]; pick=None
        if not reg.empty:
            def _opt(r):
                ph = format_display_phone(r.get("Téléphone",""))
                return f"{r.get('Nom & Prénom','')} — {ph} — {r.get('Formation','')}  [{r.get('__sheet_name','')}]"
            options = [_opt(r) for _,r in reg.iterrows()]
            pick = st.selectbox("اختر عميلًا مُسجَّلًا", ["— بدون اختيار —"]+options)
        if pick and pick!="— بدون اختيار —":
            row = reg.iloc[options.index(pick)]
            selected_client_info = {
                "name": str(row.get("Nom & Prénom","")).strip(),
                "tel":  normalize_tn_phone(str(row.get("Téléphone","")).strip()),
                "formation": str(row.get("Formation","")).strip(),
                "emp": str(row.get("__sheet_name","")).strip()
            }
            client_default_lib = f"Paiement {selected_client_info['formation']} - {selected_client_info['name']}"
            if not client_default_emp: client_default_emp = selected_client_info["emp"]

            # === عرض الدفعات السابقة عبر كل الأشهر (آمن لو Note مكرّر/مفقود) ===
            out = []
            sh_titles = [w.title for w in get_spreadsheet().worksheets()]
            months_available = [m for m in FIN_MONTHS_FR if fin_month_title(m, "Revenus", branch) in sh_titles]
            for m in months_available:
                t = fin_month_title(m, "Revenus", branch)
                try:
                    dfm = fin_read_df(t, "Revenus")
                except Exception:
                    dfm = pd.DataFrame(columns=FIN_REV_COLUMNS)
                if dfm.empty: 
                    continue
                dfm = dfm.copy()

                # سلسلة Note آمنة
                if "Note" in dfm.columns:
                    tmp = dfm["Note"]
                    if isinstance(tmp, pd.DataFrame):
                        note_series = tmp.iloc[:, 0].astype(str)
                    else:
                        note_series = tmp.astype(str)
                else:
                    note_series = pd.Series([""] * len(dfm), index=dfm.index, dtype=str)

                # Libellé + بحث بالهاتف داخل Note (بدون regex)
                lib_series = dfm["Libellé"].astype(str).str.strip().str.lower() if "Libellé" in dfm.columns else pd.Series([""]*len(dfm), index=dfm.index, dtype=str)
                cond_lib   = lib_series.eq(client_default_lib.strip().lower())
                cond_phone = note_series.str.contains(selected_client_info["tel"], na=False, regex=False)

                sub = dfm[cond_lib | cond_phone].copy()
                if not sub.empty:
                    sub["__mois"] = m
                    sub["__sheet_title"] = t
                    out.append(sub)

            prev_df = pd.concat(out, ignore_index=True) if out else pd.DataFrame(columns=FIN_REV_COLUMNS + ["__sheet_title","__mois"])
            st.markdown("#### 💾 دفعات سابقة (كل الأشهر)")
            if prev_df.empty:
                st.caption("لا توجد دفعات مسجّلة.")
                paid_so_far_all, last_reste = 0.0, 0.0
            else:
                show_cols = ["__mois","Date","Montant_Admin","Montant_Structure","Montant_PreInscription","Montant_Total","Reste","Mode","Employé","Catégorie","Note"]
                show_cols = [c for c in show_cols if c in prev_df.columns]
                safe_prev = prev_df.copy()
                safe_prev = safe_prev.loc[:, ~safe_prev.columns.duplicated()]  # نحذف الأعمدة المكررة
                show_cols = [c for c in show_cols if c in safe_prev.columns]   # نتأكد الأعمدة موجودة
                st.dataframe(safe_prev[show_cols], use_container_width=True)
                paid_so_far_all = float(prev_df.get("Montant_Total", pd.Series(dtype=float)).sum())
                last_reste = float(prev_df.get("Reste", pd.Series(dtype=float)).fillna(0).iloc[-1] if not prev_df.empty else 0.0)
            st.info(f"🔎 المجموع السابق: {paid_so_far_all:,.2f} — آخر Reste: {last_reste:,.2f}")

    with st.form("fin_add_row"):
        d1,d2,d3 = st.columns(3)
        date_val = d1.date_input("Date", value=datetime.today())
        libelle  = d2.text_input("Libellé", value=(client_default_lib if kind=="Revenus" else ""))
        employe  = d3.text_input("Employé", value=(client_default_emp if kind=="Revenus" else (employee or "")))

        if kind=="Revenus":
            r1,r2,r3 = st.columns(3)
            prix = r1.number_input("💰 Prix (سعر التكوين)", min_value=0.0, step=10.0)
            m_admin  = r2.number_input("🏢 Montant Admin", min_value=0.0, step=10.0)
            m_struct = r3.number_input("🏫 Montant Structure", min_value=0.0, step=10.0)
            r4,r5 = st.columns(2)
            m_preins = r4.number_input("📝 Montant Pré-Inscription", min_value=0.0, step=10.0)
            m_total  = float(m_admin)+float(m_struct)
            e1,e2,e3 = st.columns(3)
            ech     = e1.date_input("⏰ تاريخ الاستحقاق", value=date.today())
            mode    = e2.selectbox("Mode", ["Espèces","Virement","Carte","Chèque","Autre"])
            cat     = e3.text_input("Catégorie", value="Revenus")
            note_default = f"ClientTel:{selected_client_info['tel']}" if selected_client_info else ""
            note = st.text_area("Note", value=note_default)

            # Reste بحسب نفس الشهر لنفس libellé
            cur = fin_read_df(fin_title, "Revenus")
            paid_so_far = 0.0
            if not cur.empty and "Libellé" in cur and "Montant_Total" in cur:
                same = cur[cur["Libellé"].fillna("").str.strip().str.lower() == libelle.strip().lower()]
                paid_so_far = float(same["Montant_Total"].sum()) if not same.empty else 0.0
            reste_after = max(float(prix) - (paid_so_far + float(m_total)), 0.0)

            st.caption(f"Total=(Admin+Structure): {m_total:.2f} — مدفوع سابقًا هذا الشهر: {paid_so_far:.2f} — Reste بعد الحفظ: {reste_after:.2f} — Pré-Inscr: {m_preins:.2f}")

            submitted_fin = st.form_submit_button("✅ حفظ العملية")
            if submitted_fin:
                if not libelle.strip(): st.error("Libellé مطلوب.")
                elif prix <= 0: st.error("Prix مطلوب.")
                elif m_total<=0 and m_preins<=0: st.error("المبلغ لازم > 0.")
                else:
                    fin_append_row(fin_title, {
                        "Date": fmt_date(date_val),
                        "Libellé": libelle.strip(),
                        "Prix": f"{float(prix):.2f}",
                        "Montant_Admin": f"{float(m_admin):.2f}",
                        "Montant_Structure": f"{float(m_struct):.2f}",
                        "Montant_PreInscription": f"{float(m_preins):.2f}",
                        "Montant_Total": f"{float(m_total):.2f}",
                        "Echeance": fmt_date(ech),
                        "Reste": f"{float(reste_after):.2f}",
                        "Mode": mode,
                        "Employé": employe.strip(),
                        "Catégorie": cat.strip(),
                        "Note": note.strip(),
                    }, "Revenus")
                    st.success("تمّ الحفظ ✅"); st.cache_data.clear(); st.rerun()

        else:
            r1,r2,r3 = st.columns(3)
            montant = r1.number_input("Montant", min_value=0.0, step=10.0)
            caisse  = r2.selectbox("Caisse_Source", ["Caisse_Admin","Caisse_Structure","Caisse_Inscription"])
            mode    = r3.selectbox("Mode", ["Espèces","Virement","Carte","Chèque","Autre"])
            c2,c3 = st.columns(2)
            cat  = c2.text_input("Catégorie", value="Achat")
            note = c3.text_area("Note (اختياري)")

            submitted_dep = st.form_submit_button("✅ حفظ العملية")
            if submitted_dep:
                if not libelle.strip(): st.error("Libellé مطلوب.")
                elif montant<=0: st.error("المبلغ لازم > 0.")
                else:
                    fin_append_row(fin_title, {
                        "Date": fmt_date(date_val),
                        "Libellé": libelle.strip(),
                        "Montant": f"{float(montant):.2f}",
                        "Caisse_Source": caisse,
                        "Mode": mode,
                        "Employé": employe.strip(),
                        "Catégorie": cat.strip(),
                        "Note": note.strip(),
                    }, "Dépenses")
                    st.success("تمّ الحفظ ✅"); st.cache_data.clear(); st.rerun()

    st.stop()

# ================= Quick Dashboard =================
st.subheader("لوحة إحصائيات سريعة")
df_dash = df_all.copy()
if df_dash.empty:
    st.info("ما فماش داتا للعرض.")
else:
    df_dash["DateAjout_dt"] = pd.to_datetime(df_dash["Date ajout"], dayfirst=True, errors="coerce")
    df_dash["DateSuivi_dt"] = pd.to_datetime(df_dash["Date de suivi"], dayfirst=True, errors="coerce")
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
    registered_total = int((df_dash["Inscription_norm"]=="oui").sum())
    rate = round((registered_total/total_clients)*100,2) if total_clients else 0.0
    c1,c2,c3,c4,c5 = st.columns(5)
    c1.metric("👥 إجمالي العملاء", f"{total_clients}")
    c2.metric("🆕 المضافون اليوم", f"{added_today}")
    c3.metric("✅ المسجّلون اليوم", f"{registered_today}")
    c4.metric("🚨 التنبيهات الحالية", f"{alerts_now}")
    c5.metric("📈 نسبة التسجيل الإجمالية", f"{rate}%")

# ================= Monthly Stats + per employee/formation =================
st.markdown("---"); st.subheader("📅 إحصائيات شهرية (العملاء)")
if not df_all.empty and "DateAjout_dt" in df_all.columns:
    df_all["MonthStr"] = df_all["DateAjout_dt"].dt.strftime("%Y-%m")
    months_avail = sorted(df_all["MonthStr"].dropna().unique(), reverse=True)
    month_pick = st.selectbox("اختر شهر", months_avail, index=0 if months_avail else None)
    if month_pick:
        df_month = df_all[df_all["MonthStr"]==month_pick].copy()
        total_clients_m = len(df_month)
        total_inscrits_m= int((df_month["Inscription_norm"]=="oui").sum())
        alerts_m        = int(df_month["Alerte_view"].fillna("").astype(str).str.strip().ne("").sum())
        rate_m = round((total_inscrits_m/total_clients_m)*100,2) if total_clients_m else 0.0
        c1,c2,c3,c4 = st.columns(4)
        c1.metric("👥 عملاء هذا الشهر", f"{total_clients_m}")
        c2.metric("✅ مسجّلون", f"{total_inscrits_m}")
        c3.metric("🚨 تنبيهات", f"{alerts_m}")
        c4.metric("📈 نسبة التسجيل", f"{rate_m}%")
        st.markdown("#### 👨‍💼 حسب الموظّف")
        grp_emp = (
            df_month.groupby("__sheet_name", dropna=False)
            .agg(Clients=("Nom & Prénom","count"),
                 Inscrits=("Inscription_norm",lambda x:(x=="oui").sum()),
                 Alerts=("Alerte_view",lambda x:(x.fillna("").astype(str).str.strip()!="").sum()))
            .reset_index().rename(columns={"__sheet_name":"الموظف"})
        )
        grp_emp["% تسجيل"]=((grp_emp["Inscrits"]/grp_emp["Clients"]).replace([float("inf"),float("nan")],0)*100).round(2)
        st.dataframe(grp_emp.sort_values(["Inscrits","Clients"], ascending=False), use_container_width=True)
        st.markdown("#### 📚 حسب التكوين")
        grp_form = (
            df_month.groupby("Formation", dropna=False)
            .agg(Clients=("Nom & Prénom","count"), Inscrits=("Inscription_norm",lambda x:(x=="oui").sum()))
            .reset_index().rename(columns={"Formation":"التكوين"})
        )
        grp_form["% تسجيل"]=((grp_form["Inscrits"]/grp_form["Clients"]).replace([float("inf"),float("nan")],0)*100).round(2)
        st.dataframe(grp_form.sort_values(["Inscrits","Clients"], ascending=False), use_container_width=True)

# ================= Global phone search =================
st.subheader("🔎 بحث عام برقم الهاتف")
global_phone = st.text_input("اكتب رقم الهاتف (8 أرقام محلية أو 216XXXXXXXX)")
if global_phone.strip():
    q = normalize_tn_phone(global_phone)
    sd = df_all.copy()
    sd["Téléphone_norm"]=sd["Téléphone"].apply(normalize_tn_phone)
    sd["Alerte"]=sd.get("Alerte_view","")
    sd = sd[sd["Téléphone_norm"]==q]
    if sd.empty:
        st.info("❕ ما لقيتش عميل بهذا الرقم.")
    else:
        disp = [c for c in EXPECTED_HEADERS if c in sd.columns]
        st.dataframe(
            sd[disp].style.apply(highlight_inscrit_row, axis=1).applymap(mark_alert_cell, subset=["Alerte"]),
            use_container_width=True
        )
        st.markdown("---")

# ================= Employee Area =================
if role=="موظف" and employee:
    emp_lock_ui(employee)
    if not emp_unlocked(employee):
        st.info("🔒 أدخل كلمة سرّ الموظّف لفتح الورقة."); st.stop()

    st.subheader(f"📁 لوحة {employee}")
    df_emp = df_all[df_all["__sheet_name"]==employee].copy()
    if df_emp.empty:
        st.warning("⚠️ لا يوجد أي عملاء بعد."); st.stop()

    df_emp["DateAjout_dt"] = pd.to_datetime(df_emp["Date ajout"], dayfirst=True, errors="coerce")
    df_emp = df_emp.dropna(subset=["DateAjout_dt"])
    df_emp["Mois"] = df_emp["DateAjout_dt"].dt.strftime("%m-%Y")
    month_options = sorted(df_emp["Mois"].dropna().unique(), reverse=True)
    month_filter  = st.selectbox("🗓️ اختر شهر الإضافة", month_options)
    filtered_df   = df_emp[df_emp["Mois"]==month_filter].copy()

    def render_table(df_disp: pd.DataFrame):
        if df_disp.empty: st.info("لا توجد بيانات."); return
        _df = df_disp.copy(); _df["Alerte"]=_df.get("Alerte_view","")
        styled = (_df[[c for c in EXPECTED_HEADERS if c in _df.columns]]
                  .style.apply(highlight_inscrit_row, axis=1)
                  .applymap(mark_alert_cell, subset=["Alerte"])
                  .applymap(color_tag, subset=["Tag"]))
        st.dataframe(styled, use_container_width=True)

    st.markdown("### 📋 قائمة العملاء"); render_table(filtered_df)

    # فلترة بالتنبيهات
    _df_alerts = filtered_df.copy(); _df_alerts["Alerte"]=_df_alerts.get("Alerte_view","")
    if st.checkbox("🔴 عرض العملاء الذين لديهم تنبيهات"):
        alerts_df = _df_alerts[_df_alerts["Alerte"].fillna("").astype(str).str.strip()!=""]
        st.markdown("### 🚨 عملاء مع تنبيهات")
        render_table(alerts_df)

    # تعديل عميل
    st.markdown("### ✏️ تعديل بيانات عميل")
    df_emp_edit = df_emp.copy(); df_emp_edit["Téléphone_norm"]=df_emp_edit["Téléphone"].apply(normalize_tn_phone)
    options = {
        f"[{i}] {r['Nom & Prénom']} — {format_display_phone(r['Téléphone_norm'])}": r["Téléphone_norm"]
        for i, r in df_emp_edit.iterrows() if str(r.get("Téléphone","")).strip()!=""
    }
    if options:
        chosen_key   = st.selectbox("اختر العميل (بالاسم/الهاتف)", list(options.keys()))
        chosen_phone = options[chosen_key]
        cur_row = df_emp_edit[df_emp_edit["Téléphone_norm"]==chosen_phone].iloc[0]

        with st.form("edit_client_form"):
            col1,col2 = st.columns(2)
            with col1:
                new_name      = st.text_input("👤 الاسم و اللقب", value=str(cur_row["Nom & Prénom"]))
                new_phone_raw = st.text_input("📞 رقم الهاتف", value=str(cur_row["Téléphone"]))
                new_formation = st.text_input("📚 التكوين", value=str(cur_row["Formation"]))
            with col2:
                new_ajout = st.date_input("🕓 تاريخ الإضافة", value=pd.to_datetime(cur_row["Date ajout"], dayfirst=True, errors="coerce").date())
                new_suivi = st.date_input("📆 تاريخ المتابعة", value=(pd.to_datetime(cur_row["Date de suivi"], dayfirst=True, errors="coerce").date() if str(cur_row["Date de suivi"]).strip() else date.today()))
                new_insc  = st.selectbox("🟢 التسجيل", ["Pas encore","Inscrit"], index=(1 if str(cur_row["Inscription"]).strip().lower()=="oui" else 0))
            new_remark_full = st.text_area("🗒️ ملاحظة (استبدال كامل)", value=str(cur_row.get("Remarque","")))
            extra_note      = st.text_area("➕ أضف ملاحظة جديدة (طابع زمني)", placeholder="اكتب ملاحظة لإلحاقها…")
            submitted = st.form_submit_button("💾 حفظ التعديلات")
        if submitted:
            try:
                ws = get_spreadsheet().worksheet(employee)
                values = ws.get_all_values(); header = values[0] if values else []
                tel_idx = header.index("Téléphone")
                row_idx=None
                for i,r in enumerate(values[1:], start=2):
                    if len(r)>tel_idx and normalize_tn_phone(r[tel_idx])==chosen_phone: row_idx=i; break
                if not row_idx: st.error("❌ تعذّر إيجاد الصف."); st.stop()
                col_map = {h:(EXPECTED_HEADERS.index(h)+1) for h in ["Nom & Prénom","Téléphone","Formation","Date ajout","Date de suivi","Inscription","Remarque"]}
                new_phone_norm = normalize_tn_phone(new_phone_raw)
                if not new_name.strip(): st.error("❌ الاسم مطلوب."); st.stop()
                if not new_phone_norm.strip(): st.error("❌ الهاتف مطلوب."); st.stop()
                phones_except = set(df_all["Téléphone_norm"]) - {normalize_tn_phone(chosen_phone)}
                if new_phone_norm in phones_except: st.error("⚠️ الرقم موجود مسبقًا."); st.stop()
                ws.update_cell(row_idx, col_map["Nom & Prénom"], new_name.strip())
                ws.update_cell(row_idx, col_map["Téléphone"],   new_phone_norm)
                ws.update_cell(row_idx, col_map["Formation"],   new_formation.strip())
                ws.update_cell(row_idx, col_map["Date ajout"],  fmt_date(new_ajout))
                ws.update_cell(row_idx, col_map["Date de suivi"], fmt_date(new_suivi))
                ws.update_cell(row_idx, col_map["Inscription"], "Oui" if new_insc=="Inscrit" else "Pas encore")
                if new_remark_full.strip() != str(cur_row.get("Remarque","")).strip():
                    ws.update_cell(row_idx, col_map["Remarque"], new_remark_full.strip())
                if extra_note.strip():
                    old_rem = ws.cell(row_idx, col_map["Remarque"]).value or ""
                    stamp = datetime.now().strftime("%d/%m/%Y %H:%M")
                    appended = (old_rem+"\n" if old_rem else "")+f"[{stamp}] {extra_note.strip()}"
                    ws.update_cell(row_idx, col_map["Remarque"], appended)
                st.success("✅ تم حفظ التعديلات"); st.cache_data.clear()
            except Exception as e:
                st.error(f"❌ خطأ: {e}")

    # ملاحظات سريعة + Tag
    st.markdown("### 📝 ملاحظة سريعة")
    scope_df = filtered_df if not filtered_df.empty else df_emp
    scope_df = scope_df.copy(); scope_df["Téléphone_norm"]=scope_df["Téléphone"].apply(normalize_tn_phone)
    tel_key = st.selectbox("اختر العميل", [f"{r['Nom & Prénom']} — {format_display_phone(normalize_tn_phone(r['Téléphone']))}" for _, r in scope_df.iterrows()])
    tel_to_update = normalize_tn_phone(tel_key.split("—")[-1])
    quick_note = st.text_area("🗒️ النص")
    if st.button("📌 أضف الملاحظة"):
        try:
            ws = get_spreadsheet().worksheet(employee)
            values = ws.get_all_values(); header = values[0] if values else []
            tel_idx = header.index("Téléphone")
            row_idx=None
            for i,r in enumerate(values[1:], start=2):
                if len(r)>tel_idx and normalize_tn_phone(r[tel_idx])==tel_to_update: row_idx=i; break
            if not row_idx: st.error("❌ الهاتف غير موجود.")
            else:
                rem_col = EXPECTED_HEADERS.index("Remarque")+1
                old_rem = ws.cell(row_idx, rem_col).value or ""
                stamp = datetime.now().strftime("%d/%m/%Y %H:%M")
                updated = (old_rem+"\n" if old_rem else "")+f"[{stamp}] {quick_note.strip()}"
                ws.update_cell(row_idx, rem_col, updated)
                st.success("✅ تمت الإضافة"); st.cache_data.clear()
        except Exception as e:
            st.error(f"❌ خطأ: {e}")

    st.markdown("### 🎨 Tag لون")
    tel_key2 = st.selectbox("اختر العميل للتلوين", [f"{r['Nom & Prénom']} — {format_display_phone(normalize_tn_phone(r['Téléphone']))}" for _, r in scope_df.iterrows()], key="tag_select")
    tel_color = normalize_tn_phone(tel_key2.split("—")[-1])
    hex_color = st.color_picker("اللون", value=st.session_state.get("last_color","#00AA88"))
    if st.button("🖌️ تلوين"):
        try:
            ws = get_spreadsheet().worksheet(employee)
            values = ws.get_all_values(); header = values[0] if values else []
            tel_idx = header.index("Téléphone")
            row_idx=None
            for i,r in enumerate(values[1:], start=2):
                if len(r)>tel_idx and normalize_tn_phone(r[tel_idx])==tel_color: row_idx=i; break
            if not row_idx: st.error("❌ لم يتم إيجاد العميل.")
            else:
                st.session_state["last_color"]=hex_color
                color_col = EXPECTED_HEADERS.index("Tag")+1
                ws.update_cell(row_idx, color_col, hex_color)
                st.success("✅ تم التلوين"); st.cache_data.clear()
        except Exception as e:
            st.error(f"❌ خطأ: {e}")

    # واتساب
    st.markdown("### 💬 تواصل WhatsApp")
    try:
        scope_for_wa = (filtered_df if not filtered_df.empty else df_emp).copy()
        wa_pick = st.selectbox("اختر العميل لفتح واتساب",
                               [f"{r['Nom & Prénom']} — {format_display_phone(normalize_tn_phone(r['Téléphone']))}" for _,r in scope_for_wa.iterrows()],
                               key="wa_pick")
        default_msg = "سلام! معاك Mega Formation. بخصوص التكوين، نحبّوا ننسّقو معاك موعد المتابعة. 👍"
        wa_msg = st.text_area("الرسالة (WhatsApp)", value=default_msg, key="wa_msg")
        if st.button("📲 فتح WhatsApp"):
            raw_tel = wa_pick.split("—")[-1]
            tel_norm = normalize_tn_phone(raw_tel)
            url = f"https://wa.me/{tel_norm}?text={urllib.parse.quote(wa_msg)}"
            st.markdown(f"[افتح المحادثة الآن]({url})"); st.info("اضغط على الرابط لفتح واتساب.")
    except Exception as e:
        st.warning(f"WhatsApp: {e}")

    # نقل عميل + سجلّ
    st.markdown("### 🔁 نقل عميل بين الموظفين")
    if all_employes:
        colRA,colRB = st.columns(2)
        src_emp = colRA.selectbox("من موظّف", all_employes, key="reassign_src")
        dst_emp = colRB.selectbox("إلى موظّف", [e for e in all_employes if e!=src_emp], key="reassign_dst")
        df_src = df_all[df_all["__sheet_name"]==src_emp].copy()
        if df_src.empty: st.info("❕ لا يوجد عملاء عند هذا الموظّف.")
        else:
            pick = st.selectbox("اختر العميل للنقل", [f"{r['Nom & Prénom']} — {format_display_phone(r['Téléphone'])}" for _, r in df_src.iterrows()], key="reassign_pick")
            phone_pick = normalize_tn_phone(pick.split("—")[-1])
            if st.button("🚚 نقل الآن"):
                try:
                    sh = get_spreadsheet()
                    ws_src, ws_dst = sh.worksheet(src_emp), sh.worksheet(dst_emp)
                    values = ws_src.get_all_values(); header = values[0] if values else []
                    tel_idx = header.index("Téléphone"); row_idx=None
                    for i,r in enumerate(values[1:], start=2):
                        if len(r)>tel_idx and normalize_tn_phone(r[tel_idx])==phone_pick: row_idx=i; break
                    if not row_idx: st.error("❌ لم يتم العثور على هذا العميل."); st.stop()
                    row_values = ws_src.row_values(row_idx)
                    if len(row_values)<len(EXPECTED_HEADERS): row_values += [""]*(len(EXPECTED_HEADERS)-len(row_values))
                    row_values = row_values[:len(EXPECTED_HEADERS)]
                    row_values[EXPECTED_HEADERS.index("Employe")] = dst_emp
                    ws_dst.append_row(row_values); ws_src.delete_rows(row_idx)
                    wslog = ensure_ws(REASSIGN_LOG_SHEET, REASSIGN_LOG_HEADERS)
                    wslog.append_row([datetime.now(timezone.utc).isoformat(), employee, src_emp, dst_emp, row_values[0], normalize_tn_phone(row_values[1])])
                    st.success(f"✅ نقل ({row_values[0]}) من {src_emp} إلى {dst_emp}"); st.cache_data.clear()
                except Exception as e:
                    st.error(f"❌ خطأ أثناء النقل: {e}")

# ================= Admin Page =================
if role=="أدمن":
    st.markdown("## 👑 لوحة الأدمِن")
    if not admin_unlocked():
        st.info("🔐 أدخل كلمة سرّ الأدمِن من اليسار لفتح الصفحة.")
    else:
        colA,colB,colC = st.columns(3)
        with colA:
            st.subheader("➕ إضافة موظّف")
            new_emp = st.text_input("اسم الموظّف الجديد")
            if st.button("إنشاء ورقة"):
                try:
                    sh = get_spreadsheet()
                    titles = [w.title for w in sh.worksheets()]
                    if not new_emp or new_emp in titles: st.warning("⚠️ الاسم فارغ أو موجود.")
                    else:
                        sh.add_worksheet(title=new_emp, rows="1000", cols="20")
                        sh.worksheet(new_emp).update("1:1",[EXPECTED_HEADERS])
                        st.success("✔️ تم الإنشاء"); st.cache_data.clear()
                except Exception as e: st.error(f"❌ خطأ: {e}")
        with colB:
            st.subheader("➕ إضافة عميل (لأي موظّف)")
            sh = get_spreadsheet()
            target_emp = st.selectbox("اختر الموظّف", all_employes, key="admin_add_emp")
            with st.form("admin_add_client_form"):
                nom_a   = st.text_input("👤 الاسم و اللقب")
                tel_a   = st.text_input("📞 الهاتف")
                formation_a = st.text_input("📚 التكوين")
                type_contact_a = st.selectbox("نوع التواصل", ["Visiteur","Appel téléphonique","WhatsApp","Social media"])
                inscription_a  = st.selectbox("التسجيل", ["Pas encore","Inscrit"])
                date_ajout_a   = st.date_input("تاريخ الإضافة", value=date.today())
                suivi_date_a   = st.date_input("تاريخ المتابعة", value=date.today())
                sub_admin = st.form_submit_button("📥 أضف")
            if sub_admin:
                try:
                    if not (nom_a and tel_a and formation_a and target_emp): st.error("❌ حقول ناقصة."); st.stop()
                    tel_norm = normalize_tn_phone(tel_a)
                    if tel_norm in set(df_all["Téléphone_norm"]): st.warning("⚠️ الرقم موجود.")
                    else:
                        insc_val = "Oui" if inscription_a=="Inscrit" else "Pas encore"
                        ws = sh.worksheet(target_emp)
                        ws.append_row([nom_a, tel_norm, type_contact_a, formation_a, "", fmt_date(date_ajout_a), fmt_date(suivi_date_a), "", insc_val, target_emp, ""])
                        st.success("✅ تمت الإضافة"); st.cache_data.clear()
                except Exception as e:
                    st.error(f"❌ خطأ: {e}")
        with colC:
            st.subheader("🗑️ حذف موظّف")
            emp_to_delete = st.selectbox("اختر الموظّف", all_employes, key="admin_del_emp")
            if st.button("❗ حذف الورقة كاملة"):
                try:
                    sh = get_spreadsheet()
                    sh.del_worksheet(sh.worksheet(emp_to_delete))
                    st.success("تم الحذف"); st.cache_data.clear()
                except Exception as e: st.error(f"❌ خطأ: {e}")

        st.markdown("---"); st.subheader("📜 سجلّ نقل العملاء")
        wslog = ensure_ws(REASSIGN_LOG_SHEET, REASSIGN_LOG_HEADERS)
        vals = wslog.get_all_values()
        if vals and len(vals)>1:
            df_log = pd.DataFrame(vals[1:], columns=vals[0])
            def _fmt_ts(x):
                try: return datetime.fromisoformat(x).astimezone().strftime("%Y-%m-%d %H:%M")
                except: return x
            if "timestamp" in df_log.columns: df_log["وقت"]=df_log["timestamp"].apply(_fmt_ts)
            show_cols=["وقت","moved_by","src_employee","dst_employee","client_name","phone"]
            show_cols=[c for c in show_cols if c in df_log.columns]
            st.dataframe(df_log[show_cols].sort_values(show_cols[0], ascending=False), use_container_width=True)
        else:
            st.caption("لا يوجد سجلّ نقل.")
