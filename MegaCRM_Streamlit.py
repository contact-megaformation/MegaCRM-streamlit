# MegaCRM_Streamlit.py — CRM + Finance (MB/Bizerte) + InterNotes + Reassign Log + Quick Payment Update + Payrolls
# ===============================================================================================================
# - Backoff/retry لحماية gspread من 429/5xx
# - Finance: أرصدة Admin/Structure/Inscription تراكمي + للشهر الحالي فقط
# - Quick Payment Update: تحديث نفس الـ Libellé لعميل مُسجَّل أو تعديل آخر صف لنفس الـ Libellé
# - Reassign_Log: تسجيل شكون نقل العميل
# - تبويب "👥 خلاص المكونين والإدارة" (Payrolls) تخصم من صندوق تختارو
# - إحصائيات شهرية للموظفين/المسجّلين باختيار شهر

import json, time, urllib.parse, base64, uuid
import streamlit as st
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime, date, timedelta, timezone
from PIL import Image

# =============== Page config ===============
st.set_page_config(page_title="MegaCRM", layout="wide", initial_sidebar_state="expanded")
st.markdown(
    "<h1 style='text-align:center;margin-top:8px'>📊 CRM MEGA FORMATION - إدارة العملاء</h1><hr>",
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
        # بدّل الـ ID إذا لزم
        sheet_id = "1DV0KyDRYHofWR60zdx63a9BWBywTFhLavGAExPIa6LI"
        return client, sheet_id

client, SPREADSHEET_ID = make_client_and_sheet_id()

# =============== Backoff helpers ===============
def _is_retryable_api_error(e: Exception) -> bool:
    s = str(e)
    return ("429" in s) or ("500" in s) or ("502" in s) or ("503" in s) or ("504" in s) or ("Quota exceeded" in s)

def _backoff_call(fn, *args, **kwargs):
    delay = 0.6
    for _ in range(7):
        try:
            return fn(*args, **kwargs)
        except gspread.exceptions.APIError as e:
            if _is_retryable_api_error(e):
                time.sleep(delay); delay *= 1.7
                continue
            raise
    return fn(*args, **kwargs)

def sh_open():
    delay = 0.6
    for _ in range(7):
        try:
            return client.open_by_key(SPREADSHEET_ID)
        except gspread.exceptions.APIError as e:
            if _is_retryable_api_error(e):
                time.sleep(delay); delay *= 1.7
                continue
            raise
    return client.open_by_key(SPREADSHEET_ID)

# =============== ثوابت وأسماء أوراق ===============
INTER_NOTES_SHEET   = "InterNotes"
INTER_NOTES_HEADERS = ["timestamp","sender","receiver","message","status","note_id"]

REASSIGN_LOG_SHEET  = "Reassign_Log"
REASSIGN_HEADERS    = ["timestamp","moved_by","src_employee","dst_employee","client_name","phone","note"]

PAYROLL_PREFIX      = "Payroll "   # Payroll <Mois> (MB/BZ)
PAYROLL_COLUMNS     = ["Date","Person","Role","Montant","Caisse_Source","Mode","Employé","Note"]

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

# =============== Helpers ===============
def safe_unique_columns(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty: return df
    df = df.copy()
    df.columns = pd.Index(df.columns).astype(str)
    return df.loc[:, ~df.columns.duplicated(keep="first")]

def month_order_idx(mois: str) -> int:
    try: return FIN_MONTHS_FR.index(mois)
    except ValueError: return datetime.now().month - 1

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
        return f"background-color: {val}; color: white;"
    return ""

def mark_alert_cell(val: str):
    s = str(val).strip()
    if not s: return ''
    if "متأخر" in s: return 'background-color:#ffe6b3;color:#7a4e00'
    return 'background-color:#ffcccc;color:#7a0000'

def highlight_inscrit_row(row: pd.Series):
    insc = str(row.get("Inscription", "")).strip().lower()
    return ['background-color:#d6f5e8' if insc in ("inscrit","oui") else '' for _ in row.index]

def _to_num_series(s):
    return (pd.Series(s).astype(str)
            .str.replace(" ", "", regex=False)
            .str.replace(",", ".", regex=False)
            .pipe(pd.to_numeric, errors="coerce")
            .fillna(0.0))

def fin_month_title(mois: str, kind: str, branch: str):
    prefix = "Revenue " if kind == "Revenus" else "Dépense "
    short = "MB" if "Menzel" in branch else "BZ"
    return f"{prefix}{mois} ({short})"

def payroll_title(mois: str, branch: str):
    short = "MB" if "Menzel" in branch else "BZ"
    return f"{PAYROLL_PREFIX}{mois} ({short})"

def _branch_passwords():
    try:
        b = st.secrets["branch_passwords"]
        return {"Menzel Bourguiba": str(b.get("MB", "MB_2025!")),
                "Bizerte": str(b.get("BZ", "BZ_2025!"))}
    except Exception:
        return {"Menzel Bourguiba": "MB_2025!", "Bizerte": "BZ_2025!"}

# =============== Ensure Worksheet ===============
def ensure_ws(title: str, columns: list[str]):
    sh = sh_open()
    try:
        ws = sh.worksheet(title)
    except gspread.WorksheetNotFound:
        ws = _backoff_call(sh.add_worksheet, title=title, rows="2000", cols=str(max(len(columns), 8)))
        _backoff_call(ws.update, "1:1", [columns])
        return ws
    rows = _backoff_call(ws.get_all_values)
    if not rows:
        _backoff_call(ws.update, "1:1", [columns])
    else:
        header = rows[0]
        if not header or header[:len(columns)] != columns:
            _backoff_call(ws.update, "1:1", [columns])
    return ws

# =============== Finance Readers (cached) ===============
@st.cache_data(ttl=300, show_spinner=False)
def fin_read_df_cached(title: str, kind: str) -> pd.DataFrame:
    cols = FIN_REV_COLUMNS if kind == "Revenus" else FIN_DEP_COLUMNS
    try:
        ws = ensure_ws(title, cols)
        values = _backoff_call(ws.get_all_values)
    except Exception as e:
        st.warning(f"⚠️ تعذّر قراءة الورقة: {title} — {e}")
        return pd.DataFrame(columns=cols)

    if not values: return pd.DataFrame(columns=cols)
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
        if c not in df.columns:
            df[c] = 0 if c in ["Prix","Montant_Admin","Montant_Structure","Montant_PreInscription","Montant_Total","Reste","Montant"] else ""

    if "Date" in df.columns:
        df["Date"] = pd.to_datetime(df["Date"], errors="coerce", dayfirst=True)

    if kind == "Revenus":
        for c in ["Prix","Montant_Admin","Montant_Structure","Montant_PreInscription","Montant_Total","Reste"]:
            df[c] = _to_num_series(df[c])
        if "Echeance" in df.columns:
            df["Echeance"] = pd.to_datetime(df["Echeance"], errors="coerce", dayfirst=True)
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

# =============== Batch finance (Jan -> month) ===============
@st.cache_data(ttl=300, show_spinner=False)
def batch_finance_until(branch: str, upto_month: str):
    sh = sh_open()
    upto_idx = month_order_idx(upto_month)
    ranges, meta = [], []
    for kind in ["Revenus", "Dépenses"]:
        for i in range(upto_idx + 1):
            m = FIN_MONTHS_FR[i]
            title = fin_month_title(m, kind, branch)
            ranges.append(f"'{title}'!A1:AO2000")
            meta.append((kind, m, title))
    try:
        res = _backoff_call(sh.values_batch_get, ranges)
        value_ranges = res.get("valueRanges", [])
    except Exception as e:
        st.warning(f"⚠️ Batch fetch فشل: {e}")
        return {"Revenus": {}, "Dépenses": {}}

    out = {"Revenus": {}, "Dépenses": {}}
    for (kind, mois, _title), vr in zip(meta, value_ranges):
        values = vr.get("values", [])
        header = values[0] if values else []
        data   = values[1:] if values and len(values)>1 else []
        if not header: header = FIN_REV_COLUMNS if kind=="Revenus" else FIN_DEP_COLUMNS
        fixed = []
        for r in data:
            row = list(r)
            if len(row) < len(header): row += [""]*(len(header)-len(row))
            else: row = row[:len(header)]
            fixed.append(row)
        df = pd.DataFrame(fixed, columns=header)
        if kind == "Revenus":
            for c in ["Prix","Montant_Admin","Montant_Structure","Montant_PreInscription","Montant_Total","Reste"]:
                if c in df.columns: df[c] = _to_num_series(df[c])
            if "Date" in df.columns: df["Date"] = pd.to_datetime(df["Date"], errors="coerce", dayfirst=True)
            if "Echeance" in df.columns: df["Echeance"] = pd.to_datetime(df["Echeance"], errors="coerce", dayfirst=True)
            for c in FIN_REV_COLUMNS:
                if c not in df.columns: df[c] = 0 if c in ["Prix","Montant_Admin","Montant_Structure","Montant_PreInscription","Montant_Total","Reste"] else ""
            df = df[FIN_REV_COLUMNS]
        else:
            if "Montant" in df.columns: df["Montant"] = _to_num_series(df["Montant"])
            if "Date" in df.columns: df["Date"] = pd.to_datetime(df["Date"], errors="coerce", dayfirst=True)
            for c in FIN_DEP_COLUMNS:
                if c not in df.columns: df[c] = 0 if c=="Montant" else ""
            df = df[FIN_DEP_COLUMNS]
        out[kind][mois] = df
    return out

# =============== Payroll readers (cached) ===============
@st.cache_data(ttl=300, show_spinner=False)
def payroll_read_df(title: str) -> pd.DataFrame:
    try:
        ws = ensure_ws(title, PAYROLL_COLUMNS)
        values = _backoff_call(ws.get_all_values)
    except Exception as e:
        st.warning(f"⚠️ تعذّر قراءة ورقة الخلاص: {title} — {e}")
        return pd.DataFrame(columns=PAYROLL_COLUMNS)
    if not values: return pd.DataFrame(columns=PAYROLL_COLUMNS)
    header = values[0]; data = values[1:] if len(values)>1 else []
    fixed=[]
    for r in data:
        row=list(r)
        if len(row)<len(header): row+=[""]*(len(header)-len(row))
        else: row=row[:len(header)]
        fixed.append(row)
    df=pd.DataFrame(fixed, columns=header)
    if "Date" in df.columns: df["Date"]=pd.to_datetime(df["Date"], errors="coerce", dayfirst=True)
    if "Montant" in df.columns: df["Montant"]=_to_num_series(df["Montant"])
    for c in PAYROLL_COLUMNS:
        if c not in df.columns: df[c]="" if c not in ["Montant"] else 0.0
    return df[PAYROLL_COLUMNS]

# =============== InterNotes ===============
def inter_notes_open_ws():
    sh = sh_open()
    try:
        ws = sh.worksheet(INTER_NOTES_SHEET)
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title=INTER_NOTES_SHEET, rows="1000", cols=str(len(INTER_NOTES_HEADERS)))
        ws.update("1:1", [INTER_NOTES_HEADERS])
    return ws

def inter_notes_append(sender: str, receiver: str, message: str):
    if not message.strip(): return False, "النص فارغ"
    ws = inter_notes_open_ws()
    ts = datetime.now(timezone.utc).isoformat()
    note_id = str(uuid.uuid4())
    ws.append_row([ts, sender, receiver, message.strip(), "unread", note_id])
    return True, note_id

def inter_notes_fetch_all_df() -> pd.DataFrame:
    ws = inter_notes_open_ws()
    values = ws.get_all_values()
    if not values or len(values) <= 1: return pd.DataFrame(columns=INTER_NOTES_HEADERS)
    df = pd.DataFrame(values[1:], columns=values[0])
    for c in INTER_NOTES_HEADERS:
        if c not in df.columns: df[c] = ""
    return df

def inter_notes_fetch_unread(receiver: str) -> pd.DataFrame:
    df = inter_notes_fetch_all_df()
    return df[(df["receiver"] == receiver) & (df["status"] == "unread")].copy()

def inter_notes_mark_read(note_ids: list[str]):
    if not note_ids: return
    ws = inter_notes_open_ws(); values = ws.get_all_values()
    if not values or len(values) <= 1: return
    header = values[0]
    try:
        idx_note = header.index("note_id"); idx_status = header.index("status")
    except ValueError: return
    for r, row in enumerate(values[1:], start=2):
        if len(row) > idx_note and row[idx_note] in note_ids:
            ws.update_cell(r, idx_status + 1, "read")

def play_sound_mp3(path="notification.mp3"):
    try:
        with open(path, "rb") as f:
            b64 = base64.b64encode(f.read()).decode()
        st.markdown(
            f"<audio autoplay><source src='data:audio/mp3;base64,{b64}' type='audio/mp3'></audio>",
            unsafe_allow_html=True
        )
    except FileNotFoundError:
        pass

def inter_notes_ui(current_employee: str, all_employees: list[str], is_admin: bool=False):
    st.subheader("📝 النوط الداخلية")
    with st.expander("✍️ إرسال نوط لموظف آخر", expanded=True):
        col1, col2 = st.columns([1,2])
        with col1:
            receivers = [e for e in all_employees if e != current_employee] if all_employees else []
            receiver = st.selectbox("الموظّف المستلم", receivers)
        with col2:
            message = st.text_area("الملاحظة", placeholder="اكتب ملاحظة قصيرة...")
        if st.button("إرسال ✅", use_container_width=True):
            ok, info = inter_notes_append(current_employee, receiver, message)
            st.success("تم الإرسال 👌") if ok else st.error(f"تعذّر الإرسال: {info}")

    st.divider()
    _autorefresh = getattr(st, "autorefresh", None) or getattr(st, "experimental_autorefresh", None)
    if callable(_autorefresh): _autorefresh(interval=10_000, key="inter_notes_poll")
    if "prev_unread_count" not in st.session_state: st.session_state.prev_unread_count = 0

    unread_df = inter_notes_fetch_unread(current_employee); unread_count = len(unread_df)
    try:
        if unread_count > st.session_state.prev_unread_count:
            st.toast("📩 نوط جديدة وصْلتك!", icon="✉️"); play_sound_mp3()
    finally:
        st.session_state.prev_unread_count = unread_count

    st.markdown(f"### 📥 غير المقروء: **{unread_count}**")
    if unread_count == 0:
        st.info("ما فماش نوط غير مقروءة حاليا.")
    else:
        st.dataframe(unread_df[["timestamp","sender","message","note_id"]].sort_values("timestamp", ascending=False),
                     use_container_width=True, height=220)
        colA, colB = st.columns(2)
        with colA:
            if st.button("اعتبر الكل مقروء ✅", use_container_width=True):
                inter_notes_mark_read(unread_df["note_id"].tolist()); st.success("تم التعليم كمقروء."); st.rerun()
        with colB:
            selected_to_read = st.multiselect(
                "اختار رسائل لتعليمها كمقروء",
                options=unread_df["note_id"].tolist(),
                format_func=lambda nid: f"من {unread_df[unread_df['note_id']==nid]['sender'].iloc[0]} — {unread_df[unread_df['note_id']==nid]['message'].iloc[0][:30]}..."
            )
            if st.button("تعليم المحدد كمقروء", disabled=not selected_to_read, use_container_width=True):
                inter_notes_mark_read(selected_to_read); st.success("تم التعليم كمقروء."); st.rerun()

    st.divider()
    df_all_notes = inter_notes_fetch_all_df()
    mine = df_all_notes[(df_all_notes["receiver"] == current_employee) | (df_all_notes["sender"] == current_employee)].copy()
    st.markdown("### 🗂️ مراسلاتي")
    if mine.empty:
        st.caption("ما عندكش مراسلات مسجلة بعد.")
    else:
        def _fmt_ts(x):
            try: return datetime.fromisoformat(x).astimezone().strftime("%Y-%m-%d %H:%M")
            except: return x
        mine["وقت"] = mine["timestamp"].apply(_fmt_ts)
        mine = mine[["وقت","sender","receiver","message","status","note_id"]].sort_values("وقت", ascending=False)
        st.dataframe(mine, use_container_width=True, height=280)

    if is_admin:
        st.divider(); st.markdown("### 🛡️ لوحة مراقبة الأدمِن (كل المراسلات)")
        if df_all_notes.empty:
            st.caption("لا توجد مراسلات بعد.")
        else:
            def _fmt_ts2(x):
                try: return datetime.fromisoformat(x).astimezone().strftime("%Y-%m-%d %H:%M")
                except: return x
            df_all_notes["وقت"] = df_all_notes["timestamp"].apply(_fmt_ts2)
            disp = df_all_notes[["وقت","sender","receiver","message","status","note_id"]].sort_values("وقت", ascending=False)
            st.dataframe(disp, use_container_width=True, height=320)

# =============== Employee Password Locks ===============
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

# =============== Load all CRM data ===============
@st.cache_data(ttl=600)
def load_all_data():
    sh = sh_open()
    worksheets = sh.worksheets()
    all_dfs, all_employes = [], []
    for ws in worksheets:
        title = ws.title.strip()
        if title.endswith("_PAIEMENTS"): continue
        if title.startswith("_"): continue
        if title.startswith("Revenue ") or title.startswith("Dépense "): continue
        if title.startswith(PAYROLL_PREFIX): continue
        if title in (INTER_NOTES_SHEET, REASSIGN_LOG_SHEET): continue
        all_employes.append(title)
        rows = _backoff_call(ws.get_all_values)
        if not rows:
            _backoff_call(ws.update, "1:1", [EXPECTED_HEADERS])
            rows = _backoff_call(ws.get_all_values)
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

tab_choice = st.sidebar.radio("📑 اختر تبويب:", ["CRM", "مداخيل (MB/Bizerte)", "👥 خلاص المكونين والإدارة", "📝 نوط داخلية"], index=0)
role = st.sidebar.radio("الدور", ["موظف", "أدمن"], horizontal=True)
employee = None
if role == "موظف":
    employee = st.sidebar.selectbox("👨‍💼 اختر الموظّف (ورقة Google Sheets)", all_employes) if all_employes else None

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
                    st.success("تم فتح صفحة الأدمِن لمدة 30 دقيقة.")
                else:
                    st.error("كلمة سرّ غير صحيحة.")

if role == "أدمن":
    admin_lock_ui()

# =============== CRM مشتقات + إحصائيات ===============
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
    inscrit_mask = df_all["Inscription_norm"].isin(["oui", "inscrit"])
    df_all.loc[inscrit_mask, "Date de suivi"] = ""
    df_all.loc[inscrit_mask, "Alerte_view"] = ""
else:
    df_all["Alerte_view"] = ""; df_all["Mois"] = ""; df_all["Téléphone_norm"] = ""; ALL_PHONES = set()

# لوحة سريعة
st.subheader("لوحة إحصائيات سريعة")
df_dash = df_all.copy()
if df_dash.empty:
    st.info("ما فماش داتا للعرض.")
else:
    df_dash["DateAjout_dt"] = pd.to_datetime(df_dash.get("Date ajout"), dayfirst=True, errors="coerce")
    today = datetime.now().date()
    df_dash["Inscription_norm"] = df_dash["Inscription"].fillna("").astype(str).str.strip().str.lower()
    added_today_mask      = df_dash["DateAjout_dt"].dt.date.eq(today)
    registered_today_mask = df_dash["Inscription_norm"].isin(["oui","inscrit"]) & added_today_mask
    total_clients    = int(len(df_dash))
    added_today      = int(added_today_mask.sum())
    registered_today = int(registered_today_mask.sum())
    registered_total = int((df_dash["Inscription_norm"] == "oui").sum())
    rate = round((registered_total / total_clients) * 100, 2) if total_clients else 0.0
    c1, c2, c3 = st.columns(3)
    with c1: st.metric("👥 إجمالي العملاء", f"{total_clients}")
    with c2: st.metric("🆕 المضافون اليوم", f"{added_today}")
    with c3: st.metric("📈 نسبة التسجيل الإجمالية", f"{rate}%")

# إحصائيات شهرية بالاختيار
st.markdown("### 📅 إحصائيات حسب الشهر (الموظفون/المسجّلون)")
if not df_all.empty:
    df_stats = df_all.copy()
    df_stats["DateAjout_dt"] = pd.to_datetime(df_stats["Date ajout"], dayfirst=True, errors="coerce")
    df_stats["MonthNameFR"]  = df_stats["DateAjout_dt"].dt.month.map(lambda x: FIN_MONTHS_FR[x-1] if pd.notna(x) else "")
    month_pick = st.selectbox("اختر شهر", FIN_MONTHS_FR, index=datetime.now().month-1, key="stats_month_pick")
    subset = df_stats[df_stats["MonthNameFR"] == month_pick].copy()
    subset["Inscription_norm"] = subset["Inscription"].fillna("").astype(str).str.strip().str.lower()
    if subset.empty:
        st.info("لا توجد بيانات لهذا الشهر.")
    else:
        grp = (subset.groupby("__sheet_name", dropna=False)
               .agg(Clients=("Nom & Prénom","count"),
                    Inscrits=("Inscription_norm", lambda x: (x=="oui").sum()))
               .reset_index().rename(columns={"__sheet_name":"الموظف"}))
        grp["% تسجيل"] = ((grp["Inscrits"]/grp["Clients"]).replace([float("inf"), float("nan")], 0)*100).round(2)
        st.dataframe(grp.sort_values(["Inscrits","Clients"], ascending=[False,False]), use_container_width=True)

# =============== تبويب المداخيل/المصاريف ===============
if tab_choice == "مداخيل (MB/Bizerte)":
    st.title("💸 المداخيل والمصاريف — (منزل بورقيبة & بنزرت)")
    with st.sidebar:
        st.markdown("---"); st.subheader("🔧 إعدادات المداخيل/المصاريف")
        branch = st.selectbox("الفرع", ["Menzel Bourguiba", "Bizerte"], key="fin_branch")
        kind_ar = st.radio("النوع", ["مداخيل","مصاريف"], horizontal=True, key="fin_kind_ar")
        kind = "Revenus" if kind_ar == "مداخيل" else "Dépenses"
        mois = st.selectbox("الشهر", FIN_MONTHS_FR, index=datetime.now().month-1, key="fin_month")
        # قفل الفرع
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
    df_fin = fin_read_df_cached(fin_title, kind)
    df_view = df_fin.copy()
    if role == "موظف" and employee and "Employé" in df_view.columns:
        df_view = df_view[df_view["Employé"].fillna("").str.strip().str.lower() == (employee or "").strip().lower()]

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

    # --------- ملخّص الأموال (للأدمن فقط) ----------
    if role == "أدمن" and admin_unlocked():
        st.markdown("## 📈 إحصائيات شهرية + أرصدة الصناديق")
        with st.expander("عرض التفاصيل"):
            pack = batch_finance_until(branch, mois)
            rows = []
            upto_idx = month_order_idx(mois)
            for i in range(upto_idx + 1):
                mname = FIN_MONTHS_FR[i]
                rdf = pack["Revenus"].get(mname, pd.DataFrame(columns=FIN_REV_COLUMNS))
                ddf = pack["Dépenses"].get(mname, pd.DataFrame(columns=FIN_DEP_COLUMNS))
                rows.append({
                    "Mois": mname,
                    "Admin_Revenus": float(rdf["Montant_Admin"].sum()) if "Montant_Admin" in rdf else 0.0,
                    "Structure_Revenus": float(rdf["Montant_Structure"].sum()) if "Montant_Structure" in rdf else 0.0,
                    "Inscription_Revenus": float(rdf["Montant_PreInscription"].sum()) if "Montant_PreInscription" in rdf else 0.0,
                    "Dépenses_Admin": float(ddf.loc[ddf["Caisse_Source"]=="Caisse_Admin","Montant"].sum()) if "Caisse_Source" in ddf else 0.0,
                    "Dépenses_Structure": float(ddf.loc[ddf["Caisse_Source"]=="Caisse_Structure","Montant"].sum()) if "Caisse_Source" in ddf else 0.0,
                    "Dépenses_Inscription": float(ddf.loc[ddf["Caisse_Source"]=="Caisse_Inscription","Montant"].sum()) if "Caisse_Source" in ddf else 0.0,
                    "Reste_Cours": float(rdf["Reste"].sum()) if "Reste" in rdf else 0.0
                })
            st.dataframe(pd.DataFrame(rows), use_container_width=True)

            # تراكمي جانفي → الشهر المختار
            rev_admin = rev_struct = rev_inscr = 0.0
            dep_admin = dep_struct = dep_inscr = 0.0
            for i in range(upto_idx + 1):
                mname = FIN_MONTHS_FR[i]
                rdf = pack["Revenus"].get(mname, pd.DataFrame(columns=FIN_REV_COLUMNS))
                ddf = pack["Dépenses"].get(mname, pd.DataFrame(columns=FIN_DEP_COLUMNS))
                if not rdf.empty:
                    rev_admin  += float(rdf["Montant_Admin"].sum())
                    rev_struct += float(rdf["Montant_Structure"].sum())
                    rev_inscr  += float(rdf["Montant_PreInscription"].sum())
                if not ddf.empty and "Caisse_Source" in ddf:
                    dep_admin  += float(ddf.loc[ddf["Caisse_Source"]=="Caisse_Admin","Montant"].sum())
                    dep_struct += float(ddf.loc[ddf["Caisse_Source"]=="Caisse_Structure","Montant"].sum())
                    dep_inscr  += float(ddf.loc[ddf["Caisse_Source"]=="Caisse_Inscription","Montant"].sum())

            reste_admin_caisse  = rev_admin  - dep_admin
            reste_struct_caisse = rev_struct - dep_struct
            reste_inscr_caisse  = rev_inscr  - dep_inscr

            a1, a2, a3 = st.columns(3)
            with a1: st.metric("💼 Reste Admin (تراكمي)", f"{reste_admin_caisse:,.2f}")
            with a2: st.metric("🏢 Reste Structure (تراكمي)", f"{reste_struct_caisse:,.2f}")
            with a3: st.metric("📝 Reste Inscription (تراكمي)", f"{reste_inscr_caisse:,.2f}")

            # الشهر الحالي فقط
            rdf_cur = pack["Revenus"].get(mois, pd.DataFrame(columns=FIN_REV_COLUMNS))
            ddf_cur = pack["Dépenses"].get(mois, pd.DataFrame(columns=FIN_DEP_COLUMNS))
            cur_admin  = float(rdf_cur["Montant_Admin"].sum()) if "Montant_Admin" in rdf_cur else 0.0
            cur_struct = float(rdf_cur["Montant_Structure"].sum()) if "Montant_Structure" in rdf_cur else 0.0
            cur_inscr  = float(rdf_cur["Montant_PreInscription"].sum()) if "Montant_PreInscription" in rdf_cur else 0.0
            cur_dep_admin  = float(ddf_cur.loc[ddf_cur["Caisse_Source"]=="Caisse_Admin","Montant"].sum()) if "Caisse_Source" in ddf_cur else 0.0
            cur_dep_struct = float(ddf_cur.loc[ddf_cur["Caisse_Source"]=="Caisse_Structure","Montant"].sum()) if "Caisse_Source" in ddf_cur else 0.0
            cur_dep_inscr  = float(ddf_cur.loc[ddf_cur["Caisse_Source"]=="Caisse_Inscription","Montant"].sum()) if "Caisse_Source" in ddf_cur else 0.0

            st.markdown("#### 📅 للشهر الحالي فقط")
            c1, c2, c3 = st.columns(3)
            with c1: st.metric("Admin: Revenus / Dépenses / Reste", f"{cur_admin:,.2f} / {cur_dep_admin:,.2f} / {cur_admin-cur_dep_admin:,.2f}")
            with c2: st.metric("Structure: Revenus / Dépenses / Reste", f"{cur_struct:,.2f} / {cur_dep_struct:,.2f} / {cur_struct-cur_dep_struct:,.2f}")
            with c3: st.metric("Inscription: Revenus / Dépenses / Reste", f"{cur_inscr:,.2f} / {cur_dep_inscr:,.2f} / {cur_inscr-cur_dep_inscr:,.2f}")

    # --------- إضافة/تحديث سريع للمدفوع لنفس العميل والـ Libellé ---------
    st.markdown("### 💳 دفعة/تحديث سريع لعميل مُسجَّل (نفس Libellé)")
    reg_df = df_all.copy()
    reg_df["Inscription_norm"] = reg_df["Inscription"].fillna("").astype(str).str.strip().str.lower()
    reg_df = reg_df[reg_df["Inscription_norm"].isin(["oui","inscrit"])]

    if role == "موظف" and employee:
        reg_df = reg_df[reg_df["__sheet_name"] == employee]

    if reg_df.empty:
        st.info("ما فماش عملاء مُسجّلين.")
    else:
        def _opt(row):
            phone = format_display_phone(row.get("Téléphone",""))
            return f"{row.get('Nom & Prénom','')} — {phone} — {row.get('Formation','')} [{row.get('__sheet_name','')}]"
        options = [_opt(r) for _, r in reg_df.iterrows()]
        pick = st.selectbox("اختر العميل", options)
        idx = options.index(pick); row = reg_df.iloc[idx]
        client_name = str(row.get("Nom & Prénom","")).strip()
        client_phone= str(row.get("Téléphone","")).strip()
        client_form = str(row.get("Formation","")).strip()
        emp_default = str(row.get("__sheet_name","")).strip()

        st.caption("سيتم استعمال نفس الـ Libellé للتحديث:")
        default_lib = f"Paiement {client_form} - {client_name}".strip()
        # جلب آخر صف بنفس الـ Libellé (إن وجد)
        rev_df_month = fin_read_df_cached(fin_month_title(mois, "Revenus", branch), "Revenus")
        same_lib = pd.DataFrame()
        if not rev_df_month.empty and "Libellé" in rev_df_month.columns:
            same_lib = rev_df_month[rev_df_month["Libellé"].fillna("").str.strip().str.lower() == default_lib.lower()]
        existing_last = same_lib.tail(1) if not same_lib.empty else pd.DataFrame()

        with st.form("quick_update_payment"):
            d1, d2 = st.columns(2)
            libelle = d1.text_input("Libellé", value=default_lib)
            employe = d2.selectbox("Employé", all_employes if all_employes else [emp_default], index=(all_employes.index(emp_default) if emp_default in all_employes else 0) if all_employes else 0)
            r1, r2, r3 = st.columns(3)
            montant_admin  = r1.number_input("🏢 Montant Admin", min_value=0.0, step=10.0, value=float(existing_last["Montant_Admin"].iloc[0]) if not existing_last.empty else 0.0)
            montant_struct = r2.number_input("🏫 Montant Structure", min_value=0.0, step=10.0, value=float(existing_last["Montant_Structure"].iloc[0]) if not existing_last.empty else 0.0)
            montant_preins = r3.number_input("📝 Montant Pré-Inscription", min_value=0.0, step=10.0, value=float(existing_last["Montant_PreInscription"].iloc[0]) if not existing_last.empty else 0.0)
            mode = st.selectbox("Mode", ["Espèces","Virement","Carte","Chèque","Autre"])
            note = st.text_area("Note", value=f"Client: {client_name} ({client_form}) Tel: {client_phone}")

            colU1, colU2 = st.columns(2)
            do_overwrite = colU1.checkbox("تعديل آخر صف لنفس الـ Libellé (Overwrite)", value=(not existing_last.empty))
            date_val = colU2.date_input("Date العملية", value=date.today())

            if st.form_submit_button("حفظ"):
                try:
                    sh = sh_open()
                    title = fin_month_title(mois, "Revenus", branch)
                    ws = ensure_ws(title, FIN_REV_COLUMNS)
                    header = _backoff_call(ws.row_values, 1)
                    # حساب total
                    montant_total = float(montant_admin) + float(montant_struct)
                    # إذا overwrite: نبحث على آخر صف بنفس Libellé ونحدّث الأعمدة
                    if do_overwrite:
                        values = _backoff_call(ws.get_all_values)
                        if not values or len(values)<=1:
                            st.warning("ما فماش صفوف لتعديلها، تم إنشاء صف جديد.")
                            do_overwrite = False
                        else:
                            lib_idx = header.index("Libellé")
                            # آخر سطر مطابق
                            target_row = None
                            for i in range(len(values)-1, 0, -1):
                                rowv = values[i]
                                if len(rowv)>lib_idx and rowv[lib_idx].strip().lower()==libelle.strip().lower():
                                    target_row = i+1  # gspread index
                                    break
                            if target_row:
                                # تحديث الأعمدة
                                def set_cell(col_name, v):
                                    _backoff_call(ws.update_cell, target_row, header.index(col_name)+1, f"{float(v):.2f}" if isinstance(v,(int,float)) else str(v))
                                set_cell("Date", fmt_date(date_val))
                                set_cell("Libellé", libelle.strip())
                                set_cell("Montant_Admin", montant_admin)
                                set_cell("Montant_Structure", montant_struct)
                                set_cell("Montant_PreInscription", montant_preins)
                                set_cell("Montant_Total", montant_total)
                                set_cell("Mode", mode)
                                set_cell("Employé", employe)
                                set_cell("Catégorie", "Revenus")
                                set_cell("Note", note)
                                st.success("تم تعديل آخر صف لنفس الـ Libellé ✅"); st.cache_data.clear(); st.rerun()
                            else:
                                st.info("ما لقيتش صف قديم، نعمل إنشاء صف جديد...")
                                do_overwrite = False
                    # إنشاء صف جديد
                    if not do_overwrite:
                        row_dict = {
                            "Date": fmt_date(date_val), "Libellé": libelle.strip(), "Prix": "",
                            "Montant_Admin": f"{montant_admin:.2f}", "Montant_Structure": f"{montant_struct:.2f}",
                            "Montant_PreInscription": f"{montant_preins:.2f}", "Montant_Total": f"{montant_total:.2f}",
                            "Echeance": "", "Reste": "", "Mode": mode, "Employé": employe, "Catégorie": "Revenus", "Note": note
                        }
                        vals = [str(row_dict.get(col, "")) for col in FIN_REV_COLUMNS]
                        _backoff_call(ws.append_row, vals)
                        st.success("تم إضافة العملية ✅"); st.cache_data.clear(); st.rerun()
                except Exception as e:
                    st.error(f"❌ خطأ أثناء الحفظ: {e}")

# =============== تبويب نوط داخلية ===============
if tab_choice == "📝 نوط داخلية":
    current_emp_name = (employee if (role == "موظف" and employee) else "Admin")
    is_admin_user = (role == "أدمن")
    inter_notes_ui(current_employee=current_emp_name, all_employees=all_employes, is_admin=is_admin_user)

# =============== تبويب CRM + نقل مع Log ===============
def render_table(df_disp: pd.DataFrame):
    if df_disp.empty:
        st.info("لا توجد بيانات.")
        return
    _df = df_disp.copy()
    _df["Alerte"] = _df.get("Alerte_view", "")
    display_cols = [c for c in EXPECTED_HEADERS if c in _df.columns]
    styled = (_df[display_cols]
              .style.apply(highlight_inscrit_row, axis=1)
              .applymap(mark_alert_cell, subset=["Alerte"])
              .applymap(color_tag, subset=["Tag"]))
    st.dataframe(styled, use_container_width=True)

if tab_choice == "CRM":
    if role == "موظف" and employee:
        _emp_lock_ui(employee)
        if not _emp_unlocked(employee):
            st.info("🔒 أدخل كلمة سرّ الموظّف في أعلى هذا القسم لفتح الورقة."); st.stop()
        st.subheader(f"📁 لوحة {employee}")
        df_emp = df_all[df_all["__sheet_name"] == employee].copy()
        if not df_emp.empty:
            df_emp["DateAjout_dt"] = pd.to_datetime(df_emp["Date ajout"], dayfirst=True, errors="coerce")
            df_emp = df_emp.dropna(subset=["DateAjout_dt"])
            df_emp["Mois"] = df_emp["DateAjout_dt"].dt.strftime("%m-%Y")
            month_filter = st.selectbox("🗓️ اختر شهر الإضافة", sorted(df_emp["Mois"].dropna().unique(), reverse=True))
            filtered_df = df_emp[df_emp["Mois"] == month_filter].copy()
        else:
            st.warning("⚠️ لا يوجد أي عملاء بعد."); filtered_df = pd.DataFrame()
        st.markdown("### 📋 قائمة العملاء")
        render_table(filtered_df)

        # نقل عميل + Log
        st.markdown("### 🔁 نقل عميل بين الموظفين")
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
                mover = employee if role=="موظف" else "Admin"
                if st.button("🚚 نقل الآن"):
                    try:
                        sh = sh_open()
                        ws_src, ws_dst = sh.worksheet(src_emp), sh.worksheet(dst_emp)
                        values = _backoff_call(ws_src.get_all_values)
                        header = values[0] if values else []
                        row_idx = None
                        if "Téléphone" in header:
                            tel_idx = header.index("Téléphone")
                            for i, r in enumerate(values[1:], start=2):
                                if len(r) > tel_idx and normalize_tn_phone(r[tel_idx]) == phone_pick:
                                    row_idx = i; break
                        if not row_idx:
                            st.error("❌ لم يتم العثور على هذا العميل.")
                        else:
                            row_values = _backoff_call(ws_src.row_values, row_idx)
                            if len(row_values) < len(EXPECTED_HEADERS):
                                row_values += [""] * (len(EXPECTED_HEADERS) - len(row_values))
                            row_values = row_values[:len(EXPECTED_HEADERS)]
                            row_values[EXPECTED_HEADERS.index("Employe")] = dst_emp
                            _backoff_call(ws_dst.append_row, row_values)
                            _backoff_call(ws_src.delete_rows, row_idx)
                            # Log
                            wslog = ensure_ws(REASSIGN_LOG_SHEET, REASSIGN_HEADERS)
                            _backoff_call(wslog.append_row, [
                                datetime.now(timezone.utc).isoformat(),
                                mover, src_emp, dst_emp,
                                row_values[0],  # client_name
                                normalize_tn_phone(row_values[1]),  # phone
                                "reassign"
                            ])
                            st.success(f"✅ نقل ({row_values[0]}) من {src_emp} إلى {dst_emp}"); st.cache_data.clear()
                    except Exception as e:
                        st.error(f"❌ خطأ أثناء النقل: {e}")

# =============== تبويب 👥 خلاص المكونين والإدارة ===============
if tab_choice == "👥 خلاص المكونين والإدارة":
    st.title("👥 خلاص المكونين والإدارة")
    with st.sidebar:
        st.markdown("---"); st.subheader("⚙️ إعدادات الخلاص")
        branch_p = st.selectbox("الفرع", ["Menzel Bourguiba", "Bizerte"], key="pay_branch")
        mois_p   = st.selectbox("الشهر", FIN_MONTHS_FR, index=datetime.now().month-1, key="pay_month")
        BRANCH_PASSWORDS = _branch_passwords()
        key_pw = f"pay_pw_ok::{branch_p}"
        if key_pw not in st.session_state: st.session_state[key_pw] = False
        if not st.session_state[key_pw]:
            pw_try = st.text_input("كلمة سرّ الفرع", type="password", key=f"pay_pw_{branch_p}")
            if st.button("دخول تبويب الخلاص", key=f"pay_enter_{branch_p}"):
                if pw_try and pw_try == BRANCH_PASSWORDS.get(branch_p, ""):
                    st.session_state[key_pw] = True; st.success("تم الدخول ✅")
                else:
                    st.error("كلمة سرّ غير صحيحة ❌")
    if not st.session_state.get(f"pay_pw_ok::{branch_p}", False):
        st.info("⬅️ أدخل كلمة السرّ من اليسار للمتابعة."); st.stop()

    pay_title = payroll_title(mois_p, branch_p)
    df_pay = payroll_read_df(pay_title)
    st.subheader(f"📄 {pay_title}")
    st.dataframe(df_pay if not df_pay.empty else pd.DataFrame(columns=PAYROLL_COLUMNS), use_container_width=True)

    # إضافة سطر خلاص (يخصم من صندوق مختار مثل المصاريف)
    st.markdown("### ➕ إضافة عملية خلاص")
    with st.form("add_payroll"):
        c1, c2, c3 = st.columns(3)
        date_val = c1.date_input("Date", value=date.today())
        person   = c2.text_input("Person (الاسم)")
        rolep    = c3.selectbox("Role", ["Formateur","Admin","Autre"])
        c4, c5, c6 = st.columns(3)
        montant  = c4.number_input("Montant", min_value=0.0, step=10.0)
        caisse   = c5.selectbox("Caisse_Source", ["Caisse_Admin","Caisse_Structure","Caisse_Inscription"])
        mode     = c6.selectbox("Mode", ["Espèces","Virement","Carte","Chèque","Autre"])
        employe_p= st.selectbox("Employé (من سجّل العملية)", all_employes if all_employes else [""])
        note_p   = st.text_area("Note")
        if st.form_submit_button("حفظ الخلاص ✅"):
            try:
                ws = ensure_ws(pay_title, PAYROLL_COLUMNS)
                vals = [fmt_date(date_val), person.strip(), rolep, f"{montant:.2f}", caisse, mode, employe_p.strip(), note_p.strip()]
                _backoff_call(ws.append_row, vals)
                st.success("تمّ الحفظ ✅"); st.cache_data.clear(); st.rerun()
            except Exception as e:
                st.error(f"❌ خطأ أثناء الحفظ: {e}")

    # ملخص الخلاص + تأثيره على الصناديق (للأدمن فقط)
    if role == "أدمن" and admin_unlocked():
        st.markdown("### 📊 ملخص الخلاص (الشهر الحالي فقط)")
        if df_pay.empty:
            st.info("لا توجد عمليات خلاص لهذا الشهر.")
        else:
            total_pay_admin     = float(df_pay.loc[df_pay["Caisse_Source"]=="Caisse_Admin","Montant"].sum())
            total_pay_structure = float(df_pay.loc[df_pay["Caisse_Source"]=="Caisse_Structure","Montant"].sum())
            total_pay_inscr     = float(df_pay.loc[df_pay["Caisse_Source"]=="Caisse_Inscription","Montant"].sum())
            k1, k2, k3 = st.columns(3)
            with k1: st.metric("خصم من Caisse_Admin", f"{total_pay_admin:,.2f}")
            with k2: st.metric("خصم من Caisse_Structure", f"{total_pay_structure:,.2f}")
            with k3: st.metric("خصم من Caisse_Inscription", f"{total_pay_inscr:,.2f}")
        st.caption("ملاحظة: الخلاص يُعتبر خصم إضافي من الصناديق فوق المصاريف.")


# =============== تبويب الأدمِن ===============
if role == "أدمن":
    st.markdown("## 👑 لوحة الأدمِن")
    if not admin_unlocked():
        st.info("🔐 أدخل كلمة سرّ الأدمِن من اليسار لفتح الصفحة.")
    else:
        colA, colB, colC = st.columns(3)
        with colA:
            st.subheader("➕ إضافة موظّف")
            new_emp = st.text_input("اسم الموظّف الجديد")
            if st.button("إنشاء ورقة"):
                try:
                    sh = sh_open()
                    titles = [w.title for w in sh.worksheets()]
                    if not new_emp or new_emp in titles:
                        st.warning("⚠️ الاسم فارغ أو موجود.")
                    else:
                        _backoff_call(sh.add_worksheet, title=new_emp, rows="1000", cols="20")
                        _backoff_call(sh.worksheet(new_emp).update, "1:1", [EXPECTED_HEADERS])
                        st.success("✔️ تم الإنشاء"); st.cache_data.clear()
                except Exception as e:
                    st.error(f"❌ خطأ: {e}")
        with colB:
            st.subheader("➕ إضافة عميل (لأي موظّف)")
            sh = sh_open()
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
                        _backoff_call(ws.append_row, [nom_a, tel_a, type_contact_a, formation_a, "",
                                                      fmt_date(date_ajout_a), fmt_date(suivi_date_a), "", insc_val, target_emp, ""])
                        st.success("✅ تمت الإضافة"); st.cache_data.clear()
                except Exception as e:
                    st.error(f"❌ خطأ: {e}")
        with colC:
            st.subheader("🗑️ حذف موظّف")
            emp_to_delete = st.selectbox("اختر الموظّف", all_employes, key="admin_del_emp")
            if st.button("❗ حذف الورقة كاملة"):
                try:
                    sh = sh_open()
                    _backoff_call(sh.del_worksheet, sh.worksheet(emp_to_delete))
                    st.success("تم الحذف"); st.cache_data.clear()
                except Exception as e:
                    st.error(f"❌ خطأ: {e}")
        st.caption("صفحة الأدمِن مفتوحة لمدّة 30 دقيقة من وقت الفتح.")
