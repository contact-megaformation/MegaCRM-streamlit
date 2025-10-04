# MegaCRM_Streamlit.py — CRM + Finance (MB/Bizerte) + InterNotes + Reassign Log + Quick Payment Update + Trainers Payouts
# ====================================================================================================
# - Backoff + batching لحماية gspread من 429
# - Finance: أرصدة منفصلة (Admin / Structure / Inscription) تراكميًا + للشهر الحالي
# - Quick update: اختيار عميل مُسجّل وتحديث نفس الـ Libellé
# - Reassign_Log: تسجيل من نقل العميل
# - تبويب جديد 💰 "خلاص المكوّنين و الإدارة" (مثل المصاريف مع Caisse_Source)
# - إحصاءات شهرية: اختيار شهر ونشوف مسجّلين/موظفين لذلك الشهر فقط

import json, time, urllib.parse, base64, uuid
import streamlit as st
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime, date, timedelta, timezone
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
        # ✅ غيّر الـ ID أدناه بمتاعك إذا تحب
        sheet_id = "1DV0KyDRYHofWR60zdx63a9BWBywTFhLavGAExPIa6LI"
        return client, sheet_id

client, SPREADSHEET_ID = make_client_and_sheet_id()

# ---------- Backoff helpers (ضد 429/5xx) ----------
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
                time.sleep(delay)
                delay *= 1.7
                continue
            raise
    return fn(*args, **kwargs)

def sh_open():
    """افتح الملف مع محاولات متعددة لتفادي APIError عند open_by_key."""
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

# ============================ ثابتات عامة ============================
INTER_NOTES_SHEET = "InterNotes"
INTER_NOTES_HEADERS = ["timestamp","sender","receiver","message","status","note_id"]

REASSIGN_LOG_SHEET = "Reassign_Log"
REASSIGN_HEADERS = ["timestamp","moved_by","src_employee","dst_employee","client_name","phone","note"]

TRAINERS_SHEET = "Trainers_Payouts"
TRAINERS_HEADERS = [
    "Date","Libellé","Montant","Beneficiary_Type","Beneficiary_Name",
    "Caisse_Source","Mode","Employé","Catégorie","Note"
]

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

# ============================ Helpers ============================
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
    if "متأخر" in s: return 'background-color: #ffe6b3; color: #7a4e00'
    return 'background-color: #ffcccc; color: #7a0000'

def highlight_inscrit_row(row: pd.Series):
    insc = str(row.get("Inscription", "")).strip().lower()
    return ['background-color: #d6f5e8' if insc in ("inscrit","oui") else '' for _ in row.index]

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

def _branch_passwords():
    try:
        b = st.secrets["branch_passwords"]
        return {"Menzel Bourguiba": str(b.get("MB", "MB_2025!")),
                "Bizerte": str(b.get("BZ", "BZ_2025!"))}
    except Exception:
        return {"Menzel Bourguiba": "MB_2025!", "Bizerte": "BZ_2025!"}

# ---------- Ensure Worksheet ----------
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

# ---------- Read Finance (cached) ----------
@st.cache_data(ttl=300, show_spinner=False)
def fin_read_df_cached(title: str, kind: str) -> pd.DataFrame:
    cols = FIN_REV_COLUMNS if kind == "Revenus" else FIN_DEP_COLUMNS
    try:
        ws = ensure_ws(title, cols)
        values = _backoff_call(ws.get_all_values)
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

    # تأمين الأعمدة
    for c in cols:
        if c not in df.columns:
            df[c] = 0 if c in ["Prix","Montant_Admin","Montant_Structure","Montant_PreInscription","Montant_Total","Reste","Montant"] else ""

    # تحويلات
    if "Date" in df.columns:
        df["Date"] = pd.to_datetime(df["Date"], errors="coerce", dayfirst=True)

    if kind == "Revenus":
        for c in ["Prix","Montant_Admin","Montant_Structure","Montant_PreInscription","Montant_Total","Reste"]:
            df[c] = _to_num_series(df[c])
        if "Echeance" in df.columns:
            df["Echeance"] = pd.to_datetime(df["Echeance"], errors="coerce", dayfirst=True)
        if "Alert" not in df.columns: df["Alert"] = ""
        # Alerts
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

# ---------- Batch read Jan -> selected month ----------
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
    for (kind, mois, title), vr in zip(meta, value_ranges):
        values = vr.get("values", [])
        if not values:
            out[kind][mois] = pd.DataFrame(columns=FIN_REV_COLUMNS if kind=="Revenus" else FIN_DEP_COLUMNS); continue
        header = values[0]; data = values[1:] if len(values)>1 else []
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

# ============================ InterNotes ============================
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
            f"""
            <audio autoplay>
              <source src="data:audio/mp3;base64,{b64}" type="audio/mp3">
            </audio>
            """,
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
    if unread_df.empty:
        st.info("ما فماش نوط غير مقروءة حاليا.")
    else:
        st.dataframe(
            unread_df[["timestamp","sender","message","note_id"]].sort_values("timestamp", ascending=False),
            use_container_width=True, height=220
        )
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

# ============================ Employee Password Locks ============================
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

# ============================ Load all CRM data ============================
@st.cache_data(ttl=600)
def load_all_data():
    sh = sh_open()
    worksheets = sh.worksheets()
    all_dfs, all_employes = [], []

    for ws in worksheets:
        title = ws.title.strip()
        if title.endswith("_PAIEMENTS"):    continue
        if title.startswith("_"):           continue
        if title.startswith("Revenue ") or title.startswith("Dépense "): continue
        if title in (INTER_NOTES_SHEET, REASSIGN_LOG_SHEET, TRAINERS_SHEET): continue

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

# ============================ Sidebar ============================
try:
    st.sidebar.image(Image.open("logo.png"), use_container_width=True)
except Exception:
    pass

tab_choice = st.sidebar.radio("📑 اختر تبويب:", ["CRM", "مداخيل (MB/Bizerte)", "💰 خلاص المكونين والإدارة", "📝 نوط داخلية"], index=0)
role = st.sidebar.radio("الدور", ["موظف", "أدمن"], horizontal=True)
employee = None
if role == "موظف":
    employee = st.sidebar.selectbox("👨‍💼 اختر الموظّف (ورقة Google Sheets)", all_employes) if all_employes else None

# ============================ Admin lock ============================
def admin_unlocked() -> bool:
    ok = st.session_state.get("admin_ok", False); ts = st.session_state.get("admin_ok_at", None)
    return bool(ok and ts and (datetime.now() - ts) <= timedelta(minutes=30))

def admin_lock_ui():
    with st.sidebar.expander("🔐 إدارة (Admin)", expanded=(role=="أدمن" and not admin_unlocked())):
        if admin_unlocked():
            if st.button("قفل صفحة الأدمِن"):
                st.session_state["admin_ok"] = False; st.session_state["admin_ok_at"] = None; st.rerun()
        else:
            admin_pwd = st.text_input("كلمة سرّ الأدمِن", type="password", key="admin_pwd_inp")
            if st.button("فتح صفحة الأدمِن"):
                conf = str(st.secrets.get("admin_password", "admin123"))
                if admin_pwd and admin_pwd == conf:
                    st.session_state["admin_ok"] = True; st.session_state["admin_ok_at"] = datetime.now()
                    st.success("تم فتح صفحة الأدمِن لمدة 30 دقيقة.")
                else:
                    st.error("كلمة سرّ غير صحيحة.")

if role == "أدمن":
    admin_lock_ui()
# ============================ وظائف مساعدة للـ Finance (append/update) ============================
def fin_append_row(title: str, row: dict, kind: str):
    """إضافة سطر جديد لورقة Revenus/Dépenses حسب العناوين بالترتيب."""
    cols = FIN_REV_COLUMNS if kind == "Revenus" else FIN_DEP_COLUMNS
    ws = ensure_ws(title, cols)
    header = _backoff_call(ws.row_values, 1)
    if not header: header = cols
    vals = [str(row.get(col, "")) for col in header]
    _backoff_call(ws.append_row, vals)

def fin_find_rows_by_libelle(ws, libelle: str) -> list[int]:
    """يرجع فهارس (أرقام الصفوف) لكل الأسطر اللي Libellé متاعها يطابق."""
    values = _backoff_call(ws.get_all_values)
    if not values: return []
    header = values[0]
    if "Libellé" not in header: return []
    idx = header.index("Libellé")
    matches = []
    for r, row in enumerate(values[1:], start=2):
        if len(row) > idx and str(row[idx]).strip().lower() == libelle.strip().lower():
            matches.append(r)
    return matches

def fin_update_revenue_row(ws, row_idx: int, updates: dict):
    """يعدّل خلايا معيّنة في صفّ Revenus واحد."""
    header = _backoff_call(ws.row_values, 1)
    mapping = {h: i+1 for i, h in enumerate(header)}
    for k, v in updates.items():
        if k in mapping:
            _backoff_call(ws.update_cell, row_idx, mapping[k], str(v))

# ============================ CRM: لوحة سريعة + إحصاءات شهرية بالاختيار ============================
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
    with c1: st.metric("👥 إجمالي العملاء", f"{total_clients}")
    with c2: st.metric("🆕 المضافون اليوم", f"{added_today}")
    with c3: st.metric("✅ المسجّلون اليوم", f"{registered_today}")
    with c4: st.metric("🚨 التنبيهات الحالية", f"{alerts_now}")
    with c5: st.metric("📈 نسبة التسجيل الإجمالية", f"{rate}%")

# إحصائيات شهرية بالاختيار (عدد العملاء + المسجلين) حسب الموظف
if tab_choice == "CRM":
    st.markdown("### 📅 إحصائيات حسب شهر محدّد")
    # أسماء الأشهر FR (نستعمل القائمة نفسها)
    month_pick = st.selectbox("اختر شهر", FIN_MONTHS_FR, index=datetime.now().month-1, key="stats_month_pick")
    # نعمل تصفية حسب شهر الإضافة (بالاسم)، نعتمد على month رقم من DateAjout_dt
    df_stats = df_all.copy()
    if df_stats.empty:
        st.info("لا توجد بيانات للإحصاء.")
    else:
        df_stats["DateAjout_dt"] = pd.to_datetime(df_stats.get("Date ajout"), dayfirst=True, errors="coerce")
        df_stats["MonthNum"]     = df_stats["DateAjout_dt"].dt.month
        # تحويل MonthNum إلى اسم FR مع حماية من NaN
        def _month_name_from_num(m):
            try:
                m_int = int(m)
                return FIN_MONTHS_FR[m_int-1] if 1 <= m_int <= 12 else ""
            except Exception:
                return ""
        df_stats["MonthNameFR"]  = df_stats["MonthNum"].map(_month_name_from_num)
        df_stats["Inscription_norm"] = df_stats["Inscription"].fillna("").astype(str).str.strip().str.lower()

        df_month = df_stats[df_stats["MonthNameFR"] == month_pick].copy()
        if df_month.empty:
            st.info("لا توجد بيانات في هذا الشهر.")
        else:
            grp = (
                df_month.groupby("__sheet_name", dropna=False)
                .agg(
                    Clients=("Nom & Prénom", "count"),
                    Inscrits=("Inscription_norm", lambda x: (x == "oui").sum())
                )
                .reset_index()
                .rename(columns={"__sheet_name": "الموظف"})
            )
            grp["% تسجيل"] = ((grp["Inscrits"] / grp["Clients"]).replace([float("inf"), float("nan")], 0) * 100).round(2)
            grp = grp.sort_values(by=["Inscrits","Clients"], ascending=[False, False])
            st.dataframe(grp, use_container_width=True)

# ============================ تبويب المداخيل/المصاريف ============================
if tab_choice == "مداخيل (MB/Bizerte)":
    st.title("💸 المداخيل والمصاريف — (منزل بورقيبة & بنزرت)")

    # إعدادات يسار
    with st.sidebar:
        st.markdown("---"); st.subheader("🔧 إعدادات المداخيل/المصاريف")
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

    # --------- إحصائيات شهرية + أرصدة الصناديق ----------
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
                "Reste_Cours": float(rdf["Reste"].sum()) if "Reste" in rdf else 0.0  # ديون الدروس
            })
        st.dataframe(pd.DataFrame(rows), use_container_width=True)

        # --- أرصدة الصناديق (تراكمي من جانفي → الشهر المختار) ---
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

        # --- الشهر الحالي فقط ---
        rdf_cur = pack["Revenus"].get(mois, pd.DataFrame(columns=FIN_REV_COLUMNS))
        ddf_cur = pack["Dépenses"].get(mois, pd.DataFrame(columns=FIN_DEP_COLUMNS))
        cur_admin  = float(rdf_cur["Montant_Admin"].sum()) if "Montant_Admin" in rdf_cur else 0.0
        cur_struct = float(rdf_cur["Montant_Structure"].sum()) if "Montant_Structure" in rdf_cur else 0.0
        cur_inscr  = float(rdf_cur["Montant_PreInscription"].sum()) if "Montant_PreInscription" in rdf_cur else 0.0
        cur_dep_admin  = float(ddf_cur.loc[ddf_cur["Caisse_Source"]=="Caisse_Admin","Montant"].sum()) if "Caisse_Source" in ddf_cur else 0.0
        cur_dep_struct = float(ddf_cur.loc[ddf_cur["Caisse_Source"]=="Caisse_Structure","Montant"].sum()) if "Caisse_Source" in ddf_cur else 0.0
        cur_dep_inscr  = float(ddf_cur.loc[ddf_cur["Caisse_Source"]=="Caisse_Inscription","Montant"].sum()) if "Caisse_Source" in ddf_cur else 0.0

        st.markdown("#### 📅 للشهر الحالي فقط")
        c1b, c2b, c3b = st.columns(3)
        with c1b: st.metric("Admin: Revenus / Dépenses / Reste", f"{cur_admin:,.2f} / {cur_dep_admin:,.2f} / {cur_admin-cur_dep_admin:,.2f}")
        with c2b: st.metric("Structure: Revenus / Dépenses / Reste", f"{cur_struct:,.2f} / {cur_dep_struct:,.2f} / {cur_struct-cur_dep_struct:,.2f}")
        with c3b: st.metric("Inscription: Revenus / Dépenses / Reste", f"{cur_inscr:,.2f} / {cur_dep_inscr:,.2f} / {cur_inscr-cur_dep_inscr:,.2f}")

    st.markdown("---")
    st.markdown("### ➕ إضافة عملية جديدة")
    # نموذج مبسّط للإضافة (Revenus/Dépenses)
    with st.form("fin_add_row"):
        c1, c2, c3 = st.columns(3)
        date_val  = c1.date_input("Date", value=date.today())
        libelle   = c2.text_input("Libellé")
        employe   = c3.selectbox("Employé", all_employes if all_employes else [""])

        if kind == "Revenus":
            r1, r2, r3 = st.columns(3)
            prix = r1.number_input("💰 Prix (سعر التكوين)", min_value=0.0, step=10.0)
            montant_admin  = r2.number_input("🏢 Montant Admin", min_value=0.0, step=10.0)
            montant_struct = r3.number_input("🏫 Montant Structure", min_value=0.0, step=10.0)
            r4, r5 = st.columns(2)
            montant_preins = r4.number_input("📝 Montant Pré-Inscription", min_value=0.0, step=10.0, help="اختياري")
            echeance       = r5.date_input("⏰ تاريخ الاستحقاق", value=date.today())
            mode, categorie = st.selectbox("Mode", ["Espèces","Virement","Carte","Chèque","Autre"]), "Revenus"
            note = st.text_area("Note", value="")
            montant_total = float(montant_admin) + float(montant_struct)
            # حساب Reste تلقائي من نفس الـ Libellé
            reste_prop = max(float(prix) - float(montant_total), 0.0)
            reste = st.number_input("💳 Reste", min_value=0.0, value=float(round(reste_prop, 2)), step=10.0)

            submitted = st.form_submit_button("✅ حفظ العملية")
            if submitted:
                if not libelle.strip():
                    st.error("Libellé مطلوب.")
                elif prix <= 0 and montant_total <= 0 and montant_preins <= 0:
                    st.error("المبالغ كلّها صفر.")
                else:
                    fin_append_row(
                        fin_title,
                        {
                            "Date": fmt_date(date_val),
                            "Libellé": libelle.strip(),
                            "Prix": f"{float(prix):.2f}",
                            "Montant_Admin": f"{float(montant_admin):.2f}",
                            "Montant_Structure": f"{float(montant_struct):.2f}",
                            "Montant_PreInscription": f"{float(montant_preins):.2f}",
                            "Montant_Total": f"{float(montant_total):.2f}",
                            "Echeance": fmt_date(echeance),
                            "Reste": f"{float(reste):.2f}",
                            "Mode": mode,
                            "Employé": employe,
                            "Catégorie": categorie,
                            "Note": note.strip(),
                        },
                        "Revenus"
                    )
                    st.success("تمّ الحفظ ✅"); st.cache_data.clear(); st.rerun()
        else:
            # Dépenses
            r1, r2, r3 = st.columns(3)
            montant = r1.number_input("Montant", min_value=0.0, step=10.0)
            caisse  = r2.selectbox("Caisse_Source", ["Caisse_Admin","Caisse_Structure","Caisse_Inscription"])
            mode    = r3.selectbox("Mode", ["Espèces","Virement","Carte","Chèque","Autre"])
            c4, c5  = st.columns(2)
            categorie = c4.text_input("Catégorie", value="Achat")
            note      = c5.text_area("Note (اختياري)")
            submitted = st.form_submit_button("✅ حفظ العملية")
            if submitted:
                if not libelle.strip():
                    st.error("Libellé مطلوب.")
                elif montant <= 0:
                    st.error("المبلغ لازم > 0.")
                else:
                    fin_append_row(
                        fin_title,
                        {
                            "Date": fmt_date(date_val),
                            "Libellé": libelle.strip(),
                            "Montant": f"{float(montant):.2f}",
                            "Caisse_Source": caisse,
                            "Mode": mode,
                            "Employé": employe,
                            "Catégorie": categorie.strip(),
                            "Note": note.strip(),
                        },
                        "Dépenses"
                    )
                    st.success("تمّ الحفظ ✅"); st.cache_data.clear(); st.rerun()

    # --------- 💳 دفعة/تحديث سريع لعميل مُسجَّل (Revenus فقط) ----------
    if kind == "Revenus":
        st.markdown("---")
        st.markdown("### 💳 دفعة/تحديث سريع لعميل مُسجَّل (نفس الـ Libellé)")

        # جلب العملاء المسجلين (Inscrit/Oui)
        reg_df = df_all.copy()
        reg_df["Inscription_norm"] = reg_df["Inscription"].fillna("").astype(str).str.strip().str.lower()
        reg_df = reg_df[reg_df["Inscription_norm"].isin(["oui","inscrit"])]
        if role == "موظف" and employee:
            reg_df = reg_df[reg_df["__sheet_name"] == employee]

        if reg_df.empty:
            st.info("لا يوجد عملاء مُسجّلون للاختيار.")
        else:
            def _opt(row):
                phone = format_display_phone(row.get("Téléphone",""))
                return f"{row.get('Nom & Prénom','')} — {phone} — {row.get('Formation','')} [{row.get('__sheet_name','')}]"

            options = [_opt(r) for _, r in reg_df.iterrows()]
            pick = st.selectbox("اختر العميل", options, key="quick_pay_pick")
            idx  = options.index(pick)
            row  = reg_df.iloc[idx]
            selected_client = {
                "name": str(row.get("Nom & Prénom","")).strip(),
                "tel":  str(row.get("Téléphone","")).strip(),
                "formation": str(row.get("Formation","")).strip(),
                "emp": str(row.get("__sheet_name","")).strip()
            }

            # Libellé الافتراضي
            default_lib = f"Paiement {selected_client['formation']} - {selected_client['name']}".strip()
            st.caption("سيتم استعمال نفس الـ Libellé للتحديث:")
            lib_q = st.text_input("Libellé", value=default_lib, key="quick_lib")

            # نقرأ ورقة Revenus للشهر المختار
            rev_ws = ensure_ws(fin_month_title(mois, "Revenus", branch), FIN_REV_COLUMNS)
            matches = fin_find_rows_by_libelle(rev_ws, lib_q)
            # إذا موجود، نعرض الموجود للتعديل؛ إذا لا، ننشئ واحد جديد
            # نقرأ القيم الحالية
            cur_vals = {"Prix":0.0, "Montant_Admin":0.0, "Montant_Structure":0.0, "Montant_PreInscription":0.0, "Reste":0.0}
            if matches:
                # نأخذ آخر صف مطابق
                last_row = matches[-1]
                row_vals = _backoff_call(rev_ws.row_values, last_row)
                header   = _backoff_call(rev_ws.row_values, 1)
                hm = {h:i for i,h in enumerate(header)}
                def _read_num(key):
                    try:
                        return _to_num_series([row_vals[hm[key]]])[0] if key in hm and hm[key] < len(row_vals) else 0.0
                    except Exception:
                        return 0.0
                for k in cur_vals.keys():
                    cur_vals[k] = _read_num(k)

            st.write("القيم الحالية:", cur_vals)

            colA, colB, colC, colD = st.columns(4)
            prix_new    = colA.number_input("Prix", min_value=0.0, value=float(cur_vals["Prix"]), step=10.0)
            adm_new     = colB.number_input("Montant Admin", min_value=0.0, value=float(cur_vals["Montant_Admin"]), step=10.0)
            struct_new  = colC.number_input("Montant Structure", min_value=0.0, value=float(cur_vals["Montant_Structure"]), step=10.0)
            preins_new  = colD.number_input("Montant Pré-Inscription", min_value=0.0, value=float(cur_vals["Montant_PreInscription"]), step=10.0)

            total_new = float(adm_new) + float(struct_new)
            reste_suggest = max(float(prix_new) - total_new, 0.0)
            reste_new = st.number_input("Reste", min_value=0.0, value=float(round(reste_suggest,2)), step=10.0)
            e1, e2 = st.columns(2)
            mode_q = e1.selectbox("Mode", ["Espèces","Virement","Carte","Chèque","Autre"], index=0)
            emp_q  = e2.selectbox("Employé", all_employes if all_employes else [""], index=(all_employes.index(selected_client["emp"]) if selected_client["emp"] in all_employes else 0) if all_employes else 0)

            if st.button("💾 حفظ/تحديث السطر"):
                if matches:
                    # تحديث آخر صف
                    rix = matches[-1]
                    fin_update_revenue_row(rev_ws, rix, {
                        "Prix": f"{float(prix_new):.2f}",
                        "Montant_Admin": f"{float(adm_new):.2f}",
                        "Montant_Structure": f"{float(struct_new):.2f}",
                        "Montant_PreInscription": f"{float(preins_new):.2f}",
                        "Montant_Total": f"{float(total_new):.2f}",
                        "Reste": f"{float(reste_new):.2f}",
                        "Mode": mode_q,
                        "Employé": emp_q,
                        "Catégorie": "Revenus",
                    })
                    st.success("تمّ تحديث السطر ✅"); st.cache_data.clear(); st.rerun()
                else:
                    # إنشاء سطر جديد
                    fin_append_row(
                        fin_month_title(mois, "Revenus", branch),
                        {
                            "Date": fmt_date(date.today()),
                            "Libellé": lib_q.strip(),
                            "Prix": f"{float(prix_new):.2f}",
                            "Montant_Admin": f"{float(adm_new):.2f}",
                            "Montant_Structure": f"{float(struct_new):.2f}",
                            "Montant_PreInscription": f"{float(preins_new):.2f}",
                            "Montant_Total": f"{float(total_new):.2f}",
                            "Echeance": fmt_date(date.today()),
                            "Reste": f"{float(reste_new):.2f}",
                            "Mode": mode_q,
                            "Employé": emp_q,
                            "Catégorie": "Revenus",
                            "Note": f"Quick update for {selected_client['name']}"
                        },
                        "Revenus"
                    )
                    st.success("تمّ إنشاء السطر الجديد ✅"); st.cache_data.clear(); st.rerun()
tab_choice = st.sidebar.radio(
    "📑 اختر تبويب:",
    ["CRM", "مداخيل (MB/Bizerte)", "💼 خلاص (Formateurs & إدارة)", "📝 نوط داخلية"],
    index=0
)
