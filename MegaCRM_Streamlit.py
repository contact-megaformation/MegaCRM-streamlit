# MegaCRM_Streamlit_App.py — CRM + Finance (MB/Bizerte) + InterNotes + Reassign Log + Payouts + Monthly Stats + Payment Edit
# =================================================================================================
# - CRM كامل: موظفين (قفل بكلمة سر)، قائمة العملاء، بحث، ملاحظات/Tag، تعديل، إضافة، نقل + زر WhatsApp
# - Admin: إضافة/حذف موظف، إضافة عميل لأي موظّف + سجلّ نقل "شكون حرّك"
# - تبويب "مداخيل (MB/Bizerte)": Revenus/Dépenses + تعديل دفعة موجودة لنفس Libellé عبر نفس ورقة الشهر
# - تبويب "💼 خلاص الإدارة/المكوّنين": اختيار Caisse_Source (Admin/Structure/Inscription) لكل فرع
# - 📅 إحصائيات شهرية تفصيلية للعملاء
# - 📝 نوط داخلية بين الموظفين + صوت + Popup + مراقبة للأدمن

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
        sheet_id = "1DV0KyDRYHofWR60zdx63a9BWBywTFhLavGAExPIa6LI"  # بدّلها إذا يلزم
        return client, sheet_id

client, SPREADSHEET_ID = make_client_and_sheet_id()

# ======================================================================
#                               CONSTANTS
# ======================================================================
INTER_NOTES_SHEET = "InterNotes"
INTER_NOTES_HEADERS = ["timestamp","sender","receiver","message","status","note_id"]

REASSIGN_LOG_SHEET   = "Reassign_Log"
REASSIGN_LOG_HEADERS = ["timestamp","moved_by","src_employee","dst_employee","client_name","phone"]

EXPECTED_HEADERS = [
    "Nom & Prénom","Téléphone","Type de contact","Formation",
    "Remarque","Date ajout","Date de suivi","Alerte",
    "Inscription","Employe","Tag"
]

FIN_REV_COLUMNS = [
    "Date", "Libellé", "Prix",
    "Montant_Admin", "Montant_Structure", "Montant_PreInscription", "Montant_Total",
    "Echeance", "Reste",
    "Mode", "Employé", "Catégorie", "Note"
]
FIN_DEP_COLUMNS = ["Date","Libellé","Montant","Caisse_Source","Mode","Employé","Catégorie","Note"]

# 💼 Payouts (خلاص الإدارة/المكوّنين)
PAYOUTS_COLUMNS = [
    "Date", "Type", "Personne", "Libellé", "Montant",
    "Caisse_Source", "Mode", "Employé", "Note"
]

FIN_MONTHS_FR = ["Janvier","Février","Mars","Avril","Mai","Juin","Juillet","Aout","Septembre","Octobre","Novembre","Décembre"]

def fin_month_title(mois: str, kind: str, branch: str):
    prefix = "Revenue " if kind == "Revenus" else ("Dépense " if kind == "Dépenses" else "Payout ")
    short = "MB" if "Menzel" in branch else "BZ"
    return f"{prefix}{mois} ({short})"

def _branch_passwords():
    try:
        b = st.secrets["branch_passwords"]
        return {"Menzel Bourguiba": str(b.get("MB", "MB_2025!")), "Bizerte": str(b.get("BZ", "BZ_2025!"))}
    except Exception:
        return {"Menzel Bourguiba": "MB_2025!", "Bizerte": "BZ_2025!"}

# ======================================================================
#                               HELPERS
# ======================================================================
def safe_unique_columns(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    df = df.copy()
    df.columns = pd.Index(df.columns).astype(str)
    return df.loc[:, ~df.columns.duplicated(keep="first")]

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

def _to_num_series_any(s):
    return (
        pd.Series(s).astype(str)
        .str.replace(" ", "", regex=False)
        .str.replace(",", ".", regex=False)
        .pipe(pd.to_numeric, errors="coerce")
        .fillna(0.0)
    )

def ensure_ws(title: str, columns: list[str]):
    sh = client.open_by_key(SPREADSHEET_ID)
    try:
        ws = sh.worksheet(title)
    except gspread.WorksheetNotFound:
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

# ======================================================================
#                               InterNotes
# ======================================================================
def inter_notes_open_ws():
    return ensure_ws(INTER_NOTES_SHEET, INTER_NOTES_HEADERS)

def inter_notes_append(sender: str, receiver: str, message: str):
    if not message.strip():
        return False, "النص فارغ"
    ws = inter_notes_open_ws()
    ts = datetime.now(timezone.utc).isoformat()
    note_id = str(uuid.uuid4())
    ws.append_row([ts, sender, receiver, message.strip(), "unread", note_id])
    return True, note_id

def inter_notes_fetch_all_df() -> pd.DataFrame:
    ws = inter_notes_open_ws()
    values = ws.get_all_values()
    if not values or len(values) <= 1:
        return pd.DataFrame(columns=INTER_NOTES_HEADERS)
    df = pd.DataFrame(values[1:], columns=values[0])
    for c in INTER_NOTES_HEADERS:
        if c not in df.columns:
            df[c] = ""
    return df

def inter_notes_fetch_unread(receiver: str) -> pd.DataFrame:
    df = inter_notes_fetch_all_df()
    return df[(df["receiver"] == receiver) & (df["status"] == "unread")].copy()

def inter_notes_mark_read(note_ids: list[str]):
    if not note_ids:
        return
    ws = inter_notes_open_ws()
    values = ws.get_all_values()
    if not values or len(values) <= 1:
        return
    header = values[0]
    try:
        idx_note = header.index("note_id")
        idx_status = header.index("status")
    except ValueError:
        return
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
            unsafe_allow_html=True,
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
    if callable(_autorefresh):
        _autorefresh(interval=10_000, key="inter_notes_poll")

    if "prev_unread_count" not in st.session_state:
        st.session_state.prev_unread_count = 0

    unread_df = inter_notes_fetch_unread(current_employee)
    unread_count = len(unread_df)

    try:
        if unread_count > st.session_state.prev_unread_count:
            st.toast("📩 نوط جديدة وصْلتك!", icon="✉️")
            play_sound_mp3()
    finally:
        st.session_state.prev_unread_count = unread_count

    st.markdown(f"### 📥 غير المقروء: **{unread_count}**")
    if unread_count == 0:
        st.info("ما فماش نوط غير مقروءة حاليا.")
    else:
        st.dataframe(
            unread_df[["timestamp","sender","message","note_id"]].sort_values("timestamp", ascending=False),
            use_container_width=True, height=220
        )
        colA, colB = st.columns(2)
        with colA:
            if st.button("اعتبر الكل مقروء ✅", use_container_width=True):
                inter_notes_mark_read(unread_df["note_id"].tolist())
                st.success("تم التعليم كمقروء."); st.rerun()
        with colB:
            selected_to_read = st.multiselect(
                "اختار رسائل لتعليمها كمقروء",
                options=unread_df["note_id"].tolist(),
                format_func=lambda nid: f"من {unread_df[unread_df['note_id']==nid]['sender'].iloc[0]} — {unread_df[unread_df['note_id']==nid]['message'].iloc[0][:30]}..."
            )
            if st.button("تعليم المحدد كمقروء", disabled=not selected_to_read, use_container_width=True):
                inter_notes_mark_read(selected_to_read)
                st.success("تم التعليم كمقروء."); st.rerun()

    st.divider()
    df_all_notes = inter_notes_fetch_all_df()
    mine = df_all_notes[(df_all_notes["receiver"] == current_employee) | (df_all_notes["sender"] == current_employee)].copy()
    st.markdown("### 🗂️ مراسلاتي")
    if mine.empty:
        st.caption("ما عندكش مراسلات مسجلة بعد.")
    else:
        def _fmt_ts(x):
            try:
                return datetime.fromisoformat(x).astimezone().strftime("%Y-%m-%d %H:%M")
            except:
                return x
        mine["وقت"] = mine["timestamp"].apply(_fmt_ts)
        mine = mine[["وقت","sender","receiver","message","status","note_id"]].sort_values("وقت", ascending=False)
        st.dataframe(mine, use_container_width=True, height=280)

    if is_admin:
        st.divider(); st.markdown("### 🛡️ لوحة مراقبة الأدمِن (كل المراسلات)")
        if df_all_notes.empty:
            st.caption("لا توجد مراسلات بعد.")
        else:
            def _fmt_ts2(x):
                try:
                    return datetime.fromisoformat(x).astimezone().strftime("%Y-%m-%d %H:%M")
                except:
                    return x
            df_all_notes["وقت"] = df_all_notes["timestamp"].apply(_fmt_ts2)
            disp = df_all_notes[["وقت","sender","receiver","message","status","note_id"]].sort_values("وقت", ascending=False)
            st.dataframe(disp, use_container_width=True, height=320)

# ======================================================================
#                       Load all CRM (employee sheets only)
# ======================================================================
@st.cache_data(ttl=600)
def load_all_data():
    sh = client.open_by_key(SPREADSHEET_ID)
    worksheets = sh.worksheets()
    all_dfs, all_employes = [], []
    for ws in worksheets:
        title = ws.title.strip()
        if title.endswith("_PAIEMENTS"):    continue
        if title.startswith("_"):           continue
        if title.startswith("Revenue ") or title.startswith("Dépense ") or title.startswith("Payout "): continue
        if title in (INTER_NOTES_SHEET, REASSIGN_LOG_SHEET): continue

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

# ======================================================================
#                               Sidebar
# ======================================================================
try:
    st.sidebar.image(Image.open("logo.png"), use_container_width=True)
except Exception:
    pass

tab_choice = st.sidebar.radio(
    "📑 اختر تبويب:",
    ["CRM", "مداخيل (MB/Bizerte)", "💼 خلاص الإدارة/المكوّنين", "📝 نوط داخلية"],
    index=0
)
role = st.sidebar.radio("الدور", ["موظف", "أدمن"], horizontal=True)
employee = None
if role == "موظف":
    employee = st.sidebar.selectbox("👨‍💼 اختر الموظّف (ورقة Google Sheets)", all_employes) if all_employes else None

# ======================================================================
#                            Admin lock
# ======================================================================
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

# ======================================================================
#                    Finance Readers/Writers (Revenus/Dépenses)
# ======================================================================
def fin_read_df(title: str, kind: str) -> pd.DataFrame:
    cols = FIN_REV_COLUMNS if kind == "Revenus" else FIN_DEP_COLUMNS
    ws = ensure_ws(title, cols)
    values = ws.get_all_values()
    if not values:
        return pd.DataFrame(columns=cols)
    df = pd.DataFrame(values[1:], columns=values[0])
    if "Date" in df.columns:
        df["Date"] = pd.to_datetime(df["Date"], errors="coerce", dayfirst=True)
    if kind == "Revenus" and "Echeance" in df.columns:
        df["Echeance"] = pd.to_datetime(df["Echeance"], errors="coerce", dayfirst=True)
    if kind == "Revenus":
        for c in ["Prix","Montant_Admin","Montant_Structure","Montant_PreInscription","Montant_Total","Reste"]:
            if c in df.columns:
                df[c] = _to_num_series_any(df[c])
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
    return safe_unique_columns(df)

def fin_append_row(title: str, row: dict, kind: str):
    cols = FIN_REV_COLUMNS if kind=="Revenus" else FIN_DEP_COLUMNS
    ws = ensure_ws(title, cols)
    header = ws.row_values(1)
    vals = [str(row.get(col, "")) for col in header]
    ws.append_row(vals)

# ======================================================================
#                  Payouts (خلاص الإدارة/المكوّنين) Readers/Writers
# ======================================================================
def payouts_read_df(title: str) -> pd.DataFrame:
    ws = ensure_ws(title, PAYOUTS_COLUMNS)
    values = ws.get_all_values()
    if not values:
        return pd.DataFrame(columns=PAYOUTS_COLUMNS)
    df = pd.DataFrame(values[1:], columns=values[0])
    if "Date" in df.columns:
        df["Date"] = pd.to_datetime(df["Date"], errors="coerce", dayfirst=True)
    if "Montant" in df.columns:
        df["Montant"] = _to_num_series_any(df["Montant"])
    return safe_unique_columns(df)

def payouts_append_row(title: str, row: dict):
    ws = ensure_ws(title, PAYOUTS_COLUMNS)
    header = ws.row_values(1)
    vals = [str(row.get(col, "")) for col in header]
    ws.append_row(vals)

# ======================================================================
#      🆕 Revenus helpers لقراءة/تجميع دفعات نفس Libellé عبر كل الأشهر
# ======================================================================
def find_revenus_across_months_for_libelle(branch: str, libelle: str) -> pd.DataFrame:
    out = []
    for m in FIN_MONTHS_FR:
        title = fin_month_title(m, "Revenus", branch)
        try:
            df = fin_read_df(title, "Revenus")
        except Exception:
            df = pd.DataFrame(columns=FIN_REV_COLUMNS)
        if not df.empty and "Libellé" in df.columns:
            sub = df[df["Libellé"].fillna("").str.strip().str.lower() == libelle.strip().lower()].copy()
            if not sub.empty:
                sub["__sheet_title"] = title
                sub["__mois"] = m
                out.append(sub)
    if out:
        return pd.concat(out, ignore_index=True)
    return pd.DataFrame(columns=FIN_REV_COLUMNS + ["__sheet_title","__mois"])

def find_revenus_row_index(ws, libelle: str, date_str: str) -> int | None:
    rows = ws.get_all_values()
    if not rows: return None
    header = rows[0]
    try:
        idx_lib = header.index("Libellé")
        idx_dt  = header.index("Date")
    except ValueError:
        return None
    for i, r in enumerate(rows[1:], start=2):
        if len(r) > max(idx_lib, idx_dt):
            if r[idx_lib].strip().lower() == libelle.strip().lower() and r[idx_dt].strip() == date_str.strip():
                return i
    return None

# ======================================================================
#                                   CRM مشتقّات + لوحة
# ======================================================================
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

# ---------------- Dashboard سريع ----------------
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
    registered_today_mask = df_dash["Inscription_norm"].isin(["oui", "inscrit"]) & added_today_mask
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
    # إضافة عميل جديد
    st.markdown("### ➕ أضف عميل جديد")
    with st.form("emp_add_client"):
        col1, col2 = st.columns(2)
        with col1:
            nom = st.text_input("👤 الاسم و اللقب")
            tel_raw = st.text_input("📞 رقم الهاتف")
            formation = st.text_input("📚 التكوين")
            inscription = st.selectbox("🟢 التسجيل", ["Pas encore", "Inscrit"])
        with col2:
            type_contact = st.selectbox("📞 نوع الاتصال", ["Visiteur", "Appel téléphonique", "WhatsApp", "Social media"])
            date_ajout_in = st.date_input("🕓 تاريخ الإضافة", value=date.today())
            date_suivi_in = st.date_input("📆 تاريخ المتابعة", value=date.today())
        if st.form_submit_button("📥 أضف العميل"):
            try:
                ws = client.open_by_key(SPREADSHEET_ID).worksheet(employee)
                tel = normalize_tn_phone(tel_raw)
                if not(nom and tel and formation): st.error("❌ حقول أساسية ناقصة."); st.stop()
                if tel in ALL_PHONES: st.warning("⚠️ الرقم موجود مسبقًا."); st.stop()
                insc_val = "Oui" if inscription == "Inscrit" else "Pas encore"
                ws.append_row([nom, tel, type_contact, formation, "", fmt_date(date_ajout_in), fmt_date(date_suivi_in), "", insc_val, employee, ""])
                st.success("✅ تم إضافة العميل"); st.cache_data.clear()
            except Exception as e:
                st.error(f"❌ خطأ أثناء الإضافة: {e}")

# ======================================================================
#                                   📝 نوط داخلية
# ======================================================================
if tab_choice == "📝 نوط داخلية":
    current_emp_name = (employee if (role == "موظف" and employee) else "Admin")
    is_admin_user = (role == "أدمن")
    inter_notes_ui(
        current_employee=current_emp_name,
        all_employees=all_employes,
        is_admin=is_admin_user
    )

# ======================================================================
#                                   Admin Page
# ======================================================================
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
                    sh = client.open_by_key(SPREADSHEET_ID)
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
            sh = client.open_by_key(SPREADSHEET_ID)
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
            emp_to_delete = st.selectbox("اختر الموظّف", all_employes, key="admin_del_emp")
            if st.button("❗ حذف الورقة كاملة"):
                try:
                    sh = client.open_by_key(SPREADSHEET_ID)
                    sh.del_worksheet(sh.worksheet(emp_to_delete))
                    st.success("تم الحذف"); st.cache_data.clear()
                except Exception as e:
                    st.error(f"❌ خطأ: {e}")

        st.markdown("---")
        st.subheader("📜 سجلّ نقل العملاء (Reassign Log)")
        wslog = ensure_ws(REASSIGN_LOG_SHEET, REASSIGN_LOG_HEADERS)
        vals = wslog.get_all_values()
        if vals and len(vals) > 1:
            df_log = pd.DataFrame(vals[1:], columns=vals[0])
            def _fmt_ts3(x):
                try:
                    return datetime.fromisoformat(x).astimezone().strftime("%Y-%m-%d %H:%M")
                except:
                    return x
            if "timestamp" in df_log.columns:
                df_log["وقت"] = df_log["timestamp"].apply(_fmt_ts3)
            show_cols = ["وقت","moved_by","src_employee","dst_employee","client_name","phone"]
            show_cols = [c for c in show_cols if c in df_log.columns]
            st.dataframe(df_log[show_cols].sort_values(show_cols[0], ascending=False), use_container_width=True)
        else:
            st.caption("لا يوجد سجلّ نقل إلى حدّ الآن.")

        st.caption("صفحة الأدمِن مفتوحة لمدّة 30 دقيقة من وقت الفتح.")
