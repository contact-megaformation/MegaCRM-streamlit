# MegaCRM_Streamlit_App_PRO_Light.py
# ===============================================================================================================
# CRM + Revenus/Dépenses (MB/Bizerte) + Pré-Inscription + نوط داخلية — واجهة فاتحة + أزرار 3D + ملخّص شهري + لوج نقل
# ===============================================================================================================

import json, time, urllib.parse, base64, uuid, re
import streamlit as st
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime, date, timedelta, timezone
from PIL import Image

# ---------------- Page config ----------------
st.set_page_config(page_title="MegaCRM", layout="wide", initial_sidebar_state="expanded")

# =============== 🎨 UI SKIN — LIGHT (Real Buttons) ===============
def inject_pro_ui():
    st.markdown("""
    <style>
      @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;600;700&display=swap');
      :root{
        --bg:#f7f9fc; --card:#ffffff; --text:#1a1f36; --muted:#5b6b82;
        --border:#e7ecf3; --accent:#2563eb; --accent-2:#3b82f6;
        --success:#16a34a; --warning:#d97706; --danger:#dc2626; --radius:14px;
      }
      html, body, [data-testid="stAppViewContainer"]{
        background: var(--bg) !important; color: var(--text) !important;
        font-family: 'Inter', system-ui, -apple-system, Segoe UI, Roboto, 'Helvetica Neue', Arial, "Noto Sans", "Liberation Sans", sans-serif !important;
        font-size: 16px !important; line-height: 1.45 !important;
      }
      [data-testid="stSidebar"]{ background: #fbfdff !important; border-right: 1px solid var(--border) !important; }

      /* Real 3D Buttons */
      .stButton>button, .stDownloadButton>button{
        position: relative !important; appearance: none !important; border-radius: 12px !important;
        background: linear-gradient(180deg, var(--accent-2), var(--accent)) !important; color: #fff !important;
        border: 1px solid #1e40af !important; padding: 0.65rem 1.1rem !important;
        font-weight: 800 !important; letter-spacing: .2px !important;
        box-shadow: 0 2px 0 #153e94 inset, 0 8px 18px rgba(37,99,235,0.25), 0 0 0 1px rgba(255,255,255,0.6) inset;
        transition: transform .06s ease, box-shadow .12s ease, filter .15s ease;
      }
      .stButton>button:hover, .stDownloadButton>button:hover{
        filter: brightness(1.03);
        box-shadow: 0 2px 0 #153e94 inset, 0 10px 22px rgba(37,99,235,0.30), 0 0 0 1px rgba(255,255,255,0.65) inset;
      }
      .stButton>button:active, .stDownloadButton>button:active{
        transform: translateY(1px);
        box-shadow: 0 1px 0 #153e94 inset, 0 6px 14px rgba(37,99,235,0.22), 0 0 0 1px rgba(255,255,255,0.55) inset;
      }
      .stButton>button:focus-visible, .stDownloadButton>button:focus-visible{
        outline: none !important;
        box-shadow: 0 2px 0 #153e94 inset, 0 8px 18px rgba(37,99,235,0.25), 0 0 0 3px rgba(37,99,235,0.35) !important;
      }

      /* Inputs */
      .stTextInput>div>div>input, .stTextArea textarea, .stSelectbox>div>div>div>div,
      .stDateInput>div>div>input, .stNumberInput input{
        background: #ffffff !important; color: var(--text) !important;
        border-radius: 12px !important; border: 1px solid var(--border) !important;
        box-shadow: 0 1px 0 rgba(0,0,0,0.02) inset !important;
      }
      .stTextArea textarea{ min-height: 110px !important; }

      /* Cards */
      .topbar{
        border-radius: 18px; padding: 18px 22px; background: linear-gradient(135deg, #ffffff, #f3f7ff);
        border: 1px solid var(--border); box-shadow: 0 12px 30px rgba(16,24,40,0.08); margin-bottom: 14px;
      }
      .topbar h1{ margin:0; font-size: 26px; letter-spacing:.2px; color: var(--text);}
      .topbar p{ margin:8px 0 0; color: var(--muted); }

      .section{
        background: var(--card); border-radius: var(--radius); border: 1px solid var(--border);
        padding: 14px 16px; margin: 10px 0 18px; box-shadow: 0 8px 20px rgba(16,24,40,0.06);
      }
      .section h3{ margin: 4px 0 12px; color: var(--text); }

      /* KPI */
      .kpi-grid{ display:grid; grid-template-columns: repeat(5, minmax(140px,1fr)); gap:12px; }
      .kpi{ background:#fff; border-radius:14px; padding:14px; border:1px solid var(--border); box-shadow:0 8px 20px rgba(16,24,40,0.06);}
      .kpi .label{ color:#5b6b82; font-size:13px;}
      .kpi .value{ font-size:22px; font-weight:800; margin-top:6px; letter-spacing:.2px; color:#1a1f36;}
      .kpi.ok{ border-color: rgba(34,197,94,.45);} .kpi.warn{ border-color: rgba(217,119,6,.35);} .kpi.dng{ border-color: rgba(220,38,38,.40);}

      /* Badge */
      .pill{ display:inline-block; padding:4px 10px; border-radius:999px; font-size:12px; border:1px solid var(--border); color: var(--text); background:#fff; }
      .pill.blue{border-color:#bfdbfe; background:#eff6ff;} .pill.green{border-color:#bbf7d0; background:#ecfdf5;}
      .pill.orange{border-color:#fed7aa; background:#fff7ed;} .pill.red{border-color:#fecaca; background:#fef2f2;}
    </style>
    """, unsafe_allow_html=True)

def ui_topbar(title:str, subtitle:str=""):
    st.markdown(f"""<div class="topbar"><h1>📊 {title}</h1><p>{subtitle}</p></div>""", unsafe_allow_html=True)

def ui_section(title:str, icon:str="📦"):
    st.markdown(f"""<div class="section"><h3>{icon} {title}</h3>""", unsafe_allow_html=True)

def ui_section_end():
    st.markdown("</div>", unsafe_allow_html=True)

def ui_kpis(items):
    st.markdown('<div class="kpi-grid">', unsafe_allow_html=True)
    for it in items:
        st.markdown(f"""
          <div class="kpi {it.get('tone','')}">
            <div class="label">{it.get('label','')}</div>
            <div class="value">{it.get('value','')}</div>
          </div>
        """, unsafe_allow_html=True)
    st.markdown('</div>', unsafe_allow_html=True)

def ui_badge(text, tone="blue"):
    st.markdown(f'<span class="pill {tone}">{text}</span>', unsafe_allow_html=True)

# call once
inject_pro_ui()
ui_topbar("CRM MEGA FORMATION — إدارة العملاء", "إدارة العملاء • المداخيل والمصاريف • نوط داخلية")

# 🔎 بحث عام (placeholder)
top_col1, _ = st.columns([3,1])
with top_col1:
    global_q = st.text_input("ابحث سريعًا...", placeholder="اكتب اسم / هاتف (216XXXXXXXX أو 8 أرقام) / تكوين ...")

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
        sheet_id = "1DV0KyDRYHofWR60zdx63a9BWBywTFhLavGAExPIa6LI"  # بدّل بالـ ID متاعك
        return client, sheet_id

client, SPREADSHEET_ID = make_client_and_sheet_id()

# ============================ 🆕 InterNotes (نوط داخلية) ============================
INTER_NOTES_SHEET = "InterNotes"
INTER_NOTES_HEADERS = ["timestamp","sender","receiver","message","status","note_id"]

def inter_notes_open_ws():
    sh = client.open_by_key(SPREADSHEET_ID)
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
    ws = inter_notes_open_ws()
    values = ws.get_all_values()
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
            f"""<audio autoplay><source src="data:audio/mp3;base64,{b64}" type="audio/mp3"></audio>""",
            unsafe_allow_html=True,
        )
    except FileNotFoundError:
        pass

def inter_notes_ui(current_employee: str, all_employees: list[str], is_admin: bool=False):
    ui_section("📝 النوط الداخلية", "📝")

    # ✍️ إضافة ملاحظة جديدة (إرسال لموظف آخر)
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

    # تنبيه أوتوماتيكي
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
            selected_to_read = st.multiselect("اختار رسائل لتعليمها كمقروء",
                                              options=unread_df["note_id"].tolist(),
                                              format_func=lambda nid: f"من {unread_df[unread_df['note_id']==nid]['sender'].iloc[0]} — {unread_df[unread_df['note_id']==nid]['message'].iloc[0][:30]}...")
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
        st.dataframe(mine[["وقت","sender","receiver","message","status","note_id"]].sort_values("وقت", ascending=False),
                     use_container_width=True, height=280)

    if is_admin:
        st.divider(); st.markdown("### 🛡️ لوحة مراقبة الأدمِن (كل المراسلات)")
        if df_all_notes.empty:
            st.caption("لا توجد مراسلات بعد.")
        else:
            def _fmt_ts2(x):
                try: return datetime.fromisoformat(x).astimezone().strftime("%Y-%m-%d %H:%M")
                except: return x
            df_all_notes["وقت"] = df_all_notes["timestamp"].apply(_fmt_ts2)
            st.dataframe(df_all_notes[["وقت","sender","receiver","message","status","note_id"]].sort_values("وقت", ascending=False),
                         use_container_width=True, height=320)
    ui_section_end()

# ---------------- Schemas ----------------
EXPECTED_HEADERS = ["Nom & Prénom","Téléphone","Type de contact","Formation","Remarque","Date ajout","Date de suivi","Alerte","Inscription","Employe","Tag"]
FIN_REV_COLUMNS = ["Date","Libellé","Prix","Montant_Admin","Montant_Structure","Montant_PreInscription","Montant_Total","Echeance","Reste","Mode","Employé","Catégorie","Note"]
FIN_DEP_COLUMNS = ["Date","Libellé","Montant","Caisse_Source","Mode","Employé","Catégorie","Note"]
FIN_MONTHS_FR = ["Janvier","Février","Mars","Avril","Mai","Juin","Juillet","Aout","Septembre","Octobre","Novembre","Décembre"]

def safe_unique_columns(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty: return df
    df = df.copy(); df.columns = pd.Index(df.columns).astype(str)
    return df.loc[:, ~df.columns.duplicated(keep="first")]

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

# ===== FIX: لا تنشئ ورقة أثناء التجميع الشهري + تحكّم في الإنشاء =====
def fin_ensure_ws(client, sheet_id: str, title: str, columns: list[str], create_if_missing: bool = True):
    """يرجّع worksheet أو None إذا مش موجود و create_if_missing=False"""
    sh = client.open_by_key(sheet_id)
    try:
        ws = sh.worksheet(title)
    except gspread.WorksheetNotFound:
        if not create_if_missing:
            return None
        ws = sh.add_worksheet(title=title, rows="2000", cols=str(max(len(columns), 8)))
        try:
            ws.update("1:1", [columns])
        except gspread.exceptions.APIError:
            pass
        return ws

    try:
        rows = ws.get_all_values()
    except gspread.exceptions.APIError:
        return ws

    if not rows:
        try: ws.update("1:1", [columns])
        except gspread.exceptions.APIError: pass
    else:
        header = rows[0]
        if not header or header[:len(columns)] != columns:
            try: ws.update("1:1", [columns])
            except gspread.exceptions.APIError: pass
    return ws

def _to_num_series(s):
    return s.astype(str).str.replace(" ", "", regex=False).str.replace(",", ".", regex=False).pipe(pd.to_numeric, errors="coerce").fillna(0.0)

def fin_read_df(client, sheet_id: str, title: str, kind: str, create_if_missing: bool = True) -> pd.DataFrame:
    cols = FIN_REV_COLUMNS if kind == "Revenus" else FIN_DEP_COLUMNS
    ws = fin_ensure_ws(client, sheet_id, title, cols, create_if_missing=create_if_missing)
    if ws is None:
        return pd.DataFrame(columns=cols)
    try:
        values = ws.get_all_values()
    except gspread.exceptions.APIError:
        return pd.DataFrame(columns=cols)
    if not values:
        return pd.DataFrame(columns=cols)

    df = pd.DataFrame(values[1:], columns=values[0])

    if "Date" in df.columns: df["Date"] = pd.to_datetime(df["Date"], errors="coerce", dayfirst=True)
    if kind == "Revenus" and "Echeance" in df.columns:
        df["Echeance"] = pd.to_datetime(df["Echeance"], errors="coerce", dayfirst=True)

    if kind == "Revenus":
        for c in ["Prix","Montant_Admin","Montant_Structure","Montant_PreInscription","Montant_Total","Reste"]:
            if c in df.columns: df[c] = _to_num_series(df[c])
        if "Alert" not in df.columns: df["Alert"] = ""
        if "Echeance" in df.columns and "Reste" in df.columns:
            today_ts = pd.Timestamp.now().normalize()
            ech = pd.to_datetime(df["Echeance"], errors="coerce"); reste = pd.to_numeric(df["Reste"], errors="coerce").fillna(0.0)
            late_mask  = ech.notna() & (ech < today_ts) & (reste > 0)
            today_mask = ech.notna() & (ech.dt.normalize() == today_ts) & (reste > 0)
            df.loc[late_mask, "Alert"] = "⚠️ متأخر"; df.loc[today_mask, "Alert"] = "⏰ اليوم"
    else:
        if "Montant" in df.columns: df["Montant"] = _to_num_series(df["Montant"])
    return safe_unique_columns(df)

def fin_append_row(client, sheet_id: str, title: str, row: dict, kind: str):
    cols = FIN_REV_COLUMNS if kind=="Revenus" else FIN_DEP_COLUMNS
    ws = fin_ensure_ws(client, sheet_id, title, cols, create_if_missing=True)
    header = ws.row_values(1)
    ws.append_row([str(row.get(col, "")) for col in header])

def fmt_date(d: date | None) -> str: return d.strftime("%d/%m/%Y") if isinstance(d, date) else ""

def normalize_tn_phone(s: str) -> str:
    digits = "".join(ch for ch in str(s) if ch.isdigit())
    if digits.startswith("216"): return digits
    if len(digits) == 8: return "216" + digits
    return digits

def format_display_phone(s: str) -> str:
    d = "".join(ch for ch in str(s) if s is not None and ch.isdigit())
    return f"+{d}" if d else ""

def color_tag(val):
    if isinstance(val, str) and val.strip().startswith("#") and len(val.strip()) == 7: return f"background-color: {val}; color: white;"
    return ""

def mark_alert_cell(val: str):
    s = str(val).strip()
    if not s: return ''
    if "متأخر" in s: return 'background-color: #fff3cd; color: #7a4e00'
    return 'background-color: #ffe5e5; color: #7a0000'

def highlight_inscrit_row(row: pd.Series):
    insc = str(row.get("Inscription", "")).strip().lower()
    return ['background-color: #ecfdf5' if insc in ("inscrit","oui") else '' for _ in row.index]

# ---------------- Employee Password Locks ----------------
def _get_emp_password(emp_name: str) -> str:
    try:
        mp = st.secrets["employee_passwords"]; return str(mp.get(emp_name, mp.get("_default", "1234")))
    except Exception: return "1234"

def _emp_unlocked(emp_name: str) -> bool:
    ok = st.session_state.get(f"emp_ok::{emp_name}", False); ts = st.session_state.get(f"emp_ok_at::{emp_name}")
    return bool(ok and ts and (datetime.now() - ts) <= timedelta(minutes=15))

def _emp_lock_ui(emp_name: str):
    with st.expander(f"🔐 حماية ورقة الموظّف: {emp_name}", expanded=not _emp_unlocked(emp_name)):
        if _emp_unlocked(emp_name):
            c1, c2 = st.columns(2)
            with c1: st.success("مفتوح (15 دقيقة).")
            with c2:
                if st.button("قفل الآن"): st.session_state[f"emp_ok::{emp_name}"] = False; st.session_state[f"emp_ok_at::{emp_name}"] = None; st.info("تم القفل.")
        else:
            pwd_try = st.text_input("أدخل كلمة السرّ", type="password", key=f"emp_pwd_{emp_name}")
            if st.button("فتح"):
                if pwd_try and pwd_try == _get_emp_password(emp_name):
                    st.session_state[f"emp_ok::{emp_name}"] = True; st.session_state[f"emp_ok_at::{emp_name}"] = datetime.now(); st.success("تم الفتح لمدة 15 دقيقة.")
                else: st.error("كلمة سرّ غير صحيحة.")

# ---------------- Load all CRM data ----------------
@st.cache_data(ttl=600)
def load_all_data():
    sh = client.open_by_key(SPREADSHEET_ID); worksheets = sh.worksheets()
    all_dfs, all_employes = [], []
    for ws in worksheets:
        title = ws.title.strip()
        if title.endswith("_PAIEMENTS"): continue
        if title.startswith("_"): continue
        if title.startswith("Revenue ") or title.startswith("Dépense "): continue
        if title in (INTER_NOTES_SHEET, "TransferLog"): continue
        all_employes.append(title)
        rows = ws.get_all_values()
        if not rows:
            try: ws.update("1:1", [EXPECTED_HEADERS]); rows = ws.get_all_values()
            except Exception: pass
        data_rows = rows[1:] if len(rows) > 1 else []
        fixed_rows = []
        for r in data_rows:
            r = list(r or [])
            if len(r) < len(EXPECTED_HEADERS): r += [""] * (len(EXPECTED_HEADERS) - len(r))
            else: r = r[:len(EXPECTED_HEADERS)]
            fixed_rows.append(r)
        df = pd.DataFrame(fixed_rows, columns=EXPECTED_HEADERS); df["__sheet_name"] = title; all_dfs.append(df)
    big = pd.concat(all_dfs, ignore_index=True) if all_dfs else pd.DataFrame(columns=EXPECTED_HEADERS + ["__sheet_name"])
    return big, all_employes

df_all, all_employes = load_all_data()

# ---------------- Transfer Log (من نقل/إلى/منفّذ) ----------------
TRANSFER_SHEET = "TransferLog"
TRANSFER_HEADERS = ["timestamp","from_emp","to_emp","client_name","phone","performed_by"]

def transfer_ws():
    sh = client.open_by_key(SPREADSHEET_ID)
    try:
        ws = sh.worksheet(TRANSFER_SHEET)
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title=TRANSFER_SHEET, rows="1000", cols=str(len(TRANSFER_HEADERS)))
        ws.update("1:1", [TRANSFER_HEADERS])
    return ws

def log_transfer(from_emp, to_emp, client_name, phone, performed_by):
    ws = transfer_ws()
    ts = datetime.now(timezone.utc).isoformat()
    ws.append_row([ts, from_emp, to_emp, client_name, phone, performed_by])

@st.cache_data(ttl=300)
def fetch_transfer_log() -> pd.DataFrame:
    ws = transfer_ws()
    values = ws.get_all_values()
    if not values or len(values) <= 1: return pd.DataFrame(columns=TRANSFER_HEADERS)
    df = pd.DataFrame(values[1:], columns=values[0])
    return df

# ---------------- Sidebar ----------------
try: st.sidebar.image(Image.open("logo.png"), use_container_width=True)
except Exception: pass

tab_choice = st.sidebar.radio("📑 اختر تبويب:", ["CRM", "مداخيل (MB/Bizerte)", "📝 نوط داخلية"], index=0)
role = st.sidebar.radio("الدور", ["موظف", "أدمن"], horizontal=True)
employee = st.sidebar.selectbox("👨‍💼 اختر الموظّف (ورقة Google Sheets)", all_employes) if (role == "موظف" and all_employes) else None

# ---------------- Admin lock ----------------
def admin_unlocked() -> bool:
    ok = st.session_state.get("admin_ok", False); ts = st.session_state.get("admin_ok_at", None)
    return bool(ok and ts and (datetime.now() - ts) <= timedelta(minutes=30))

def admin_lock_ui():
    with st.sidebar.expander("🔐 إدارة (Admin)", expanded=(role=="أدمن" and not admin_unlocked())):
        if admin_unlocked():
            if st.button("قفل صفحة الأدمِن"): st.session_state["admin_ok"] = False; st.session_state["admin_ok_at"] = None; st.rerun()
        else:
            admin_pwd = st.text_input("كلمة سرّ الأدمِن", type="password", key="admin_pwd_inp")
            if st.button("فتح صفحة الأدمِن"):
                conf = str(st.secrets.get("admin_password", "admin123"))
                if admin_pwd and admin_pwd == conf: st.session_state["admin_ok"] = True; st.session_state["admin_ok_at"] = datetime.now(); st.success("تم فتح صفحة الأدمِن لمدة 30 دقيقة.")
                else: st.error("كلمة سرّ غير صحيحة.")
if role == "أدمن": admin_lock_ui()

# ---------------- Finance Tab ----------------
@st.cache_data(ttl=300)
def collect_monthly_finance():
    """DF لكل الأشهر (الفرعين) بدون إنشاء أوراق ناقصة."""
    months = FIN_MONTHS_FR
    rows = []
    for branch in ["Menzel Bourguiba", "Bizerte"]:
        for mois in months:
            rev = fin_read_df(client, SPREADSHEET_ID, fin_month_title(mois, "Revenus", branch), "Revenus", create_if_missing=False)
            dep = fin_read_df(client, SPREADSHEET_ID, fin_month_title(mois, "Dépenses", branch), "Dépenses", create_if_missing=False)
            revenus_AS = float(rev["Montant_Total"].sum()) if "Montant_Total" in rev.columns else 0.0
            depenses    = float(dep["Montant"].sum())      if "Montant" in dep.columns else 0.0
            reste       = float(rev["Reste"].sum())        if "Reste" in rev.columns else 0.0
            prix_total  = float(rev["Prix"].sum())         if "Prix" in rev.columns else 0.0
            rows.append({
                "mois": mois, "branch": branch,
                "Total_AplusS": revenus_AS,
                "Total_Dep": depenses,
                "Total_Reste": reste,
                "Montant_Inscrits": prix_total
            })
    df = pd.DataFrame(rows)
    df["mois"] = pd.Categorical(df["mois"], categories=FIN_MONTHS_FR, ordered=True)
    return df.sort_values(["mois", "branch"])

if tab_choice == "مداخيل (MB/Bizerte)":
    ui_section("💸 المداخيل والمصاريف — (منزل بورقيبة & بنزرت)", "💸")
    with st.sidebar:
        st.markdown("---"); st.subheader("🔧 إعدادات المداخيل/المصاريف")
        branch = st.selectbox("الفرع", ["Menzel Bourguiba", "Bizerte"], key="fin_branch")
        kind_ar = st.radio("النوع", ["مداخيل","مصاريف"], horizontal=True, key="fin_kind_ar")
        kind = "Revenus" if kind_ar == "مداخيل" else "Dépenses"
        mois = st.selectbox("الشهر", FIN_MONTHS_FR, index=datetime.now().month-1, key="fin_month")
        BRANCH_PASSWORDS = _branch_passwords(); key_pw = f"finance_pw_ok::{branch}"
        if key_pw not in st.session_state: st.session_state[key_pw] = False
        if not st.session_state[key_pw]:
            pw_try = st.text_input("كلمة سرّ الفرع", type="password", key=f"fin_pw_{branch}")
            if st.button("دخول الفرع", key=f"fin_enter_{branch}"):
                if pw_try and pw_try == BRANCH_PASSWORDS.get(branch, ""): st.session_state[key_pw] = True; st.success("تم الدخول ✅")
                else: st.error("كلمة سرّ غير صحيحة ❌")
    if not st.session_state.get(f"finance_pw_ok::{branch}", False): st.info("⬅️ أدخل كلمة السرّ من اليسار للمتابعة."); st.stop()

    fin_title = fin_month_title(mois, kind, branch)
    df_fin = fin_read_df(client, SPREADSHEET_ID, fin_title, kind); df_view = df_fin.copy()
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

    ui_section(f"{'💰' if kind=='Revenus' else '🧾'} {fin_title}", "🗂️")
    df_view = safe_unique_columns(df_view)
    cols_show = (["Date","Libellé","Prix","Montant_Admin","Montant_Structure","Montant_PreInscription","Montant_]()_
