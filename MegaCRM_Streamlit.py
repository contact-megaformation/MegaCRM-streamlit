# MegaCRM_Streamlit_App_PRO_Light.py
# ===============================================================================================================
# تغييرات النسخة:
# - ثيم فاتح + أزرار 3D واقعية
# - إصلاح عدّاد "مضافين بلا ملاحظات": يستثني المسجّلين (Inscrit)
# - تسجيل اسم الموظّف الذي يقوم بنقل عميل داخل "Remarque" مع طابع زمني
# - للأدمِن: تجميع شهري للمبالغ (Total A+S / مصاريف / Reste / مبلغ المسجّلين)
# - حذف قسم "📝 أضف ملاحظة (سريعة)"

import json, time, urllib.parse, base64, uuid, re
import streamlit as st
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime, date, timedelta, timezone
from PIL import Image

# ---------------- Page config ----------------
st.set_page_config(page_title="MegaCRM", layout="wide", initial_sidebar_state="expanded")

# =============== 🎨 UI SKIN — LIGHT (Clear, Professional, Real Buttons) ===============
def inject_pro_ui():
    st.markdown("""
    <style>
      @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;600;700&display=swap');
      :root{
        --bg:#f7f9fc;         --card:#ffffff;       --text:#1a1f36;       --muted:#5b6b82;
        --border:#e7ecf3;     --accent:#2563eb;     --accent-2:#3b82f6;   --accent-3:#1d4ed8;
        --success:#16a34a;    --warning:#d97706;    --danger:#dc2626;     --radius:14px;
      }
      html, body, [data-testid="stAppViewContainer"]{
        background: var(--bg) !important;
        color: var(--text) !important;
        font-family: 'Inter', system-ui, -apple-system, Segoe UI, Roboto, 'Helvetica Neue', Arial, "Noto Sans", "Liberation Sans", sans-serif !important;
        font-size: 16px !important; line-height: 1.45 !important;
      }
      [data-testid="stSidebar"]{
        background: #fbfdff !important; border-right: 1px solid var(--border) !important;
      }
      /* ---------- Real Buttons (3D look) ---------- */
      .stButton>button, .stDownloadButton>button{
        position: relative !important;
        appearance: none !important;
        border-radius: 12px !important;
        background: linear-gradient(180deg, var(--accent-2), var(--accent)) !important;
        color: #fff !important;
        border: 1px solid #1e40af !important;
        padding: 0.65rem 1.1rem !important;
        font-weight: 800 !important;
        letter-spacing: .2px !important;
        box-shadow:
          0 2px 0 #153e94 inset,
          0 8px 18px rgba(37, 99, 235, 0.25),
          0 0 0 1px rgba(255,255,255,0.6) inset;
        transition: transform .06s ease, box-shadow .12s ease, filter .15s ease;
      }
      .stButton>button:hover, .stDownloadButton>button:hover{
        filter: brightness(1.03);
        box-shadow:
          0 2px 0 #153e94 inset,
          0 10px 22px rgba(37,99,235,0.30),
          0 0 0 1px rgba(255,255,255,0.65) inset;
      }
      .stButton>button:active, .stDownloadButton>button:active{
        transform: translateY(1px);
        box-shadow:
          0 1px 0 #153e94 inset,
          0 6px 14px rgba(37,99,235,0.22),
          0 0 0 1px rgba(255,255,255,0.55) inset;
      }
      .stButton>button:focus-visible, .stDownloadButton>button:focus-visible{
        outline: none !important;
        box-shadow:
          0 2px 0 #153e94 inset,
          0 8px 18px rgba(37,99,235,0.25),
          0 0 0 3px rgba(37,99,235,0.35) !important;
      }
      /* Inputs */
      .stTextInput>div>div>input, .stTextArea textarea, .stSelectbox>div>div>div>div,
      .stDateInput>div>div>input, .stNumberInput input{
        background: #ffffff !important; color: var(--text) !important;
        border-radius: 12px !important; border: 1px solid var(--border) !important;
        box-shadow: 0 1px 0 rgba(0,0,0,0.02) inset !important;
      }
      .stTextArea textarea{ min-height: 110px !important; }
      /* DataFrame container */
      .stDataFrame{
        background: var(--card) !important; border-radius: var(--radius) !important; border: 1px solid var(--border) !important;
        box-shadow: 0 10px 24px rgba(16, 24, 40, 0.06) !important; overflow: hidden !important;
      }
      /* Topbar */
      .topbar{
        border-radius: 18px; padding: 18px 22px; background: linear-gradient(135deg, #ffffff, #f3f7ff);
        border: 1px solid var(--border); box-shadow: 0 12px 30px rgba(16,24,40,0.08); margin-bottom: 14px;
      }
      .topbar h1{ margin:0; font-size: 26px; letter-spacing:.2px; color: var(--text);}
      .topbar p{ margin:8px 0 0; color: var(--muted); }
      /* Section/Card */
      .section{
        background: var(--card); border-radius: var(--radius); border: 1px solid var(--border);
        padding: 14px 16px; margin: 10px 0 18px; box-shadow: 0 8px 20px rgba(16,24,40,0.06);
      }
      .section h3{ margin: 4px 0 12px; color: var(--text); }
      /* KPI Cards */
      .kpi-grid{ display:grid; grid-template-columns: repeat(5, minmax(140px,1fr)); gap:12px; }
      .kpi{ background: #ffffff; border-radius: 14px; padding: 14px; border: 1px solid var(--border); box-shadow: 0 8px 20px rgba(16,24,40,0.06); }
      .kpi .label{ color: var(--muted); font-size: 13px; }
      .kpi .value{ font-size: 22px; font-weight: 800; margin-top: 6px; letter-spacing: .2px; color: var(--text); }
      .kpi.ok   { border-color: rgba(34,197,94,.45); }
      .kpi.warn { border-color: rgba(217,119,6,.35); }
      .kpi.dng  { border-color: rgba(220,38,38,.40); }
      /* Pills */
      .pill{ display:inline-block; padding:4px 10px; border-radius:999px; font-size:12px; border:1px solid var(--border); color: var(--text); background:#fff; }
      .pill.blue{border-color:#bfdbfe; background:#eff6ff;} .pill.green{border-color:#bbf7d0; background:#ecfdf5;}
      .pill.orange{border-color:#fed7aa; background:#fff7ed;} .pill.red{border-color:#fecaca; background:#fef2f2;}
      .muted{ color: var(--muted); font-size: 14px; }
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
top_col1, top_col2 = st.columns([3,1])
with top_col1:
    global_q = st.text_input("ابحث سريعًا...", placeholder="اكتب اسم / هاتف (216XXXXXXXX أو 8 أرقام) / تكوين ...")
with top_col2:
    st.caption("")

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
        sheet_id = "1DV0KyDRYHofWR60zdx63a9BWBywTFhLavGAExPIa6LI"
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

    _autorefresh = getattr(st, "autorefresh", None) or getattr(st, "experimental_autorefresh", None)
    if callable(_autorefresh): _autorefresh(interval=10_000, key="inter_notes_poll")

    if "prev_unread_count" not in st.session_state: st.session_state.prev_unread_count = 0
    unread_df = inter_notes_fetch_unread(current_employee); unread_count = len(unread_df)
    try:
        if unread_count > st.session_state.prev_unread_count: st.toast("📩 نوط جديدة وصْلتك!", icon="✉️"); play_sound_mp3()
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

def fin_ensure_ws(client, sheet_id: str, title: str, columns: list[str]):
    sh = client.open_by_key(sheet_id)
    try: ws = sh.worksheet(title)
    except Exception:
        ws = sh.add_worksheet(title=title, rows="2000", cols=str(max(len(columns), 8))); ws.update("1:1", [columns]); return ws
    rows = ws.get_all_values()
    if not rows: ws.update("1:1", [columns])
    else:
        header = rows[0]
        if not header or header[:len(columns)] != columns: ws.update("1:1", [columns])
    return ws

def _to_num_series(s): 
    return s.astype(str).str.replace(" ", "", regex=False).str.replace(",", ".", regex=False).pipe(pd.to_numeric, errors="coerce").fillna(0.0)

def fin_read_df(client, sheet_id: str, title: str, kind: str) -> pd.DataFrame:
    cols = FIN_REV_COLUMNS if kind == "Revenus" else FIN_DEP_COLUMNS
    ws = fin_ensure_ws(client, sheet_id, title, cols)
    values = ws.get_all_values()
    if not values: return pd.DataFrame(columns=cols)
    df = pd.DataFrame(values[1:], columns=values[0])
    if "Date" in df.columns: df["Date"] = pd.to_datetime(df["Date"], errors="coerce", dayfirst=True)
    if kind == "Revenus" and "Echeance" in df.columns: df["Echeance"] = pd.to_datetime(df["Echeance"], errors="coerce", dayfirst=True)
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
    ws = fin_ensure_ws(client, sheet_id, title, cols)
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

# ---------------- Helpers إضافية للتجميع الشهري والمسجّلين ----------------
def _normalize_name(s: str) -> str:
    return re.sub(r"\s+", " ", str(s or "")).strip().lower()

def extract_client_from_note(note: str) -> str:
    """يستخرج اسم العميل من Note بالصيغة: Client: <name> / <formation>"""
    if not note: return ""
    m = re.search(r"client\s*:\s*([^/\n\r]+)", str(note), flags=re.IGNORECASE)
    return _normalize_name(m.group(1)) if m else ""

@st.cache_data(ttl=600)
def collect_all_revenus(client, sheet_id: str) -> pd.DataFrame:
    sh = client.open_by_key(sheet_id)
    dfs = []
    for ws in sh.worksheets():
        title = ws.title.strip()
        if title.startswith("Revenue "):
            df = fin_read_df(client, sheet_id, title, "Revenus")
            if not df.empty:
                df["__sheet_title"] = title
                dfs.append(df)
    if not dfs:
        return pd.DataFrame()
    big = pd.concat(dfs, ignore_index=True)
    big["Month"] = pd.to_datetime(big.get("Date"), errors="coerce").dt.strftime("%Y-%m")
    big["Client_extracted"] = big.get("Note", "").apply(extract_client_from_note)
    return big

@st.cache_data(ttl=600)
def collect_all_depenses(client, sheet_id: str) -> pd.DataFrame:
    sh = client.open_by_key(sheet_id)
    dfs = []
    for ws in sh.worksheets():
        title = ws.title.strip()
        if title.startswith("Dépense "):
            df = fin_read_df(client, sheet_id, title, "Dépenses")
            if not df.empty:
                df["__sheet_title"] = title
                dfs.append(df)
    if not dfs:
        return pd.DataFrame()
    big = pd.concat(dfs, ignore_index=True)
    big["Month"] = pd.to_datetime(big.get("Date"), errors="coerce").dt.strftime("%Y-%m")
    return big

def build_monthly_totals(df_all_crm: pd.DataFrame) -> pd.DataFrame:
    """يرجّع جدول شهري فيه:
       Month | Total A+S (مداخيل) | Total مصاريف | إجمالي Reste بالدروس | إجمالي مبلغ المسجّلين
    """
    rev = collect_all_revenus(client, SPREADSHEET_ID)
    dep = collect_all_depenses(client, SPREADSHEET_ID)

    # --- Total A+S (مداخيل) و Reste ---
    rev_grp = pd.DataFrame()
    if not rev.empty:
        rev_grp = (rev.groupby("Month", dropna=False)
                     .agg(**{
                         "Total A+S (مداخيل)": ("Montant_Total", "sum"),
                         "إجمالي Reste بالدروس": ("Reste", "sum"),
                     })
                     .reset_index())

    # --- Total مصاريف ---
    dep_grp = pd.DataFrame()
    if not dep.empty:
        dep_grp = (dep.groupby("Month", dropna=False)
                     .agg(**{"Total مصاريف": ("Montant", "sum")})
                     .reset_index())

    # --- إجمالي مبلغ المسجّلين ---
    inscrit_total = pd.DataFrame()
    if not df_all_crm.empty and not rev.empty:
        crm = df_all_crm.copy()
        crm["Inscription_norm"] = crm["Inscription"].fillna("").astype(str).str.strip().str.lower()
        inscrit_names = set(_normalize_name(n) for n in crm.loc[crm["Inscription_norm"].isin(["oui","inscrit"]), "Nom & Prénom"].astype(str))
        if inscrit_names:
            rev_inscrit = rev[rev["Client_extracted"].isin(inscrit_names)].copy()
            if not rev_inscrit.empty:
                inscrit_total = (rev_inscrit.groupby("Month", dropna=False)
                                 .agg(**{"إجمالي مبلغ المسجّلين": ("Montant_Total", "sum")})
                                 .reset_index())

    # دمج الجداول
    out = None
    for part in [rev_grp, dep_grp, inscrit_total]:
        if part is None or part.empty:
            continue
        out = part if out is None else pd.merge(out, part, on="Month", how="outer")

    if out is None:
        return pd.DataFrame(columns=["Month","Total A+S (مداخيل)","Total مصاريف","إجمالي Reste بالدروس","إجمالي مبلغ المسجّلين"])

    # أصفار بدل NaN
    for c in ["Total A+S (مداخيل)","Total مصاريف","إجمالي Reste بالدروس","إجمالي مبلغ المسجّلين"]:
        if c in out.columns:
            out[c] = pd.to_numeric(out[c], errors="coerce").fillna(0.0)

    # ترتيب نزولي حسب الشهر (نوفمبر، أكتوبر، ...)
    out["_dt"] = pd.to_datetime(out["Month"] + "-01", errors="coerce")
    out = out.sort_values("_dt", ascending=False).drop(columns=["_dt"]).reset_index(drop=True)
    return out

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

# ---------------- Load all CRM data (hide non-employee sheets) ----------------
@st.cache_data(ttl=600)
def load_all_data():
    sh = client.open_by_key(SPREADSHEET_ID); worksheets = sh.worksheets()
    all_dfs, all_employes = [], []
    for ws in worksheets:
        title = ws.title.strip()
        if title.endswith("_PAIEMENTS"): continue
        if title.startswith("_"): continue
        if title.startswith("Revenue ") or title.startswith("Dépense "): continue
        if title == INTER_NOTES_SHEET: continue
        all_employes.append(title)
        rows = ws.get_all_values()
        if not rows: ws.update("1:1", [EXPECTED_HEADERS]); rows = ws.get_all_values()
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

# ---------------- "مداخيل (MB/Bizerte)" Tab ----------------
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
    cols_show = (["Date","Libellé","Prix","Montant_Admin","Montant_Structure","Montant_PreInscription","Montant_Total","Echeance","Reste","Alert","Mode","Employé","Catégorie","Note"]
                 if kind=="Revenus" else
                 ["Date","Libellé","Montant","Caisse_Source","Mode","Employé","Catégorie","Note"])
    cols_show = [c for c in cols_show if c in df_view.columns]
    st.dataframe(df_view[cols_show] if not df_view.empty else pd.DataFrame(columns=cols_show), use_container_width=True)
    ui_section_end()

    if role == "أدمن" and admin_unlocked():
        rev_df = fin_read_df(client, SPREADSHEET_ID, fin_month_title(mois, "Revenus", branch), "Revenus")
        dep_df = fin_read_df(client, SPREADSHEET_ID, fin_month_title(mois, "Dépenses", branch), "Dépenses")
        sum_admin    = rev_df["Montant_Admin"].sum()           if ("Montant_Admin" in rev_df.columns and not rev_df.empty) else 0.0
        sum_struct   = rev_df["Montant_Structure"].sum()       if ("Montant_Structure" in rev_df.columns and not rev_df.empty) else 0.0
        sum_preins   = rev_df["Montant_PreInscription"].sum()  if ("Montant_PreInscription" in rev_df.columns and not rev_df.empty) else 0.0
        sum_total_as = rev_df["Montant_Total"].sum()           if ("Montant_Total" in rev_df.columns and not rev_df.empty) else (sum_admin + sum_struct)
        sum_reste_due= rev_df["Reste"].sum()                   if ("Reste" in rev_df.columns and not rev_df.empty) else 0.0
        if not dep_df.empty and "Caisse_Source" in dep_df.columns and "Montant" in dep_df.columns:
            dep_admin  = dep_df.loc[dep_df["Caisse_Source"]=="Caisse_Admin", "Montant"].sum()
            dep_struct = dep_df.loc[dep_df["Caisse_Source"]=="Caisse_Structure", "Montant"].sum()
            dep_inscr  = dep_df.loc[dep_df["Caisse_Source"]=="Caisse_Inscription", "Montant"].sum()
        else: dep_admin = dep_struct = dep_inscr = 0.0
        reste_admin  = float(sum_admin)  - float(dep_admin)
        reste_struct = float(sum_struct) - float(dep_struct)
        reste_inscr  = float(sum_preins) - float(dep_inscr)

        ui_section("ملخّص الفرع للشهر — Admin", "🧮")
        ui_kpis([
            {"label":"مداخيل Admin","value": f"{sum_admin:,.2f}","tone":"ok"},
            {"label":"مصاريف Admin","value": f"{dep_admin:,.2f}","tone":"warn"},
            {"label":"Reste Admin","value": f"{reste_admin:,.2f}","tone":"ok" if reste_admin>=0 else "dng"},
        ])
        ui_kpis([
            {"label":"مداخيل Structure","value": f"{sum_struct:,.2f}","tone":"ok"},
            {"label":"مصاريف Structure","value": f"{dep_struct:,.2f}","tone":"warn"},
            {"label":"Reste Structure","value": f"{reste_struct:,.2f}","tone":"ok" if reste_struct>=0 else "dng"},
        ])
        ui_kpis([
            {"label":"مداخيل Inscription","value": f"{sum_preins:,.2f}","tone":"ok"},
            {"label":"مصاريف Inscription","value": f"{dep_inscr:,.2f}","tone":"warn"},
            {"label":"Reste Inscription","value": f"{reste_inscr:,.2f}","tone":"ok" if reste_inscr>=0 else "dng"},
        ])
        ui_kpis([
            {"label":"Total A+S (مداخيل)","value": f"{sum_total_as:,.2f}","tone":"blue"},
            {"label":"Total مصاريف","value": f"{(dep_admin+dep_struct+dep_inscr):, .2f}","tone":"warn"},
            {"label":"إجمالي Reste بالدروس","value": f"{sum_reste_due:,.2f}","tone":"orange"},
        ])
        ui_section_end()

    st.markdown("---"); ui_section("➕ إضافة عملية جديدة", "➕")
    selected_client_info = None; client_default_lib = ""; emp_default = (employee or "")
    if kind == "Revenus":
        st.markdown("#### 👤 اربط الدفعة بعميل مُسجَّل (اختياري)")
        reg_df = df_all.copy(); reg_df["Inscription_norm"] = reg_df["Inscription"].fillna("").astype(str).str.strip().str.lower()
        reg_df = reg_df[reg_df["Inscription_norm"].isin(["oui","inscrit"])]
        if role == "موظف" and employee: reg_df = reg_df[reg_df["__sheet_name"] == employee]
        if not reg_df.empty:
            def _opt(row): return f"{row.get('Nom & Prénom','')} — {format_display_phone(row.get('Téléphone',''))} — {row.get('Formation','')}  [{row.get('__sheet_name','')}]"
            options = [_opt(r) for _, r in reg_df.iterrows()]
            pick = st.selectbox("اختر عميلًا مُسجَّلًا", ["— بدون اختيار —"] + options, key="fin_client_pick")
            if pick and pick != "— بدون اختيار —":
                idx = options.index(pick); row = reg_df.iloc[idx]
                selected_client_info = {"name":str(row.get("Nom & Prénom","")).strip(),"tel":str(row.get("Téléphone","")).strip(),"formation":str(row.get("Formation","")).strip(),"emp":str(row.get("__sheet_name","")).strip()}
                client_default_lib = f"Paiement {selected_client_info['formation']} - {selected_client_info['name']}".strip()
                if not emp_default: emp_default = selected_client_info["emp"]

    with st.form("fin_add_row"):
        d1, d2, d3 = st.columns(3)
        date_val = d1.date_input("Date", value=datetime.today())
        libelle  = d2.text_input("Libellé", value=(client_default_lib if kind=="Revenus" else ""))
        employe  = d3.selectbox("Employé", all_employes if all_employes else [""], index=(all_employes.index(emp_default) if (emp_default in all_employes) else 0) if all_employes else 0)

        if kind == "Revenus":
            r1, r2, r3 = st.columns(3)
            prix = r1.number_input("💰 Prix (سعر التكوين)", min_value=0.0, step=10.0)
            montant_admin  = r2.number_input("🏢 Montant Admin", min_value=0.0, step=10.0)
            montant_struct = r3.number_input("🏫 Montant Structure", min_value=0.0, step=10.0)
            r4, r5 = st.columns(2)
            montant_preins = r4.number_input("📝 Montant Pré-Inscription", min_value=0.0, step=10.0, help="اختياري")
            montant_total  = float(montant_admin) + float(montant_struct)
            e1, e2, e3 = st.columns(3)
            echeance = e1.date_input("⏰ تاريخ الاستحقاق", value=date.today())
            mode     = e2.selectbox("Mode", ["Espèces","Virement","Carte","Chèque","Autre"])
            categorie= e3.text_input("Catégorie", value="Revenus")
            note_default = f"Client: {selected_client_info['name']} / {selected_client_info['formation']}" if selected_client_info else ""
            note = st.text_area("Note", value=note_default)
            rev_df_current = fin_read_df(client, SPREADSHEET_ID, fin_title, "Revenus")
            paid_so_far = 0.0
            if not rev_df_current.empty and "Libellé" in rev_df_current.columns and "Montant_Total" in rev_df_current.columns:
                same = rev_df_current[rev_df_current["Libellé"].fillna("").str.strip().str.lower() == libelle.strip().lower()]
                paid_so_far = float(same["Montant_Total"].sum()) if not same.empty else 0.0
            reste_calc = max(float(prix) - (paid_so_far + float(montant_total)), 0.0)
            reste_input = st.number_input("💳 الباقي للدفع (Reste)", min_value=0.0, value=float(round(reste_calc,2)), step=10.0, help="يتحسب آليًا، وتنجم تبدّلو يدويًا")
            st.caption(f"💡 Total (Admin+Structure): {montant_total:.2f} — مدفوع سابقًا لنفس Libellé: {paid_so_far:.2f} — Reste المقترح: {reste_calc:.2f} — Pré-Inscription منفصل: {montant_preins:.2f}")
            if st.form_submit_button("✅ حفظ العملية"):
                if not libelle.strip(): st.error("Libellé مطلوب.")
                elif prix <= 0: st.error("Prix مطلوب.")
                elif montant_total <= 0 and montant_preins <= 0: st.error("المبلغ لازم > 0 (Admin/Structure أو Pré-Inscription).")
                else:
                    fin_append_row(client, SPREADSHEET_ID, fin_title, {
                        "Date": fmt_date(date_val), "Libellé": libelle.strip(), "Prix": f"{float(prix):.2f}",
                        "Montant_Admin": f"{float(montant_admin):.2f}", "Montant_Structure": f"{float(montant_struct):.2f}",
                        "Montant_PreInscription": f"{float(montant_preins):.2f}", "Montant_Total": f"{float(montant_total):.2f}",
                        "Echeance": fmt_date(echeance), "Reste": f"{float(reste_input):.2f}", "Mode": mode,
                        "Employé": employe.strip(), "Catégorie": categorie.strip(), "Note": note.strip(),
                    }, "Revenus")
                    st.success("تمّ الحفظ ✅"); st.cache_data.clear(); st.rerun()
        else:
            r1, r2, r3 = st.columns(3)
            montant = r1.number_input("Montant", min_value=0.0, step=10.0)
            caisse  = r2.selectbox("Caisse_Source", ["Caisse_Admin","Caisse_Structure","Caisse_Inscription"])
            mode    = r3.selectbox("Mode", ["Espèces","Virement","Carte","Chèque","Autre"])
            c2, c3 = st.columns(2)
            categorie = c2.text_input("Catégorie", value="Achat")
            note      = c3.text_area("Note (اختياري)")
            if st.form_submit_button("✅ حفظ العملية"):
                if not libelle.strip(): st.error("Libellé مطلوب.")
                elif montant <= 0: st.error("المبلغ لازم > 0.")
                else:
                    fin_append_row(client, SPREADSHEET_ID, fin_title, {
                        "Date": fmt_date(date_val), "Libellé": libelle.strip(), "Montant": f"{float(montant):.2f}",
                        "Caisse_Source": caisse, "Mode": mode, "Employé": employe.strip(), "Catégorie": categorie.strip(), "Note": note.strip(),
                    }, "Dépenses")
                    st.success("تمّ الحفظ ✅"); st.cache_data.clear(); st.rerun()
    ui_section_end()

# ---------------- CRM: مشتقّات وعرض ----------------
df_all = df_all.copy()
if not df_all.empty:
    df_all["DateAjout_dt"] = pd.to_datetime(df_all["Date ajout"], dayfirst=True, errors="coerce")
    df_all["DateSuivi_dt"] = pd.to_datetime(df_all["Date de suivi"], dayfirst=True, errors="coerce")
    df_all["Mois"] = df_all["DateAjout_dt"].dt.strftime("%m-%Y")
    today = datetime.now().date()
    base_alert = df_all["Alerte"].fillna("").astype(str).str.strip()
    dsv_date = df_all["DateSuivi_dt"].dt.date
    due_today = dsv_date.eq(today).fillna(False); overdue = dsv_date.lt(today).fillna(False)
    df_all["Alerte_view"] = base_alert
    df_all.loc[base_alert.eq("") & overdue, "Alerte_view"] = "⚠️ متابعة متأخرة"
    df_all.loc[base_alert.eq("") & due_today, "Alerte_view"] = "⏰ متابعة اليوم"
    df_all["Téléphone_norm"] = df_all["Téléphone"].apply(normalize_tn_phone)
    ALL_PHONES = set(df_all["Téléphone_norm"].dropna().astype(str))
    df_all["Inscription_norm"] = df_all["Inscription"].fillna("").astype(str).str.strip().str.lower()
    inscrit_mask = df_all["Inscription_norm"].isin(["oui","inscrit"])
    df_all.loc[inscrit_mask, "Date de suivi"] = ""; df_all.loc[inscrit_mask, "Alerte_view"] = ""
else:
    df_all["Alerte_view"] = ""; df_all["Mois"] = ""; df_all["Téléפון_norm"] = ""; ALL_PHONES = set()

# ---------------- Dashboard KPIs ----------------
ui_section("لوحة الإحصائيات السريعة", "📈")
df_dash = df_all.copy()
if df_dash.empty:
    st.info("ما فماش داتا للعرض.")
else:
    df_dash["DateAjout_dt"] = pd.to_datetime(df_dash.get("Date ajout"), dayfirst=True, errors="coerce")
    df_dash["Inscription_norm"] = df_dash["Inscription"].fillna("").astype(str).str.strip().str.lower()
    df_dash["Alerte_norm"] = df_dash["Alerte_view"].fillna("").astype(str).str.strip()
    today = datetime.now().date()
    added_today_mask = df_dash["DateAjout_dt"].dt.date.eq(today)
    registered_today_mask = df_dash["Inscription_norm"].isin(["oui","inscrit"]) & added_today_mask
    alert_now_mask = df_dash["Alerte_norm"].ne("")
    total_clients = int(len(df_dash))
    added_today = int(added_today_mask.sum())
    registered_today = int(registered_today_mask.sum())
    alerts_now = int(alert_now_mask.sum())
    registered_total = int((df_dash["Inscription_norm"] == "oui").sum())
    rate = round((registered_total / total_clients) * 100, 2) if total_clients else 0.0
    ui_kpis([
        {"label":"👥 إجمالي العملاء","value": f"{total_clients}", "tone":"ok"},
        {"label":"🆕 المضافون اليوم","value": f"{added_today}", "tone": "blue"},
        {"label":"✅ المسجّلون اليوم","value": f"{registered_today}", "tone": "ok"},
        {"label":"🚨 التنبيهات الحالية","value": f"{alerts_now}", "tone": "warn" if alerts_now else "ok"},
        {"label":"📈 نسبة التسجيل الإجمالية","value": f"{rate}%", "tone": "ok" if rate>=25 else "warn"},
    ])
ui_section_end()

# ---------------- Stats per employee ----------------
df_stats = df_all.copy()
df_stats["Inscription_norm"] = df_stats["Inscription"].fillna("").astype(str).str.strip().str.lower()
df_stats["Alerte_norm"] = df_stats["Alerte_view"].fillna("").astype(str).str.strip()
df_stats["DateAjout_dt"] = pd.to_datetime(df_stats.get("Date ajout"), dayfirst=True, errors="coerce")
today = datetime.now().date()
added_today_mask = df_stats["DateAjout_dt"].dt.date.eq(today)
registered_today_mask = df_stats["Inscription_norm"].isin(["oui","inscrit"]) & added_today_mask
alert_now_mask = df_stats["Alerte_norm"].ne("")
df_stats["__added_today"] = added_today_mask; df_stats["__reg_today"] = registered_today_mask; df_stats["__has_alert"] = alert_now_mask

grp_base = (df_stats.groupby("__sheet_name", dropna=False)
    .agg(Clients=("Nom & Prénom","count"),
         Inscrits=("Inscription_norm", lambda x: (x=="oui").sum()),
         تنبيهات=("__has_alert","sum"),
         مضافون_اليوم=("__added_today","sum"),
         مسجلون_اليوم=("__reg_today","sum")).reset_index()
    .rename(columns={"__sheet_name":"الموظف"}))
grp_base["% تسجيل"] = ((grp_base["Inscrits"] / grp_base["Clients"]).replace([float("inf"), float("nan")], 0) * 100).round(2)
grp_base = grp_base.sort_values(by=["تنبيهات","Clients"], ascending=[False, False])

ui_section("حسب الموظّف", "🧑‍💼"); st.dataframe(grp_base, use_container_width=True); ui_section_end()

# ---------------- Global phone search ----------------
ui_section("🔎 بحث عام برقم الهاتف", "🔎")
global_phone = st.text_input("اكتب رقم الهاتف (8 أرقام محلية أو 216XXXXXXXX)", key="global_phone_all")
if global_phone.strip():
    q_norm = normalize_tn_phone(global_phone)
    search_df = df_all.copy(); search_df["Téléphone_norm"] = search_df["Téléphone"].apply(normalize_tn_phone)
    search_df["Alerte"] = search_df.get("Alerte_view", "")
    search_df = search_df[search_df["Téléphone_norm"] == q_norm]
    if search_df.empty: st.info("❕ ما لقيتش عميل بهذا الرقم.")
    else:
        display_cols = [c for c in EXPECTED_HEADERS if c in search_df.columns]
        if "Employe" in search_df.columns and "Employe" not in display_cols: display_cols.append("Employe")
        styled_global = (search_df[display_cols].style
                         .apply(highlight_inscrit_row, axis=1)
                         .applymap(mark_alert_cell, subset=["Alerte"]))
        st.dataframe(styled_global, use_container_width=True)
st.markdown("---"); ui_section_end()

# ---------------- Employee area ----------------
if role == "موظف" و employee:
    _emp_lock_ui(employee)
    if not _emp_unlocked(employee): st.info("🔒 أدخل كلمة سرّ الموظّف في أعلى هذا القسم لفتح الورقة."); st.stop()

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

    def render_table(df_disp: pd.DataFrame, title="📋 قائمة العملاء"):
        ui_section(title, "📋")
        if df_disp.empty: st.info("لا توجد بيانات.")
        else:
            _df = df_disp.copy(); _df["Alerte"] = _df.get("Alerte_view", "")
            display_cols = [c for c in EXPECTED_HEADERS if c in _df.columns]
            styled = (_df[display_cols].style
                      .apply(highlight_inscrit_row, axis=1)
                      .applymap(mark_alert_cell, subset=["Alerte"])
                      .applymap(color_tag, subset=["Tag"]))
            st.dataframe(styled, use_container_width=True)
        ui_section_end()

    if not filtered_df.empty:
        # إصلاح العدّاد: استثناء المسجّلين
        filtered_df["Inscription_norm"] = filtered_df["Inscription"].fillna("").astype(str).str.strip().str.lower()
        pending_mask = (
            (filtered_df["Remarque"].fillna("").astype(str).str.strip() == "") &
            (~filtered_df["Inscription_norm"].isin(["oui","inscrit"]))
        )
        ui_badge(f"⏳ مضافين بلا ملاحظات (غير مسجّلين): {int(pending_mask.sum())}", "orange")
        formations = sorted([f for f in filtered_df["Formation"].dropna().astype(str).unique() if f.strip()])
        formation_choice = st.selectbox("📚 فلترة بالتكوين", ["الكل"] + formations)
        if formation_choice != "الكل":
            filtered_df = filtered_df[filtered_df["Formation"].astype(str) == formation_choice]

    render_table(filtered_df, "📋 قائمة العملاء")

    if not filtered_df.empty and st.checkbox("🔴 عرض العملاء الذين لديهم تنبيهات"):
        _df = filtered_df.copy(); _df["Alerte"] = _df.get("Alerte_view", "")
        alerts_df = _df[_df["Alerte"].fillna("").astype(str).str.strip() != ""]
        render_table(alerts_df, "🚨 عملاء مع تنبيهات")

    # ✏️ تعديل بيانات عميل
    if not df_emp.empty:
        ui_section("✏️ تعديل بيانات عميل", "✏️")
        df_emp_edit = df_emp.copy(); df_emp_edit["Téléphone_norm"] = df_emp_edit["Téléphone"].apply(normalize_tn_phone)
        phone_choices = { f"[{i}] {row['Nom & Prénom']} — {format_display_phone(row['Téléphone'])}": row["Téléphone_norm"]
                          for i, row in df_emp_edit.iterrows() if str(row["Téléphone"]).strip() != "" }
        if phone_choices:
            chosen_key = st.selectbox("اختر العميل (بالاسم/الهاتف)", list(phone_choices.keys()), key="edit_pick")
            chosen_phone = phone_choices.get(chosen_key, "")
            cur_row = df_emp_edit[df_emp_edit["Téléphone_norm"] == chosen_phone].iloc[0] if chosen_phone else None

            cur_name = str(cur_row["Nom & Prénom"]) if cur_row is not None else ""
            cur_tel_raw = str(cur_row["Téléphone"]) if cur_row is not None else ""
            cur_formation = str(cur_row["Formation"]) if cur_row is not None else ""
            cur_remark = str(cur_row.get("Remarque", "")) if cur_row is not None else ""
            cur_ajout = pd.to_datetime(cur_row["Date ajout"], dayfirst=True, errors="coerce").date() if cur_row is not None else date.today()
            cur_suivi = pd.to_datetime(cur_row["Date de suivi"], dayfirst=True, errors="coerce").date() if cur_row is not None and str(cur_row["Date de suivi"]).strip() else date.today()
            cur_insc  = str(cur_row["Inscription"]).strip().lower() if cur_row is not None else ""

            name_key=f"edit_name_txt::{chosen_phone}"; phone_key=f"edit_phone_txt::{chosen_phone}"
            form_key=f"edit_formation_txt::{chosen_phone}"; ajout_key=f"edit_ajout_dt::{chosen_phone}"
            suivi_key=f"edit_suivi_dt::{chosen_phone}"; insc_key=f"edit_insc_sel::{chosen_phone}"
            remark_key=f"edit_remark_txt::{chosen_phone}"; note_key=f"append_note_txt::{chosen_phone}"

            col1, col2 = st.columns(2)
            with col1:
                new_name = st.text_input("👤 الاسم و اللقب", value=cur_name, key=name_key)
                new_phone_raw = st.text_input("📞 رقم الهاتف", value=cur_tel_raw, key=phone_key)
                new_formation = st.text_input("📚 التكوين", value=cur_formation, key=form_key)
            with col2:
                new_ajout = st.date_input("🕓 تاريخ الإضافة", value=cur_ajout, key=ajout_key)
                new_suivi = st.date_input("📆 تاريخ المتابعة", value=cur_suivi, key=suivi_key)
                new_insc = st.selectbox("🟢 التسجيل", ["Pas encore", "Inscrit"], index=(1 if cur_insc == "oui" else 0), key=insc_key)

            new_remark_full = st.text_area("🗒️ ملاحظة (استبدال كامل)", value=cur_remark, key=remark_key)
            extra_note = st.text_area("➕ أضف ملاحظة جديدة (طابع زمني)", placeholder="اكتب ملاحظة لإلحاقها…", key=note_key)

            def find_row_by_phone(ws, phone_digits: str) -> int | None:
                values = ws.get_all_values()
                if not values: return None
                header = values[0]
                if "Téléphone" not in header: return None
                tel_idx = header.index("Téléphone")
                for i, r in enumerate(values[1:], start=2):
                    if len(r) > tel_idx and normalize_tn_phone(r[tel_idx]) == phone_digits: return i
                return None

            if st.button("💾 حفظ التعديلات", key="save_all_edits"):
                try:
                    ws = client.open_by_key(SPREADSHEET_ID).worksheet(employee)
                    row_idx = find_row_by_phone(ws, normalize_tn_phone(chosen_phone))
                    if not row_idx: st.error("❌ تعذّر إيجاد الصف لهذا الهاتف.")
                    else:
                        col_map = {h: EXPECTED_HEADERS.index(h) + 1 for h in ["Nom & Prénom","Téléphone","Formation","Date ajout","Date de suivi","Inscription","Remarque"]}
                        new_phone_norm = normalize_tn_phone(new_phone_raw)
                        if not new_name.strip(): st.error("❌ الاسم و اللقب إجباري."); st.stop()
                        if not new_phone_norm.strip(): st.error("❌ رقم الهاتف إجباري."); st.stop()
                        phones_except_current = set(df_all["Téléphone_norm"]) - {normalize_tn_phone(chosen_phone)}
                        if new_phone_norm in phones_except_current: st.error("⚠️ الرقم موجود مسبقًا."); st.stop()
                        ws.update_cell(row_idx, col_map["Nom & Prénom"], new_name.strip())
                        ws.update_cell(row_idx, col_map["Téléphone"], new_phone_norm)
                        ws.update_cell(row_idx, col_map["Formation"], new_formation.strip())
                        ws.update_cell(row_idx, col_map["Date ajout"], fmt_date(new_ajout))
                        ws.update_cell(row_idx, col_map["Date de suivi"], fmt_date(new_suivi))
                        ws.update_cell(row_idx, col_map["Inscription"], "Oui" if new_insc == "Inscrit" else "Pas encore")
                        if new_remark_full.strip() != cur_remark.strip(): ws.update_cell(row_idx, col_map["Remarque"], new_remark_full.strip())
                        if extra_note.strip():
                            old_rem = ws.cell(row_idx, col_map["Remarque"]).value or ""
                            stamp = datetime.now().strftime("%d/%m/%Y %H:%M")
                            appended = (old_rem + "\n" if old_rem else "") + f"[{stamp}] {extra_note.strip()}"
                            ws.update_cell(row_idx, col_map["Remarque"], appended)
                        st.success("✅ تم حفظ التعديلات"); st.cache_data.clear()
                except Exception as e:
                    st.error(f"❌ خطأ أثناء التعديل: {e}")
        ui_section_end()

    # 🎨 Tag فقط (تم حذف ملاحظة سريعة حسب طلبك)
    if not df_emp.empty:
        ui_section("🎨 اختر لون/Tag للعميل", "🎨")
        scope_df = (filtered_df if not filtered_df.empty else df_emp).copy()
        scope_df["Téléphone_norm"] = scope_df["Téléphone"].apply(normalize_tn_phone)
        tel_color_key = st.selectbox("اختر العميل",
            [f"{r['Nom & Prénom']} — {format_display_phone(normalize_tn_phone(r['Téléphone']))}" for _, r in scope_df.iterrows()],
            key="tag_select")
        tel_color = normalize_tn_phone(tel_color_key.split("—")[-1])
        hex_color = st.color_picker("اختر اللون")
        if st.button("🖌️ تلوين"):
            try:
                ws = client.open_by_key(SPREADSHEET_ID).worksheet(employee); values = ws.get_all_values(); header = values[0] if values else []; row_idx = None
                if "Téléphone" in header:
                    tel_idx = header.index("Téléphone")
                    for i, r in enumerate(values[1:], start=2):
                        if len(r) > tel_idx and normalize_tn_phone(r[tel_idx]) == tel_color: row_idx = i; break
                if not row_idx: st.error("❌ لم يتم إيجاد العميل.")
                else:
                    color_cell = EXPECTED_HEADERS.index("Tag") + 1; ws.update_cell(row_idx, color_cell, hex_color); st.success("✅ تم التلوين"); st.cache_data.clear()
            except Exception as e: st.error(f"❌ خطأ: {e}")
        ui_section_end()

    # ➕ أضف عميل جديد
    ui_section("➕ أضف عميل جديد", "➕")
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
            except Exception as e: st.error(f"❌ خطأ أثناء الإضافة: {e}")
    ui_section_end()

    # 🔁 نقل + WhatsApp
    ui_section("🔁 نقل عميل بين الموظفين", "🔁")
    if all_employes:
        colRA, colRB = st.columns(2)
        with colRA: src_emp = st.selectbox("من موظّف", all_employes, key="reassign_src")
        with colRB: dst_emp = st.selectbox("إلى موظّف", [e for e in all_employes if e != src_emp], key="reassign_dst")
        df_src = df_all[df_all["__sheet_name"] == src_emp].copy()
        if df_src.empty: st.info("❕ لا يوجد عملاء عند هذا الموظّف.")
        else:
            pick = st.selectbox("اختر العميل للنقل", [f"{r['Nom & Prénom']} — {format_display_phone(r['Téléphone'])}" for _, r in df_src.iterrows()], key="reassign_pick")
            phone_pick = normalize_tn_phone(pick.split("—")[-1])
            if st.button("🚚 نقل الآن"):
                try:
                    sh = client.open_by_key(SPREADSHEET_ID); ws_src, ws_dst = sh.worksheet(src_emp), sh.worksheet(dst_emp)
                    values = ws_src.get_all_values(); header = values[0] if values else []; row_idx = None
                    if "Téléphone" in header:
                        tel_idx = header.index("Téléphone")
                        for i, r in enumerate(values[1:], start=2):
                            if len(r) > tel_idx and normalize_tn_phone(r[tel_idx]) == phone_pick: row_idx = i; break
                    if not row_idx: st.error("❌ لم يتم العثور على هذا العميل.")
                    else:
                        row_values = ws_src.row_values(row_idx)
                        if len(row_values) < len(EXPECTED_HEADERS): row_values += [""] * (len(EXPECTED_HEADERS) - len(row_values))
                        row_values = row_values[:len(EXPECTED_HEADERS)]
                        row_values[EXPECTED_HEADERS.index("Employe")] = dst_emp
                        ws_dst.append_row(row_values); ws_src.delete_rows(row_idx)

                        # ✍️ ملاحظة تلقائية باسم الفاعل ومن → إلى
                        dst_values = ws_dst.get_all_values()
                        dst_header = dst_values[0] if dst_values else []
                        if "Remarque" in dst_header:
                            rem_col = dst_header.index("Remarque") + 1
                            last_row_idx = len(dst_values)  # الصف الأخير بعد append
                            old_remark = ws_dst.cell(last_row_idx, rem_col).value or ""
                            stamp = datetime.now().strftime("%d/%m/%Y %H:%M")
                            actor = st.session_state.get("current_user", "Admin")
                            moved_note = f"[{stamp}] نقل عميل بواسطة: {actor} — من {src_emp} إلى {dst_emp}"
                            new_remark = (old_remark + "\n" if old_remark else "") + moved_note
                            ws_dst.update_cell(last_row_idx, rem_col, new_remark)

                        st.success(f"✅ نقل ({row_values[0]}) من {src_emp} إلى {dst_emp}"); st.cache_data.clear()
                except Exception as e: st.error(f"❌ خطأ أثناء النقل: {e}")
    ui_section_end()

    ui_section("💬 تواصل WhatsApp", "💬")
    if not df_emp.empty:
        wa_pick = st.selectbox("اختر العميل لفتح واتساب",
                               [f"{r['Nom & Prénom']} — {format_display_phone(r['Téléphone'])}" for _, r in df_emp.iterrows()],
                               key="wa_pick")
        default_msg = "سلام! معاك Mega Formation. بخصوص التكوين، نحبّوا ننسّقو معاك موعد المتابعة. 👍"
        wa_msg = st.text_area("الرسالة (WhatsApp)", value=default_msg, key="wa_msg")
        if st.button("📲 فتح WhatsApp"):
            try:
                raw_tel = wa_pick.split("—")[-1]; tel_norm = normalize_tn_phone(raw_tel)
                url = f"https://wa.me/{tel_norm}?text={urllib.parse.quote(wa_msg)}"
                st.markdown(f"[افتح المحادثة الآن]({url})")
                st.info("اضغط على الرابط لفتح واتساب في نافذة/تبويب جديد.")
            except Exception as e: st.error(f"❌ تعذّر إنشاء رابط واتساب: {e}")
    ui_section_end()

# ---------------- 📝 نوط داخلية Tab ----------------
if tab_choice == "📝 نوط داخلية":
    current_emp_name = (employee if (role == "موظف" and employee) else "Admin"); is_admin_user = (role == "أدمن")
    inter_notes_ui(current_employee=current_emp_name, all_employees=all_employes, is_admin=is_admin_user)

# ---------------- Admin Page ----------------
if role == "أدمن":
    ui_section("👑 لوحة الأدمِن", "👑")
    if not admin_unlocked():
        st.info("🔐 أدخل كلمة سرّ الأدمِن من اليسار لفتح الصفحة.")
    else:
        colA, colB, colC = st.columns(3)
        with colA:
            st.subheader("➕ إضافة موظّف")
            new_emp = st.text_input("اسم الموظّف الجديد")
            if st.button("إنشاء ورقة"):
                try:
                    sh = client.open_by_key(SPREADSHEET_ID); titles = [w.title for w in sh.worksheets()]
                    if not new_emp or new_emp in titles: st.warning("⚠️ الاسم فارغ أو موجود.")
                    else:
                        sh.add_worksheet(title=new_emp, rows="1000", cols="20"); sh.worksheet(new_emp).update("1:1", [EXPECTED_HEADERS])
                        st.success("✔️ تم الإنشاء"); st.cache_data.clear()
                except Exception as e: st.error(f"❌ خطأ: {e}")
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
                except Exception as e: st.error(f"❌ خطأ: {e}")
        with colC:
            st.subheader("🗑️ حذف موظّف")
            emp_to_delete = st.selectbox("اختر الموظّف", all_employes, key="admin_del_emp")
            if st.button("❗ حذف الورقة كاملة"):
                try:
                    sh = client.open_by_key(SPREADSHEET_ID); sh.del_worksheet(sh.worksheet(emp_to_delete))
                    st.success("تم الحذف"); st.cache_data.clear()
                except Exception as e: st.error(f"❌ خطأ: {e}")
        st.caption("صفحة الأدمِن مفتوحة لمدّة 30 دقيقة من وقت الفتح.")

        # ========== تجميع شهري للمبالغ ==========
        ui_section("📅 ملخّص شهري: المداخيل / المصاريف / البواقي / مبلغ المسجّلين", "📅")
        monthly_df = build_monthly_totals(df_all)
        if monthly_df.empty:
            st.info("لا توجد بيانات كافية لعرض الملخّص الشهري.")
        else:
            n_months = st.slider("عدد الأشهر للعرض", min_value=3, max_value=24, value=12, step=1)
            st.dataframe(monthly_df.head(n_months), use_container_width=True)
        ui_section_end()
    ui_section_end()
