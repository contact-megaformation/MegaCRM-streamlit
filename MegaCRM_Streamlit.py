# MegaCRM_Streamlit_App.py — CRM + Payments + Finance(MB/Bizerte)
# دفوعات مطوّرة: نحسب نصيب "الإدارة" وحدو و "الهيكل" وحدو، والباقي Reste أوتوماتيك.
# ما عادش نخزّن Montant_Total، نحسبوه عند العرض (Admin+Structure).

import json, time
import streamlit as st
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime, date, timedelta
from PIL import Image

# ========== Page config ==========
st.set_page_config(page_title="MegaCRM", layout="wide", initial_sidebar_state="expanded")

# ===== عنوان =====
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

EXPECTED_HEADERS = [
    "Nom & Prénom","Téléphone","Type de contact","Formation",
    "Remarque","Date ajout","Date de suivi","Alerte",
    "Inscription","Employe","Tag"
]

# ===== Finance (MB/Bizerte) — Helpers =====
FIN_COLUMNS = ["Date", "Libellé", "Montant", "Mode", "Employé", "Catégorie", "Note"]
FIN_MONTHS_FR = ["Janvier","Février","Mars","Avril","Mai","Juin","Juillet","Aout","Septembre","Octobre","Novembre","Décembre"]

def _branch_passwords():
    try:
        b = st.secrets["branch_passwords"]
        return {"Menzel Bourguiba": str(b.get("MB", "MB_2025!")), "Bizerte": str(b.get("BZ", "BZ_2025!"))}
    except Exception:
        return {"Menzel Bourguiba": "MB_2025!", "Bizerte": "BZ_2025!"}

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

def fin_month_title(mois: str, kind: str, branch: str):
    prefix = "Revenue " if kind == "Revenus" else "Dépense "
    short = "MB" if "Menzel" in branch else "BZ"
    return f"{prefix}{mois} ({short})"

def fin_read_df(client, sheet_id: str, title: str) -> pd.DataFrame:
    ws = fin_ensure_ws(client, sheet_id, title, FIN_COLUMNS)
    values = ws.get_all_values()
    if not values:
        return pd.DataFrame(columns=FIN_COLUMNS)
    df = pd.DataFrame(values[1:], columns=values[0])
    if "Montant" in df.columns:
        df["Montant"] = (
            df["Montant"].astype(str).str.replace(",", ".", regex=False).str.replace(" ", "", regex=False)
            .apply(lambda x: pd.to_numeric(x, errors="coerce"))
        )
    if "Date" in df.columns:
        def _p(x):
            for fmt in ("%d/%m/%Y","%Y-%m-%d","%d-%m-%Y","%m/%d/%Y"):
                try: return datetime.strptime(str(x), fmt).date()
                except: pass
            return pd.NaT
        df["Date"] = df["Date"].apply(_p)
    return df

def fin_append_row(client, sheet_id: str, title: str, row: dict):
    ws = fin_ensure_ws(client, sheet_id, title, FIN_COLUMNS)
    header = ws.row_values(1)
    vals = [str(row.get(col, "")) for col in header]
    ws.append_row(vals)

# ===== Helpers (CRM) =====
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

def find_row_by_phone(ws, phone_digits: str) -> int | None:
    values = ws.get_all_values()
    if not values: return None
    header = values[0]
    if "Téléphone" not in header: return None
    tel_idx = header.index("Téléphone")
    for i, r in enumerate(values[1:], start=2):
        if len(r) > tel_idx and normalize_tn_phone(r[tel_idx]) == phone_digits:
            return i
    return None

# ===== Styling =====
def color_tag(val):
    if isinstance(val, str) and val.strip().startswith("#") and len(val.strip()) == 7:
        return f"background-color: {val}; color: white;"
    return ""

def mark_alert_cell(val: str):
    s = str(val).strip()
    if not s: return ''
    if "متأخرة" in s: return 'background-color: #ffe6b3; color: #7a4e00'
    return 'background-color: #ffcccc; color: #7a0000'

def highlight_inscrit_row(row: pd.Series):
    insc = str(row.get("Inscription", "")).strip().lower()
    return ['background-color: #d6f5e8' if insc in ("inscrit","oui") else '' for _ in row.index]

# ===== تحميل بيانات CRM + إخفاء أوراق الدفوعات =====
@st.cache_data(ttl=600)
def load_all_data():
    sh = client.open_by_key(SPREADSHEET_ID)
    worksheets = sh.worksheets()
    all_dfs, all_employes = [], []

    for ws in worksheets:
        title = ws.title.strip()
        if title.endswith("_PAIEMENTS") or title.startswith("_"):
            continue
        all_employes.append(title)

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
            r = list(r or [])
            if len(r) < len(EXPECTED_HEADERS):
                r += [""] * (len(EXPECTED_HEADERS) - len(r))
            else:
                r = r[:len(EXPECTED_HEADERS)]
            fixed_rows.append(r)

        df = pd.DataFrame(fixed_rows, columns=EXPECTED_HEADERS)
        df["__sheet_name"] = title
        all_dfs.append(df)

    big = pd.concat(all_dfs, ignore_index=True) if all_dfs else pd.DataFrame(columns=EXPECTED_HEADERS + ["__sheet_name"])
    return big, all_employes

df_all, all_employes = load_all_data()
df_emp = pd.DataFrame()
filtered_df = pd.DataFrame()

# ===== Sidebar =====
try:
    st.sidebar.image(Image.open("logo.png"), use_container_width=True)
except Exception:
    pass

role = st.sidebar.radio("الدور", ["موظف", "أدمن"], horizontal=True)
tab_choice = st.sidebar.selectbox("📑 التبويب", ["CRM", "Finance (MB/Bizerte)"], index=0)
employee = None
if role == "موظف":
    employee = st.sidebar.selectbox("👨‍💼 اختر الموظّف (ورقة Google Sheets)", all_employes) if all_employes else None

# ===== Finance (MB/Bizerte) Tab =====
if tab_choice == "Finance (MB/Bizerte)":
    st.title("💸 المالية — مداخيل/مصاريف (منزل بورقيبة & بنزرت)")

    with st.sidebar:
        st.markdown("---")
        st.subheader("🔧 إعدادات المالية")
        branch = st.selectbox("الفرع", ["Menzel Bourguiba", "Bizerte"], key="fin_branch")
        kind   = st.radio("النوع", ["Revenus","Dépenses"], horizontal=True, key="fin_kind")
        mois   = st.selectbox("الشهر", FIN_MONTHS_FR, index=datetime.now().month-1, key="fin_month")
        BRANCH_PASSWORDS = _branch_passwords()
        key_pw = f"finance_pw_ok::{branch}"
        if key_pw not in st.session_state: st.session_state[key_pw] = False
        if not st.session_state[key_pw]:
            pw_try = st.text_input("كلمة سرّ الفرع", type="password", key=f"fin_pw_{branch}")
            if st.button("دخول الفرع", key=f"fin_enter_{branch}"):
                if pw_try and pw_try == BRANCH_PASSWORDS.get(branch, ""):
                    st.session_state[key_pw] = True
                    st.success("تم الدخول ✅")
                else:
                    st.error("كلمة سرّ غير صحيحة ❌")

    if not st.session_state.get(f"finance_pw_ok::{branch}", False):
        st.info("⬅️ أدخل كلمة السرّ من اليسار للمتابعة.")
        st.stop()

    fin_title = fin_month_title(mois, kind, branch)
    current_role = role
    current_employee = st.session_state.get("employee", "")

    df_fin = fin_read_df(client, SPREADSHEET_ID, fin_title)
    df_view = df_fin.copy()
    if current_role == "موظف" and current_employee.strip() and "Employé" in df_view.columns:
        df_view = df_view[df_view["Employé"].fillna("").str.strip().str.lower() == current_employee.strip().lower()]

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
            for col in [c for c in ["Libellé","Catégorie","Mode","Employé","Note"] if c in df_view.columns]:
                m |= df_view[col].fillna("").str.contains(search, case=False, na=False)
            df_view = df_view[m]

    st.subheader(f"📄 {fin_title}")
    st.dataframe(df_view if not df_view.empty else pd.DataFrame(columns=FIN_COLUMNS), use_container_width=True)

    total = df_view["Montant"].sum() if ("Montant" in df_view.columns and not df_view.empty) else 0.0
    st.metric("إجمالي المبلغ", f"{total:,.2f}")

    with st.expander("📊 ملخّص الفرع للشهر"):
        rev_df = fin_read_df(client, SPREADSHEET_ID, fin_month_title(mois, "Revenus", branch))
        dep_df = fin_read_df(client, SPREADSHEET_ID, fin_month_title(mois, "Dépenses", branch))
        rev = rev_df["Montant"].sum() if "Montant" in rev_df.columns else 0
        dep = dep_df["Montant"].sum() if "Montant" in dep_df.columns else 0
        a,b,c = st.columns(3)
        a.metric("مداخيل", f"{rev:,.2f}")
        b.metric("مصاريف", f"{dep:,.2f}")
        c.metric("الصافي", f"{(rev-dep):,.2f}")

    # ======== إضافة عملية — Revenus تربط بعميل مُسجَّل ========
    st.markdown("---")
    st.markdown("### ➕ إضافة عملية جديدة")

    selected_client_info = None
    client_default_lib = ""
    client_default_emp = (st.session_state.get("employee","") or "")

    if kind == "Revenus":
        st.markdown("#### 👤 اربط الدفعة بعميل مُسجَّل")
        reg_df = df_all.copy()
        reg_df["Inscription_norm"] = reg_df["Inscription"].fillna("").astype(str).str.strip().str.lower()
        reg_df = reg_df[reg_df["Inscription_norm"].isin(["oui","inscrit"])]

        if role == "موظف" and employee:
            reg_df = reg_df[reg_df["__sheet_name"] == employee]

        if reg_df.empty:
            st.info("لا يوجد عملاء مُسجَّلين ضمن هذا النطاق.")
        else:
            def _opt(row):
                phone = format_display_phone(row.get("Téléphone",""))
                return f"{row.get('Nom & Prénom','')} — {phone} — {row.get('Formation','')}  [{row.get('__sheet_name','')}]"
            options = [_opt(r) for _, r in reg_df.iterrows()]
            pick = st.selectbox("اختر عميلًا مُسجَّلًا (اختياري)", ["— بدون اختيار —"] + options, key="fin_client_pick")

            if pick and pick != "— بدون اختيار —":
                idx = options.index(pick)
                row = reg_df.iloc[idx]
                selected_client_info = {
                    "name": str(row.get("Nom & Prénom","")).strip(),
                    "tel":  str(row.get("Téléphone","")).strip(),
                    "formation": str(row.get("Formation","")).strip(),
                    "emp": str(row.get("__sheet_name","")).strip()
                }
                client_default_lib = f"Paiement {selected_client_info['formation']} - {selected_client_info['name']}".strip()
                if not client_default_emp:
                    client_default_emp = selected_client_info["emp"]
                st.caption(f"سيتم اقتراح: **Libellé =** {client_default_lib}  —  **Employé =** {client_default_emp}")

    with st.form("fin_add_row"):
        d1, d2, d3 = st.columns(3)
        date_val = d1.date_input("Date", value=datetime.today(), key="fin_date")

        libelle_init = client_default_lib if client_default_lib else ""
        libelle  = d2.text_input("Libellé", libelle_init, key="fin_lib")

        montant  = d3.number_input("Montant", min_value=0.0, step=1.0, format="%.2f", key="fin_montant")

        e1, e2, e3 = st.columns(3)
        mode      = e1.selectbox("Mode", ["Espèces","Virement","Carte","Autre"], key="fin_mode")
        employe_init = client_default_emp if client_default_emp else (st.session_state.get("employee","") or "")
        employe   = e2.text_input("Employé", value=employe_init, key="fin_emp")
        categorie = e3.text_input("Catégorie", value=("Vente" if kind=="Revenus" else "Achat"), key="fin_cat")

        note_default = ""
        if selected_client_info:
            note_default = f"Client: {selected_client_info['name']} / {format_display_phone(selected_client_info['tel'])} / {selected_client_info['formation']}"
        note = st.text_area("Note", note_default, key="fin_note")

        if st.form_submit_button("✅ حفظ العملية"):
            if not libelle.strip():
                st.error("Libellé مطلوب.")
            elif montant <= 0:
                st.error("المبلغ لازم > 0.")
            elif role == "موظف" and not employe.strip():
                st.error("اسم الموظّف مطلوب.")
            else:
                fin_append_row(
                    client, SPREADSHEET_ID, fin_title,
                    {
                        "Date": date_val.strftime("%d/%m/%Y"),
                        "Libellé": libelle.strip(),
                        "Montant": f"{montant:.2f}",
                        "Mode": mode,
                        "Employé": employe.strip(),
                        "Catégorie": categorie.strip(),
                        "Note": note.strip(),
                    }
                )
                st.success("تمّ الحفظ ✅"); st.cache_data.clear(); st.rerun()
    st.stop()

# ===== أعمدة مشتقّة (CRM) =====
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
    df_all["Alerte_view"] = ""
    df_all["Mois"] = ""
    df_all["Téléphone_norm"] = ""
    ALL_PHONES = set()

# ===== Admin lock =====
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

# ===== Dashboard =====
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

# ===== إحصائيات مفصّلة: حسب الموظّف =====
df_stats = df_all.copy()
df_stats["Inscription_norm"] = df_stats["Inscription"].fillna("").astype(str).str.strip().str.lower()
df_stats["Alerte_norm"]      = df_stats["Alerte_view"].fillna("").astype(str).str.strip()
df_stats["DateAjout_dt"]     = pd.to_datetime(df_stats.get("Date ajout"), dayfirst=True, errors="coerce")
df_stats["DateSuivi_dt"]     = pd.to_datetime(df_stats.get("Date de suivi"), dayfirst=True, errors="coerce")
today = datetime.now().date()

added_today_mask      = df_stats["DateAjout_dt"].dt.date.eq(today)
registered_today_mask = df_stats["Inscription_norm"].isin(["oui","inscrit"]) & added_today_mask
alert_now_mask        = df_stats["Alerte_norm"].ne("")

df_stats["__added_today"] = added_today_mask
df_stats["__reg_today"]   = registered_today_mask
df_stats["__has_alert"]   = alert_now_mask

grp_base = (
    df_stats.groupby("__sheet_name", dropna=False)
    .agg(
        Clients   = ("Nom & Prénom", "count"),
        Inscrits  = ("Inscription_norm", lambda x: (x == "oui").sum()),
        تنبيهات     = ("__has_alert", "sum"),
        مضافون_اليوم = ("__added_today", "sum"),
        مسجلون_اليوم = ("__reg_today", "sum"),
    )
    .reset_index()
    .rename(columns={"__sheet_name": "الموظف"})
)

grp_base["% تسجيل"] = (
    (grp_base["Inscrits"] / grp_base["Clients"]).replace([float("inf"), float("nan")], 0) * 100
).round(2)

grp_base = grp_base.sort_values(by=["تنبيهات", "Clients"], ascending=[False, False])

st.markdown("#### حسب الموظّف")
st.dataframe(grp_base, use_container_width=True)

# ===== Global search by phone =====
st.subheader("🔎 بحث عام برقم الهاتف")
global_phone = st.text_input("اكتب رقم الهاتف (8 أرقام محلية أو 216XXXXXXXX)", key="global_phone_all")
if global_phone.strip():
    q_norm = normalize_tn_phone(global_phone)
    search_df = df_all.copy()
    if "Téléphone_norm" not in search_df.columns:
        search_df["Téléphone_norm"] = search_df["Téléphone"].apply(normalize_tn_phone)
    if "Alerte_view" in search_df.columns:
        search_df["Alerte"] = search_df["Alerte_view"]
    search_df = search_df[search_df["Téléphone_norm"] == q_norm]
    if search_df.empty:
        st.info("❕ ما لقيتش عميل بهذا الرقم في كامل النظام.")
    else:
        display_cols = [c for c in EXPECTED_HEADERS if c in search_df.columns]
        if "Employe" in search_df.columns and "Employe" not in display_cols:
            display_cols.append("Employe")
        styled_global = (
            search_df[display_cols]
            .style.apply(highlight_inscrit_row, axis=1)
            .applymap(mark_alert_cell, subset=["Alerte"])
        )
        st.dataframe(styled_global, use_container_width=True)
        st.markdown("---")

# ================== واجهة الموظّف ==================
if role == "موظف" and employee:
    # reset lock عند تبديل الموظّف
    if "last_emp" not in st.session_state:
        st.session_state["last_emp"] = employee
    if st.session_state["last_emp"] != employee:
        for emp_name in (st.session_state["last_emp"], employee):
            st.session_state[f"payments_ok::{emp_name}"] = False
            st.session_state[f"payments_ok_at::{emp_name}"] = None
        st.session_state["last_emp"] = employee

    st.subheader(f"📁 لوحة {employee}")
    df_emp = df_all[df_all["__sheet_name"] == employee].copy()

    # ===== فلترة بالشهر =====
    if not df_emp.empty:
        df_emp["DateAjout_dt"] = pd.to_datetime(df_emp["Date ajout"], dayfirst=True, errors="coerce")
        df_emp = df_emp.dropna(subset=["DateAjout_dt"])
        df_emp["Mois"] = df_emp["DateAjout_dt"].dt.strftime("%m-%Y")
        month_filter = st.selectbox("🗓️ اختر شهر الإضافة", sorted(df_emp["Mois"].dropna().unique(), reverse=True))
        filtered_df = df_emp[df_emp["Mois"] == month_filter].copy()
    else:
        st.warning("⚠️ لا يوجد أي عملاء بعد. قاعدة البيانات فارغة.")
        filtered_df = pd.DataFrame()

    # ===== عدّاد + Formation =====
    if not filtered_df.empty:
        pending_mask = filtered_df["Remarque"].fillna("").astype(str).str.strip() == ""
        st.markdown("### 📊 متابعتك")
        st.metric("⏳ مضافين بلا ملاحظات", int(pending_mask.sum()))
        formations = sorted([f for f in filtered_df["Formation"].dropna().astype(str).unique() if f.strip()])
        formation_choice = st.selectbox("📚 فلترة بالتكوين", ["الكل"] + formations)
        if formation_choice != "الكل":
            filtered_df = filtered_df[filtered_df["Formation"].astype(str) == formation_choice]

    def render_table(df_disp: pd.DataFrame):
        if df_disp.empty:
            st.info("لا توجد بيانات في هذا الفلتر."); return
        _df = df_disp.copy()
        if "Alerte_view" in _df.columns: _df["Alerte"] = _df["Alerte_view"]
        display_cols = [c for c in EXPECTED_HEADERS if c in _df.columns]
        styled = (
            _df[display_cols]
            .style.apply(highlight_inscrit_row, axis=1)
            .applymap(mark_alert_cell, subset=["Alerte"])
            .applymap(color_tag, subset=["Tag"])
        )
        st.dataframe(styled, use_container_width=True)

    st.markdown("### 📋 قائمة العملاء")
    render_table(filtered_df)

    if not filtered_df.empty and st.checkbox("🔴 عرض العملاء الذين لديهم تنبيهات"):
        _df = filtered_df.copy()
        if "Alerte_view" in _df.columns: _df["Alerte"] = _df["Alerte_view"]
        alerts_df = _df[_df["Alerte"].fillna("").astype(str).str.strip() != ""]
        st.markdown("### 🚨 عملاء مع تنبيهات")
        render_table(alerts_df)

    # ===== ✏️ تعديل بيانات عميل =====
    if not df_emp.empty:
        st.markdown("### ✏️ تعديل بيانات عميل")
        df_emp_edit = df_emp.copy()
        df_emp_edit["Téléphone_norm"] = df_emp_edit["Téléphone"].apply(normalize_tn_phone)
        phone_choices = {
            f"[{i}] {row['Nom & Prénom']} — {format_display_phone(row['Téléphone_norm'])}": row["Téléphone_norm"]
            for i, row in df_emp_edit.iterrows() if str(row["Téléphone"]).strip() != ""
        }

        if phone_choices:
            chosen_key   = st.selectbox("اختر العميل (بالاسم/الهاتف)", list(phone_choices.keys()), key="edit_pick")
            chosen_phone = phone_choices.get(chosen_key, "")
            cur_row = df_emp_edit[df_emp_edit["Téléphone_norm"] == chosen_phone].iloc[0] if chosen_phone else None
            cur_name = str(cur_row["Nom & Prénom"]) if cur_row is not None else ""
            cur_tel_raw = str(cur_row["Téléphone"]) if cur_row is not None else ""
            cur_formation = str(cur_row["Formation"]) if cur_row is not None else ""
            cur_remark = str(cur_row.get("Remarque", "")) if cur_row is not None else ""
            cur_ajout = pd.to_datetime(cur_row["Date ajout"], dayfirst=True, errors="coerce").date() if cur_row is not None else date.today()
            cur_suivi = pd.to_datetime(cur_row["Date de suivi"], dayfirst=True, errors="coerce").date() if cur_row is not None and str(cur_row["Date de suivi"]).strip() else date.today()
            cur_insc  = str(cur_row["Inscription"]).strip().lower() if cur_row is not None else ""

            col1, col2 = st.columns(2)
            with col1:
                new_name = st.text_input("👤 الاسم و اللقب", value=cur_name, key="edit_name_txt")
                new_phone_raw = st.text_input("📞 رقم الهاتف (8 أرقام أو 216XXXXXXXX)", value=cur_tel_raw, key="edit_phone_txt")
                new_formation = st.text_input("📚 التكوين (Formation)", value=cur_formation, key="edit_formation_txt")
            with col2:
                new_ajout = st.date_input("🕓 تاريخ الإضافة", value=cur_ajout, key="edit_ajout_dt")
                new_suivi = st.date_input("📆 تاريخ المتابعة", value=cur_suivi, key="edit_suivi_dt")
                new_insc = st.selectbox("🟢 التسجيل", ["Pas encore", "Inscrit"], index=(1 if cur_insc == "oui" else 0), key="edit_insc_sel")

            new_remark_full = st.text_area("🗒️ ملاحظة (استبدال كامل)", value=cur_remark, key="edit_remark_txt")
            extra_note = st.text_area("➕ أضف ملاحظة جديدة (طابع زمني)", placeholder="اكتب ملاحظة لإلحاقها…", key="append_note_txt")

            if st.button("💾 حفظ التعديلات", key="save_all_edits"):
                try:
                    ws = client.open_by_key(SPREADSHEET_ID).worksheet(employee)
                    row_idx = find_row_by_phone(ws, chosen_phone)
                    if not row_idx:
                        st.error("❌ تعذّر إيجاد الصف لهذا الهاتف.")
                    else:
                        col_map = {h: EXPECTED_HEADERS.index(h) + 1 for h in [
                            "Nom & Prénom", "Téléphone", "Formation", "Date ajout", "Date de suivi", "Inscription", "Remarque"
                        ]}
                        new_phone_norm = normalize_tn_phone(new_phone_raw)
                        if not new_name.strip(): st.error("❌ الاسم و اللقب إجباري."); st.stop()
                        if not new_phone_norm.strip(): st.error("❌ رقم الهاتف إجباري."); st.stop()
                        phones_except_current = set(ALL_PHONES) - {chosen_phone}
                        if new_phone_norm in phones_except_current: st.error("⚠️ الرقم موجود مسبقًا."); st.stop()

                        ws.update_cell(row_idx, col_map["Nom & Prénom"], new_name.strip())
                        ws.update_cell(row_idx, col_map["Téléphone"], new_phone_norm)
                        ws.update_cell(row_idx, col_map["Formation"], new_formation.strip())
                        ws.update_cell(row_idx, col_map["Date ajout"], fmt_date(new_ajout))
                        ws.update_cell(row_idx, col_map["Date de suivi"], fmt_date(new_suivi))
                        ws.update_cell(row_idx, col_map["Inscription"], "Oui" if new_insc == "Inscrit" else "Pas encore")

                        if new_remark_full.strip() != cur_remark.strip():
                            ws.update_cell(row_idx, col_map["Remarque"], new_remark_full.strip())
                        if extra_note.strip():
                            old_rem = ws.cell(row_idx, col_map["Remarque"]).value or ""
                            stamp = datetime.now().strftime("%d/%m/%Y %H:%M")
                            appended = (old_rem + "\n" if old_rem else "") + f"[{stamp}] {extra_note.strip()}"
                            ws.update_cell(row_idx, col_map["Remarque"], appended)

                        st.success("✅ تم حفظ التعديلات"); st.cache_data.clear()
                except Exception as e:
                    st.error(f"❌ خطأ أثناء التعديل: {e}")

    # ===== 💳 الدفوعات (الموظف) — نسخة تفصل الإدارة عن الهيكل =====
    def _to_float(x):
        s = str(x or "").strip()
        if not s: return 0.0
        for ch in ["DT","TND","د","د.","دينار","€","$"]:
            s = s.replace(ch, "")
        s = s.replace(" ", "").replace(",", ".")
        try:
            return float(s)
        except Exception:
            return 0.0

    # هيدر الدفوعات — بدون Montant_Total
    PAY_HEADERS_STD = [
        "Tel","Formation","Prix",
        "Montant_Admin","Montant_Structure",
        "Date","Echeance","Reste",
        "Branch","Note"
    ]

    def ensure_payments_ws(sh, employee_name: str):
        ws_name = f"{employee_name}_PAIEMENTS"
        try:
            ws = sh.worksheet(ws_name)
        except Exception:
            ws = sh.add_worksheet(title=ws_name, rows="3000", cols="20")
            ws.update("1:1", [PAY_HEADERS_STD]); return ws
        rows = ws.get_all_values()
        if not rows:
            ws.update("1:1", [PAY_HEADERS_STD])
        else:
            header = [h.strip() for h in rows[0]]
            if header != PAY_HEADERS_STD:
                ws.update("1:1", [PAY_HEADERS_STD])
        return ws

    def _parse_date_any(x):
        if x is None or str(x).strip() == "": return pd.NaT
        for fmt in ("%d/%m/%Y","%Y-%m-%d","%d-%m-%Y","%m/%d/%Y"):
            try:
                return pd.to_datetime(str(x), format=fmt, dayfirst=True, errors="raise")
            except Exception:
                pass
        try:
            return pd.to_datetime(str(x), dayfirst=True, errors="coerce")
        except Exception:
            return pd.NaT

    def _read_payments_for(sh, phone_norm: str, employee_name: str) -> pd.DataFrame:
        ws = ensure_payments_ws(sh, employee_name)
        rows = ws.get_all_values()
        if not rows or len(rows) == 1:
            return pd.DataFrame(columns=PAY_HEADERS_STD)

        df = pd.DataFrame(rows[1:], columns=rows[0])
        df["Tel"] = df["Tel"].apply(normalize_tn_phone)
        df = df[df["Tel"] == str(phone_norm)]

        for c in ["Prix","Montant_Admin","Montant_Structure","Reste"]:
            if c in df.columns:
                df[c] = df[c].apply(_to_float)

        df["Date_dt"] = df["Date"].apply(_parse_date_any) if "Date" in df.columns else pd.NaT
        df["Echeance_dt"] = df["Echeance"].apply(_parse_date_any) if "Echeance" in df.columns else pd.NaT

        # احسب Reste على التاريخ
        prix = float(df["Prix"].max()) if "Prix" in df.columns and not df["Prix"].isna().all() else 0.0
        paid_admin = float(df["Montant_Admin"].sum()) if "Montant_Admin" in df.columns else 0.0
        paid_struct = float(df["Montant_Structure"].sum()) if "Montant_Structure" in df.columns else 0.0
        reste_now = max(prix - (paid_admin + paid_struct), 0.0)
        if "Reste" in df.columns and len(df) > 0:
            df.loc[df.index[-1], "Reste"] = reste_now

        df = df.sort_values(by=["Date_dt"], ascending=True)

        today_ts = pd.Timestamp(datetime.now().date())
        df["__ALERT"] = df["Echeance_dt"].notna() & (df["Echeance_dt"] < today_ts) & (df["Reste"].fillna(0) > 0)

        return df

    def _append_payment(
        sh, employee_name: str, phone_norm: str, formation: str,
        prix_total: float, montant_admin: float, montant_structure: float,
        dt: date, echeance: date | None, branch: str, note: str = ""
    ):
        ws = ensure_payments_ws(sh, employee_name)
        existing = _read_payments_for(sh, phone_norm, employee_name)
        sum_admin = float(existing["Montant_Admin"].sum()) if not existing.empty else 0.0
        sum_struct = float(existing["Montant_Structure"].sum()) if not existing.empty else 0.0
        total_now = float(montant_admin) + float(montant_structure)
        reste = max(float(prix_total) - (sum_admin + sum_struct + total_now), 0.0)

        row = {
            "Tel": phone_norm,
            "Formation": str(formation or ""),
            "Prix": f"{float(prix_total):.2f}",
            "Montant_Admin": f"{float(montant_admin):.2f}",
            "Montant_Structure": f"{float(montant_structure):.2f}",
            "Date": datetime.strptime(fmt_date(dt), "%d/%m/%Y").strftime("%d/%m/%Y"),
            "Echeance": fmt_date(echeance) if isinstance(echeance, date) else "",
            "Reste": f"{reste:.2f}",
            "Branch": str(branch or ""),
            "Note": note.strip(),
        }
        vals = [row.get(col, "") for col in PAY_HEADERS_STD]
        ws.append_row(vals)
        return reste

    # ==== واجهة الدفوعات للموظّف ====
    st.markdown("## 💳 الدفوعات")
    def _get_pay_password_for(user_login: str | None) -> str:
        try:
            secrets = st.secrets["payments_protect"]
        except Exception:
            return "1234"
        if user_login and "by_user" in secrets and user_login in secrets["by_user"]:
            return str(secrets["by_user"][user_login])
        return str(secrets.get("password", "1234"))

    def _session_key_open_for(user_login: str) -> str:
        return f"payments_ok::{user_login}"

    def _session_key_time_for(user_login: str) -> str:
        return f"payments_ok_at::{user_login}"

    def payments_unlocked(user_login: str) -> bool:
        ok = st.session_state.get(_session_key_open_for(user_login), False)
        ts = st.session_state.get(_session_key_time_for(user_login))
        return bool(ok and ts and (datetime.now() - ts) <= timedelta(minutes=15))

    def payments_lock_ui(user_login: str):
        with st.expander("🔒 حماية المدفوعات (Password)", expanded=not payments_unlocked(user_login)):
            if payments_unlocked(user_login):
                col1, col2 = st.columns([1,1])
                with col1: st.success("تم فتح قسم المدفوعات (ينتهي بعد 15 دقيقة).")
                with col2:
                    if st.button("🔐 قفل الآن"):
                        st.session_state[_session_key_open_for(user_login)] = False
                        st.session_state[_session_key_time_for(user_login)] = None
                        st.info("تم القفل.")
            else:
                pwd_cfg = _get_pay_password_for(user_login)
                pwd_try = st.text_input("أدخل كلمة السرّ لفتح قسم المدفوعات", type="password", key=f"pwd_{user_login}")
                if st.button("🔓 فتح", key=f"open_{user_login}"):
                    if pwd_try and pwd_try == pwd_cfg:
                        st.session_state[_session_key_open_for(user_login)] = True
                        st.session_state[_session_key_time_for(user_login)] = datetime.now()
                        st.success("تم الفتح لمدة 15 دقيقة.")
                    else:
                        st.error("كلمة سرّ غير صحيحة.")

    payments_lock_ui(employee)
    if payments_unlocked(employee):
        df_emp_for_pay = df_all[df_all["__sheet_name"] == employee].copy()
        df_emp_for_pay["Téléphone_norm"] = df_emp_for_pay["Téléphone"].apply(normalize_tn_phone)

        pay_choices = {
            f"{row['Nom & Prénom']} — {format_display_phone(row['Téléphone_norm'])} — {row.get('Formation','')}": row["Téléphone_norm"]
            for _, row in df_emp_for_pay.iterrows() if str(row["Téléphone"]).strip() != ""
        }

        if not pay_choices:
            st.info("لا يوجد عملاء لاختيارهم للدفوعات.")
        else:
            pay_key = st.selectbox("اختر العميل (للـدفوعات)", list(pay_choices.keys()), key="pay_pick_v2")
            pay_phone = pay_choices.get(pay_key, "")
            cur_row = df_emp_for_pay[df_emp_for_pay["Téléphone_norm"] == pay_phone].iloc[0]
            cur_formation = str(cur_row.get("Formation", ""))

            sh = client.open_by_key(SPREADSHEET_ID)
            df_payments = _read_payments_for(sh, pay_phone, employee)

            if df_payments.empty:
                st.info("لا توجد دفوعات سابقة لهذا العميل.")
                paid_admin = 0.0
                paid_struct = 0.0
                prix_max = 0.0
            else:
                st.dataframe(
                    df_payments[[
                        "Date","Echeance","Formation","Prix",
                        "Montant_Admin","Montant_Structure",
                        "Reste","Branch","Note","__ALERT"
                    ]],
                    use_container_width=True
                )
                paid_admin = float(df_payments["Montant_Admin"].sum())
                paid_struct = float(df_payments["Montant_Structure"].sum())
                prix_max = float(df_payments["Prix"].max() if "Prix" in df_payments.columns else 0.0)
                late_count = int(df_payments["__ALERT"].sum())
                if late_count > 0:
                    st.warning(f"⚠️ يوجد {late_count} سطر(أسطر) بها إستحقاق فائت و رصيد متبقٍ.")

            # فورم الإضافة
            with st.form("pay_add_form_v2", border=True):
                c1, c2, c3, c4 = st.columns(4)
                with c1:
                    prix_total = st.number_input("💰 سعر التكوين (Prix)", min_value=0.0, step=10.0,
                                                 value=prix_max, key="v2_prix")
                with c2:
                    montant_admin = st.number_input("🏢 مدفوع الإدارة", min_value=0.0, step=10.0, key="v2_m_admin")
                with c3:
                    montant_struct = st.number_input("🏫 مدفوع الهيكل", min_value=0.0, step=10.0, key="v2_m_struct")
                with c4:
                    date_pay = st.date_input("📅 تاريخ الدفع", value=date.today(), key="v2_date")

                d1, d2, d3 = st.columns(3)
                with d1:
                    echeance = st.date_input("⏰ تاريخ استحقاق الباقي", value=date.today(), key="v2_due")
                with d2:
                    branch = st.selectbox("الفرع", ["Menzel Bourguiba","Bizerte"], key="v2_branch")
                with d3:
                    note = st.text_input("ملاحظة", key="v2_note")

                total_now = float(montant_admin) + float(montant_struct)
                reste_preview = max(float(prix_total) - (paid_admin + paid_struct + total_now), 0.0)

                st.caption(
                    f"مجموع الإدارة (قديمة+جديدة): **{paid_admin + float(montant_admin):.2f}** — "
                    f"مجموع الهيكل (قديمة+جديدة): **{paid_struct + float(montant_struct):.2f}** — "
                    f"الباقي المتوقع بعد الحفظ: **{reste_preview:.2f}**"
                )

                if st.form_submit_button("➕ أضف الدفعة"):
                    if prix_total <= 0 or (montant_admin <= 0 and montant_struct <= 0):
                        st.warning("رجاءً أدخل سعر تكوين موجب و مبلغ مدفوع (الإدارة أو الهيكل).")
                    else:
                        try:
                            reste = _append_payment(
                                sh, employee, pay_phone, cur_formation,
                                prix_total, montant_admin, montant_struct,
                                date_pay, echeance, branch, note
                            )
                            st.success(f"✅ تمت الإضافة. المتبقي الآن: {reste:.2f}")
                            st.cache_data.clear(); st.rerun()
                        except Exception as e:
                            st.error(f"❌ خطأ أثناء إضافة الدفعة: {e}")
    else:
        st.info("🔒 قسم المدفوعات مقفول لهذا الموظّف. أدخل كلمة السرّ لفتحه من الأعلى.")

# ================== صفحة الأدمِن — تقارير الدفوعات ==================
@st.cache_data(ttl=60)
def read_all_payments_concat(_spreadsheet_id: str, employees: tuple[str, ...]) -> pd.DataFrame:
    """اقرأ كل الدفوعات عبر *_PAIEMENTS بالهيدر الجديد + عمود Employe."""
    c = client.open_by_key(_spreadsheet_id)
    all_rows = []
    for emp in employees:
        try:
            ws = c.worksheet(f"{emp}_PAIEMENTS")
        except Exception:
            continue
        try:
            rows = ws.get_all_values()
        except Exception:
            time.sleep(0.3); 
            try: rows = ws.get_all_values()
            except Exception: continue
        if not rows or len(rows) == 1:
            continue

        df = pd.DataFrame(rows[1:], columns=rows[0])
        df["Employe"] = emp

        for col in ["Prix","Montant_Admin","Montant_Structure","Reste"]:
            if col in df.columns: df[col] = df[col].apply(_to_float)

        if "Date" in df.columns:     df["Date_dt"] = pd.to_datetime(df["Date"], dayfirst=True, errors="coerce")
        else:                        df["Date_dt"] = pd.NaT
        if "Echeance" in df.columns: df["Echeance_dt"] = pd.to_datetime(df["Echeance"], dayfirst=True, errors="coerce")
        else:                        df["Echeance_dt"] = pd.NaT

        if "Branch" not in df.columns: df["Branch"] = ""
        all_rows.append(df)
        time.sleep(0.1)

    if not all_rows:
        return pd.DataFrame(columns=PAY_HEADERS_STD + ["Employe","Date_dt","Echeance_dt","__ALERT"])

    big = pd.concat(all_rows, ignore_index=True)

    # احسب Reste من جديد لضمان الدقة: Prix - (sum admin + sum struct) per Tel+Employe
    # (تبسيط: نستعمل المخزّن ونكمّلو بالتجميع للعرض)
    today = pd.Timestamp(datetime.now().date())
    big["__ALERT"] = big["Echeance_dt"].notna() & (big["Echeance_dt"] < today) & (big["Reste"].fillna(0) > 0)

    return big

if role == "أدمن":
    st.markdown("## 👑 لوحة الأدمِن")
    if not admin_unlocked():
        st.info("🔐 أدخل كلمة سرّ الأدمِن من اليسار لفتح الصفحة.")
    else:
        # ===== إدارة الموظفين (اختياري: حافظ على بلوكاتك المعتادة إن لزم) =====
        st.markdown("### 👨‍💼 إدارة الموظفين")
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

        # ===== تقارير الدفوعات =====
        st.markdown("---")
        st.markdown("### 💳 جميع الدفوعات (كل الموظفين) + تقارير الفروع")

        emps_tuple = tuple(sorted(set(df_all["__sheet_name"].dropna().astype(str))))
        try:
            df_allp = read_all_payments_concat(SPREADSHEET_ID, emps_tuple)
        except Exception as e:
            st.error(f"تعذّر قراءة الدفوعات: {e}")
            df_allp = pd.DataFrame(columns=["Employe","Date_dt","Echeance_dt","__ALERT"] + PAY_HEADERS_STD)

        if df_allp.empty:
            st.info("لا توجد أي دفوعات بعد.")
        else:
            colf1, colf2, colf3, colf4 = st.columns([1,1,1,2])
            with colf1:
                branches_pick = st.multiselect("🏢 الفروع", sorted([b for b in df_allp["Branch"].dropna().unique().tolist() if str(b).strip()]))
            with colf2:
                emps_pick = st.multiselect("👨‍💼 الموظفون", sorted(df_allp["Employe"].unique().tolist()))
            with colf3:
                late_only = st.checkbox("⏰ فقط المتأخرة (Reste>0 & Echéance فائتة)")
            with colf4:
                min_d = pd.to_datetime(df_allp["Date_dt"]).min()
                max_d = pd.to_datetime(df_allp["Date_dt"]).max()
                if pd.isna(min_d): min_d = date.today()
                if pd.isna(max_d): max_d = date.today()
                d_from, d_to = st.date_input("📅 المدة", [min_d.date(), max_d.date()])

            filt = df_allp.copy()
            if branches_pick: filt = filt[filt["Branch"].isin(branches_pick)]
            if emps_pick:     filt = filt[filt["Employe"].isin(emps_pick)]
            if isinstance(d_from, date) and isinstance(d_to, date):
                filt = filt[(filt["Date_dt"] >= pd.Timestamp(d_from)) & (filt["Date_dt"] <= pd.Timestamp(d_to))]
            if late_only:
                filt = filt[filt["__ALERT"] == True]

            if filt.empty:
                st.info("لا نتائج بالفلتر الحالي.")
            else:
                base_cols = ["Date","Echeance","Branch","Employe","Tel","Formation","Prix",
                             "Montant_Admin","Montant_Structure","Reste","Note","__ALERT"]
                base_cols = [c for c in base_cols if c in filt.columns]
                st.dataframe(filt[base_cols].sort_values("Date_dt"), use_container_width=True)

                st.markdown("#### 📈 إحصائيات (مفصولة)")
                k1,k2,k3,k4 = st.columns(4)
                total_admin  = float(filt["Montant_Admin"].sum()) if "Montant_Admin" in filt.columns else 0.0
                total_struct = float(filt["Montant_Structure"].sum()) if "Montant_Structure" in filt.columns else 0.0
                reste_sum    = float(filt["Reste"].sum()) if "Reste" in filt.columns else 0.0
                late_cnt     = int(filt["__ALERT"].sum()) if "__ALERT" in filt.columns else 0
                with k1: st.metric("Admin (مجموعة)", f"{total_admin:,.2f}")
                with k2: st.metric("Structure (مجموعة)", f"{total_struct:,.2f}")
                with k3: st.metric("Reste الإجمالي", f"{reste_sum:,.2f}")
                with k4: st.metric("إنذارات متأخرة", f"{late_cnt}")

                st.markdown("#### 🔎 تجميعات حسب الفرع والموظف")
                g1,g2 = st.columns(2)
                if "Branch" in filt.columns:
                    grp_branch = (
                        filt.groupby("Branch", dropna=False)[["Montant_Admin","Montant_Structure","Reste"]]
                        .sum().reset_index()
                    )
                    with g1: st.dataframe(grp_branch, use_container_width=True)
                if "Employe" in filt.columns:
                    grp_emp = (
                        filt.groupby("Employe", dropna=False)[["Montant_Admin","Montant_Structure","Reste"]]
                        .sum().reset_index()
                    )
                    with g2: st.dataframe(grp_emp, use_container_width=True)

        st.caption("صفحة الأدمِن مفتوحة لمدّة 30 دقيقة من وقت الفتح.")
