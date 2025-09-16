# MegaCRM_Streamlit_App.py — CRM + Finance (MB/Bizerte)
# تغييرات:
# - Fix: sort على Date_dt مع fallback على Date لتفادي KeyError
# - إخفاء الأوراق: *_PAIEMENTS و "_" و أوراق المالية (Revenue*/Dépense*)
# - حذف قسم الدفوعات من واجهة الموظّف
# - في المصاريف: الموظف يضيف فقط شنوة خلّص (Libellé) والمبلغ (Montant) والباقي اختياري

import json, time
import streamlit as st
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime, date, timedelta
from PIL import Image

# ========== Page config ==========
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

# ===== CRM schema =====
EXPECTED_HEADERS = [
    "Nom & Prénom","Téléphone","Type de contact","Formation",
    "Remarque","Date ajout","Date de suivi","Alerte",
    "Inscription","Employe","Tag"
]

# ===== Finance helpers =====
FIN_COLUMNS = ["Date", "Libellé", "Montant", "Mode", "Employé", "Catégorie", "Note"]
FIN_MONTHS_FR = ["Janvier","Février","Mars","Avril","Mai","Juin","Juillet","Aout","Septembre","Octobre","Novembre","Décembre"]

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

# ===== Common helpers =====
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

# ===== تحميل بيانات CRM (مع إخفاء أوراق المالية والدفوعات) =====
@st.cache_data(ttl=600)
def load_all_data():
    sh = client.open_by_key(SPREADSHEET_ID)
    worksheets = sh.worksheets()
    all_dfs, all_employes = [], []

    for ws in worksheets:
        title = ws.title.strip()

        # اخفاء:
        if title.endswith("_PAIEMENTS"):            # دفوعات
            continue
        if title.startswith("_"):                   # سيستام
            continue
        if title.startswith("Revenue ") or title.startswith("Dépense "):  # أوراق المالية
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

# ======= تبويب المالية (MB/Bizerte) =======
if tab_choice == "Finance (MB/Bizerte)":
    st.title("💸 المالية — مداخيل/مصاريف (منزل بورقيبة & بنزرت)")

    with st.sidebar:
        st.markdown("---")
        st.subheader("🔧 إعدادات المالية")
        branch = st.selectbox("الفرع", ["Menzel Bourguiba", "Bizerte"], key="fin_branch")
        kind   = st.radio("النوع", ["Revenus","Dépenses"], horizontal=True, key="fin_kind")
        mois   = st.selectbox("الشهر", FIN_MONTHS_FR, index=datetime.now().month-1, key="fin_month")

        # كلمة سر الفرع
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

    # قراءة البيانات
    df_fin = fin_read_df(client, SPREADSHEET_ID, fin_title)
    df_view = df_fin.copy()

    # الموظف يشوف عملياتو فقط (إذا لزم)
    if role == "موظف" and employee and "Employé" in df_view.columns:
        df_view = df_view[df_view["Employé"].fillna("").str.strip().str.lower() == employee.strip().lower()]

    # فلاتر
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

    # ملخّص الشهر
    with st.expander("📊 ملخّص الفرع للشهر"):
        rev_df = fin_read_df(client, SPREADSHEET_ID, fin_month_title(mois, "Revenus", branch))
        dep_df = fin_read_df(client, SPREADSHEET_ID, fin_month_title(mois, "Dépenses", branch))
        rev = rev_df["Montant"].sum() if "Montant" in rev_df.columns else 0
        dep = dep_df["Montant"].sum() if "Montant" in dep_df.columns else 0
        a,b,c = st.columns(3)
        a.metric("مداخيل", f"{rev:,.2f}")
        b.metric("مصاريف", f"{dep:,.2f}")
        c.metric("الصافي", f"{(rev-dep):,.2f}")

    # ================== إضافة عملية جديدة ==================
    st.markdown("---")
    st.markdown("### ➕ إضافة عملية جديدة")

    selected_client_info = None
    client_default_lib = ""
    client_default_emp = employee or ""

    # Revenus: (اختياري) ربط بعميل مُسجّل لاقتراح Libellé/Employé
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
                if not client_default_emp:
                    client_default_emp = selected_client_info["emp"]

    # الفورم:
    # - Revenus: Libellé + Montant + بقية الحقول (عادي)
    # - Dépenses: للموظّف فقط يلزم "شنوة خلّص" (Libellé) و"المبلغ" (Montant) — الباقي اختياري
    with st.form("fin_add_row"):
        if kind == "Revenus":
            d1, d2, d3 = st.columns(3)
            date_val = d1.date_input("Date", value=datetime.today(), key="fin_date")
            libelle  = d2.text_input("Libellé", client_default_lib, key="fin_lib")
            montant  = d3.number_input("Montant", min_value=0.0, step=1.0, format="%.2f", key="fin_montant")

            e1, e2, e3 = st.columns(3)
            mode      = e1.selectbox("Mode", ["Espèces","Virement","Carte","Autre"], key="fin_mode")
            employe   = e2.text_input("Employé", value=client_default_emp or "", key="fin_emp")
            categorie = e3.text_input("Catégorie", value="Vente", key="fin_cat")

            note_default = ""
            if selected_client_info:
                note_default = f"Client: {selected_client_info['name']} / {selected_client_info['formation']}"
            note = st.text_area("Note", note_default, key="fin_note")

        else:  # Dépenses
            d1, d2, d3 = st.columns(3)
            date_val = d1.date_input("Date", value=datetime.today(), key="fin_date_dep")
            libelle  = d2.text_input("Libellé (شنوة خلّصت؟)", "", key="fin_lib_dep")
            montant  = d3.number_input("Montant", min_value=0.0, step=1.0, format="%.2f", key="fin_montant_dep")

            e1, e2, e3 = st.columns(3)
            mode      = e1.selectbox("Mode", ["Espèces","Virement","Carte","Autre"], key="fin_mode_dep")
            employe   = e2.text_input("Employé", value=(employee or ""), key="fin_emp_dep")
            categorie = e3.text_input("Catégorie", value="Achat", key="fin_cat_dep")

            note = st.text_area("Note (اختياري)", "", key="fin_note_dep")

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

# ================== CRM (لوحة عامة) ==================
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

# ===== إحصائيات حسب الموظّف =====
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

# ترتيب: الأكثر تنبيهات ثم الأكثر عملاء
grp_base = grp_base.sort_values(by=["تنبيهات", "Clients"], ascending=[False, False])

st.markdown("#### حسب الموظّف")
st.dataframe(grp_base, use_container_width=True)

# ===== بحث عام برقم الهاتف =====
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
        def _mark_alert_cell(val: str):
            s = str(val).strip()
            if not s: return ''
            if "متأخرة" in s: return 'background-color: #ffe6b3; color: #7a4e00'
            return 'background-color: #ffcccc; color: #7a0000'
        def _highlight_inscrit_row(row: pd.Series):
            insc = str(row.get("Inscription", "")).strip().lower()
            return ['background-color: #d6f5e8' if insc in ("inscrit","oui") else '' for _ in row.index]
        styled_global = (
            search_df[display_cols]
            .style.apply(_highlight_inscrit_row, axis=1)
            .applymap(_mark_alert_cell, subset=["Alerte"])
        )
        st.dataframe(styled_global, use_container_width=True)
        st.markdown("---")
