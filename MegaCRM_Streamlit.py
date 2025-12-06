# MegaCRM_Streamlit.py
# CRM فقط + أرشيف — بدون أي كود مداخيل/مصاريف
# + أزرار تفتح MegaPay و Mega Formateur و AttendanceHub
# + فلترة بالتكوين داخل لوحة الموظّف
# + إحصائيات شهرية فيها Clients اليوم و Inscrits اليوم
# + تاريخ ميلاد العميل + تنبيه أعياد الميلاد
# + تقرير يومي عبر WhatsApp للإدارة مع الملاحظات كاملة

import json, urllib.parse, time
import streamlit as st
import pandas as pd
import gspread
import gspread.exceptions as gse
from google.oauth2.service_account import Credentials
from datetime import datetime, date, timedelta, timezone
from PIL import Image

# ============ إعداد الصفحة ============
st.set_page_config(page_title="MegaCRM", layout="wide", initial_sidebar_state="expanded")
st.markdown(
    """
    <div style='text-align:center'>
      <h1>📊 CRM MEGA FORMATION - إدارة العملاء</h1>
    </div>
    <hr/>
    """,
    unsafe_allow_html=True,
)

# ============ Sidebar Links ============
with st.sidebar:
    st.markdown("### 💵 إدارة المداخيل والمصاريف")
    st.markdown(
        """
        <a href="https://megapay.streamlit.app/" target="_blank"
           style="
              display:inline-block;
              background:linear-gradient(90deg,#16a085,#1abc9c);
              color:#fff;
              padding:10px 18px;
              border-radius:10px;
              text-decoration:none;
              font-weight:600;
              font-size:15px;
              text-align:center;
              width:100%;
              box-shadow:0 4px 8px rgba(0,0,0,0.15);
           ">
           🚀 فتح MegaPay
        </a>
        """,
        unsafe_allow_html=True,
    )

    st.markdown("---")

    st.markdown("### 👨‍🏫 بوابة المكوّنين")
    st.markdown(
        """
        <a href="https://mega-formateur.streamlit.app/" target="_blank"
           style="
              display:inline-block;
              background:linear-gradient(90deg,#0078d7,#00b7ff);
              color:#fff;
              padding:10px 18px;
              border-radius:10px;
              text-decoration:none;
              font-weight:600;
              font-size:15px;
              text-align:center;
              width:100%;
              box-shadow:0 4px 8px rgba(0,0,0,0.15);
           ">
           🔀 فتح Mega Formateur
        </a>
        """,
        unsafe_allow_html=True,
    )

    st.markdown("---")

    st.markdown("### 🕒 غيابات المتربّصين")
    st.markdown(
        """
        <a href="https://crm-apprenant.streamlit.app//" target="_blank"
           style="
              display:inline-block;
              background:linear-gradient(90deg,#c0392b,#e74c3c);
              color:#fff;
              padding:10px 18px;
              border-radius:10px;
              text-decoration:none;
              font-weight:600;
              font-size:15px;
              text-align:center;
              width:100%;
              box-shadow:0 4px 8px rgba(0,0,0,0.15);
           ">
           🕒 فتح AttendanceHub
        </a>
        """,
        unsafe_allow_html=True,
    )

# ============ Google Auth ============
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
        # لوكال
        creds = Credentials.from_service_account_file("service_account.json", scopes=SCOPE)
        client = gspread.authorize(creds)
        sheet_id = "PUT_YOUR_SHEET_ID_HERE"
        return client, sheet_id

client, SPREADSHEET_ID = make_client_and_sheet_id()

# ============ ثوابت الجداول ============
EXPECTED_HEADERS = [
    "Nom & Prénom",      # 0
    "Téléphone",         # 1
    "Date de naissance", # 2
    "Type de contact",   # 3
    "Formation",         # 4
    "Remarque",          # 5
    "Date ajout",        # 6
    "Date de suivi",     # 7
    "Alerte",            # 8
    "Inscription",       # 9
    "Employe",           # 10
    "Tag",               # 11,
]

REASSIGN_LOG_SHEET   = "Reassign_Log"
REASSIGN_LOG_HEADERS = ["timestamp","moved_by","src_employee","dst_employee","client_name","phone"]

# ============ Helpers ============
def fmt_date(d: date | None) -> str:
    return d.strftime("%d/%m/%Y") if isinstance(d, date) else ""

def normalize_tn_phone(s: str) -> str:
    digits = "".join(ch for ch in str(s) if ch.isdigit())
    if digits.startswith("216"):
        return digits
    if len(digits) == 8:
        return "216" + digits
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
    if not s:
        return ""
    if "متأخر" in s:
        return "background-color:#ffe6b3;color:#7a4e00"
    return "background-color:#ffcccc;color:#7a0000"

def highlight_inscrit_row(row: pd.Series):
    insc = str(row.get("Inscription", "")).strip().lower()
    return ["background-color:#d6f5e8" if insc in ("inscrit", "oui") else "" for _ in row.index]

# ===================== Sheets Utils (Backoff + Cache) =====================
def get_spreadsheet():
    if st.session_state.get("sh_id") == SPREADSHEET_ID and "sh_obj" in st.session_state:
        return st.session_state["sh_obj"]
    last_err = None
    for i in range(5):
        try:
            sh = client.open_by_key(SPREADSHEET_ID)
            st.session_state["sh_obj"] = sh
            st.session_state["sh_id"] = SPREADSHEET_ID
            return sh
        except gse.APIError as e:
            last_err = e
            time.sleep(0.5 * (2**i))
    st.error("تعذر فتح Google Sheet (ربما الكوتا تعدّت).")
    raise last_err

def ensure_ws(title: str, columns: list[str]):
    sh = get_spreadsheet()
    try:
        ws = sh.worksheet(title)
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title=title, rows="2000", cols=str(max(len(columns), 8)))
        ws.update("1:1", [columns])
        return ws
    header = ws.row_values(1)
    if not header or header[:len(columns)] != columns:
        ws.update("1:1", [columns])
    return ws

# ============ تحميل كل أوراق الموظفين (باستعمال أسماء الأعمدة) ============
@st.cache_data(ttl=600)
def load_all_data():
    sh = get_spreadsheet()
    all_dfs, all_emps = [], []

    for ws in sh.worksheets():
        title = ws.title.strip()

        # نستثني أوراق المداخيل والأنظمة الداخلية
        if title.endswith("_PAIEMENTS"):
            continue
        if title.startswith("_"):
            continue
        if title in (REASSIGN_LOG_SHEET,):
            continue

        all_emps.append(title)

        rows = ws.get_all_values()
        if not rows:
            # لو الورقة فارغة، نعمل header بالصيغة الجديدة
            ws.update("1:1", [EXPECTED_HEADERS])
            rows = ws.get_all_values()

        header_row = rows[0] if rows else []
        data_rows = rows[1:] if len(rows) > 1 else []

        # مابينغ من اسم العمود → index
        header_map = {str(name).strip(): idx for idx, name in enumerate(header_row)}

        fixed = []
        for r in data_rows:
            r = list(r or [])
            new_row = []
            # نركّب صف جديد حسب EXPECTED_HEADERS
            for col_name in EXPECTED_HEADERS:
                idx = header_map.get(col_name)
                if idx is not None and idx < len(r):
                    new_row.append(r[idx])
                else:
                    new_row.append("")  # لو الكولون موش موجود في النسخة القديمة
            fixed.append(new_row)

        df = pd.DataFrame(fixed, columns=EXPECTED_HEADERS)
        df["__sheet_name"] = title
        all_dfs.append(df)

    big = (
        pd.concat(all_dfs, ignore_index=True)
        if all_dfs
        else pd.DataFrame(columns=EXPECTED_HEADERS + ["__sheet_name"])
    )
    return big, all_emps

df_all, all_employes = load_all_data()

# ============ Sidebar ============
try:
    st.sidebar.image(Image.open("logo.png"), use_container_width=True)
except Exception:
    pass

tab_choice = st.sidebar.radio("📑 اختر تبويب:", ["CRM", "أرشيف"], index=0)
role = st.sidebar.radio("الدور", ["موظف", "أدمن"], horizontal=True)
employee = (
    st.sidebar.selectbox("👨‍💼 اختر الموظّف (ورقة Google Sheets)", all_employes)
    if (role == "موظف" and all_employes)
    else None
)

# ============ أقفال ============
def admin_unlocked() -> bool:
    ok = st.session_state.get("admin_ok", False)
    ts = st.session_state.get("admin_ok_at")
    return bool(ok and ts and (datetime.now() - ts) <= timedelta(minutes=30))

def admin_lock_ui():
    with st.sidebar.expander(
        "🔐 إدارة (Admin)", expanded=(role == "أدمن" and not admin_unlocked())
    ):
        if admin_unlocked():
            if st.button("قفل صفحة الأدمِن"):
                st.session_state["admin_ok"] = False
                st.session_state["admin_ok_at"] = None
                st.rerun()
        else:
            admin_pwd = st.text_input("كلمة سرّ الأدمِن", type="password")
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

def emp_pwd_for(emp_name: str) -> str:
    try:
        mp = st.secrets["employee_passwords"]
        return str(mp.get(emp_name, mp.get("_default", "1234")))
    except Exception:
        return "1234"

def emp_unlocked(emp_name: str) -> bool:
    ok = st.session_state.get(f"emp_ok::{emp_name}", False)
    ts = st.session_state.get(f"emp_ok_at::{emp_name}")
    return bool(ok and ts and (datetime.now() - ts) <= timedelta(minutes=15))

def emp_lock_ui(emp_name: str, ns: str = ""):
    ns_prefix = f"{emp_name}::{ns}" if ns else emp_name
    with st.expander(
        f"🔐 حماية ورقة الموظّف: {emp_name}", expanded=not emp_unlocked(emp_name)
    ):
        if emp_unlocked(emp_name):
            c1, c2 = st.columns(2)
            c1.success("مفتوح (15 دقيقة).")
            if c2.button("قفل الآن", key=f"btn_close::{ns_prefix}"):
                st.session_state[f"emp_ok::{emp_name}"] = False
                st.session_state[f"emp_ok_at::{emp_name}"] = None
        else:
            pwd_try = st.text_input(
                "أدخل كلمة السرّ", type="password", key=f"pwd::{ns_prefix}"
            )
            if st.button("فتح", key=f"btn_open::{ns_prefix}"):
                if pwd_try == emp_pwd_for(emp_name):
                    st.session_state[f"emp_ok::{emp_name}"] = True
                    st.session_state[f"emp_ok_at::{emp_name}"] = datetime.now()
                    st.success("تم الفتح لمدة 15 دقيقة.")
                else:
                    st.error("كلمة سرّ غير صحيحة.")

# ============ مشتقات عامة ============
df_all = df_all.copy()
if not df_all.empty:
    df_all["DateAjout_dt"] = pd.to_datetime(
        df_all["Date ajout"], dayfirst=True, errors="coerce"
    )
    df_all["DateSuivi_dt"] = pd.to_datetime(
        df_all["Date de suivi"], dayfirst=True, errors="coerce"
    )
    df_all["Mois"] = df_all["DateAjout_dt"].dt.strftime("%m-%Y")

    today = datetime.now().date()
    base_alert = df_all["Alerte"].fillna("").astype(str).str.strip()
    dsv_date = df_all["DateSuivi_dt"].dt.date
    due_today = dsv_date.eq(today).fillna(False)
    overdue = dsv_date.lt(today).fillna(False)

    df_all["Alerte_view"] = base_alert
    df_all.loc[base_alert.eq("") & overdue, "Alerte_view"] = "⚠️ متابعة متأخرة"
    df_all.loc[base_alert.eq("") & due_today, "Alerte_view"] = "⏰ متابعة اليوم"

    df_all["Téléphone_norm"] = df_all["Téléphone"].apply(normalize_tn_phone)
    ALL_PHONES = set(df_all["Téléphone_norm"].dropna().astype(str))

    df_all["Inscription_norm"] = (
        df_all["Inscription"].fillna("").astype(str).str.strip().str.lower()
    )
    inscrit_mask = df_all["Inscription_norm"].isin(["oui", "inscrit"])
    df_all.loc[inscrit_mask, "Date de suivi"] = ""
    df_all.loc[inscrit_mask, "Alerte_view"] = ""
else:
    df_all["Alerte_view"] = ""
    df_all["Mois"] = ""
    df_all["Téléphone_norm"] = ""
    ALL_PHONES = set()

# ============ Dashboard سريع ============
st.subheader("لوحة إحصائيات سريعة")
df_dash = df_all.copy()
if df_dash.empty:
    st.info("ما فماش داتا للعرض.")
else:
    df_dash["DateAjout_dt"] = pd.to_datetime(
        df_dash["Date ajout"], dayfirst=True, errors="coerce"
    )
    df_dash["DateSuivi_dt"] = pd.to_datetime(
        df_dash["Date de suivi"], dayfirst=True, errors="coerce"
    )
    today = datetime.now().date()
    df_dash["Inscription_norm"] = (
        df_dash["Inscription"].fillna("").astype(str).str.strip().str.lower()
    )
    df_dash["Alerte_norm"] = (
        df_dash["Alerte_view"].fillna("").astype(str).str.strip()
    )

    added_today_mask = df_dash["DateAjout_dt"].dt.date.eq(today)
    registered_today_mask = df_dash["Inscription_norm"].isin(
        ["oui", "inscrit"]
    ) & added_today_mask
    alert_now_mask = df_dash["Alerte_norm"].ne("")

    total_clients = int(len(df_dash))
    added_today = int(added_today_mask.sum())
    registered_today = int(registered_today_mask.sum())
    alerts_now = int(alert_now_mask.sum())
    registered_total = int((df_dash["Inscription_norm"] == "oui").sum())
    rate = round((registered_total / total_clients) * 100, 2) if total_clients else 0.0

    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("👥 إجمالي العملاء", f"{total_clients}")
    c2.metric("🆕 المضافون اليوم", f"{added_today}")
    c3.metric("✅ المسجّلون اليوم", f"{registered_today}")
    c4.metric("🚨 التنبيهات الحالية", f"{alerts_now}")
    c5.metric("📈 نسبة التسجيل الإجمالية", f"{rate}%")

# ============ إحصائيات شهرية ============
st.markdown("---")
st.subheader("📅 إحصائيات شهرية (العملاء)")
if not df_all.empty and "DateAjout_dt" in df_all.columns:
    df_all["MonthStr"] = df_all["DateAjout_dt"].dt.strftime("%Y-%m")
    months_avail = sorted(df_all["MonthStr"].dropna().unique(), reverse=True)
    month_pick = (
        st.selectbox("اختر شهر", months_avail, index=0) if months_avail else None
    )
    if month_pick:
        df_month = df_all[df_all["MonthStr"] == month_pick].copy()

        total_clients_m = len(df_month)
        total_inscrits_m = int((df_month["Inscription_norm"] == "oui").sum())
        alerts_m = int(
            df_month["Alerte_view"]
            .fillna("")
            .astype(str)
            .str.strip()
            .ne("")
            .sum()
        )
        rate_m = (
            round((total_inscrits_m / total_clients_m) * 100, 2)
            if total_clients_m
            else 0.0
        )

        c1, c2, c3, c4 = st.columns(4)
        c1.metric("👥 عملاء هذا الشهر", f"{total_clients_m}")
        c2.metric("✅ مسجّلون", f"{total_inscrits_m}")
        c3.metric("🚨 تنبيهات", f"{alerts_m}")
        c4.metric("📈 نسبة التسجيل", f"{rate_m}%")

        st.markdown("#### 👨‍💼 حسب الموظّف")
        grp_emp = (
            df_month.groupby("__sheet_name", dropna=False)
            .agg(
                Clients=("Nom & Prénom", "count"),
                Inscrits=(
                    "Inscription_norm",
                    lambda x: (x == "oui").sum(),
                ),
                Alerts=(
                    "Alerte_view",
                    lambda x: (
                        x.fillna("").astype(str).str.strip() != ""
                    ).sum(),
                ),
            )
            .reset_index()
            .rename(columns={"__sheet_name": "الموظف"})
        )

        _today = datetime.now().date()
        df_all_dates = df_all.copy()
        df_all_dates["DateAjout_dt"] = pd.to_datetime(
            df_all_dates["Date ajout"], dayfirst=True, errors="coerce"
        )

        daily_clients_map = (
            df_all_dates[df_all_dates["DateAjout_dt"].dt.date == _today]
            .groupby("__sheet_name")["Nom & Prénom"]
            .count()
        )

        daily_inscrits_map = (
            df_all_dates[
                (df_all_dates["DateAjout_dt"].dt.date == _today)
                & (
                    df_all_dates["Inscription"]
                    .fillna("")
                    .astype(str)
                    .str.strip()
                    .str.lower()
                    .isin(["oui", "inscrit"])
                )
            ]
            .groupby("__sheet_name")["Nom & Prénom"]
            .count()
        )

        grp_emp = grp_emp.merge(
            daily_clients_map.rename("Clients اليوم")
            .reset_index()
            .rename(columns={"__sheet_name": "الموظف"}),
            on="الموظف",
            how="left",
        )
        grp_emp = grp_emp.merge(
            daily_inscrits_map.rename("Inscrits اليوم")
            .reset_index()
            .rename(columns={"__sheet_name": "الموظف"}),
            on="الموظف",
            how="left",
        )

        grp_emp["Clients اليوم"] = grp_emp["Clients اليوم"].fillna(0).astype(int)
        grp_emp["Inscrits اليوم"] = grp_emp["Inscrits اليوم"].fillna(0).astype(int)

        grp_emp["% تسجيل"] = (
            (grp_emp["Inscrits"] / grp_emp["Clients"])
            .replace([float("inf"), float("nan")], 0)
            .mul(100)
            .round(2)
        )

        cols_order = [
            "الموظف",
            "Clients",
            "Clients اليوم",
            "Inscrits اليوم",
            "Inscrits",
            "% تسجيل",
            "Alerts",
        ]
        grp_emp = grp_emp[[c for c in cols_order if c in grp_emp.columns]]

        st.dataframe(
            grp_emp.sort_values(["Inscrits", "Clients"], ascending=False),
            use_container_width=True,
        )

# ============ بحث عام برقم الهاتف ============
st.subheader("🔎 بحث عام برقم الهاتف")
global_phone = st.text_input("اكتب رقم الهاتف (8 أرقام محلية أو 216XXXXXXXX)")
if global_phone.strip():
    q = normalize_tn_phone(global_phone)
    sd = df_all.copy()
    sd["Téléphone_norm"] = sd["Téléphone"].apply(normalize_tn_phone)
    sd["Alerte"] = sd.get("Alerte_view", "")
    sd = sd[sd["Téléphone_norm"] == q]
    if sd.empty:
        st.info("❕ ما لقيتش عميل بهذا الرقم.")
    else:
        disp = [c for c in EXPECTED_HEADERS if c in sd.columns]
        st.dataframe(
            sd[disp]
            .style.apply(highlight_inscrit_row, axis=1)
            .applymap(mark_alert_cell, subset=["Alerte"]),
            use_container_width=True,
        )
        st.markdown("---")

# ============ تبويب CRM للموظّف ============
if role == "موظف" and employee:
    emp_lock_ui(employee, ns="crm")
    if not emp_unlocked(employee):
        st.info("🔒 أدخل كلمة سرّ الموظّف لفتح الورقة.")
        st.stop()

    st.subheader(f"📁 لوحة {employee}")
    df_emp_raw = df_all[df_all["__sheet_name"] == employee].copy()
    if df_emp_raw.empty:
        st.warning("⚠️ لا يوجد أي عملاء بعد.")
        st.stop()

    # ===== 🎂 تنبيهات أعياد الميلاد =====
    try:
        if "Date de naissance" in df_emp_raw.columns:
            df_birth = df_emp_raw.copy()
            df_birth["Birth_dt"] = pd.to_datetime(
                df_birth["Date de naissance"], dayfirst=True, errors="coerce"
            )
            today = datetime.now().date()
            bday_mask = (
                df_birth["Birth_dt"].dt.month.eq(today.month)
                & df_birth["Birth_dt"].dt.day.eq(today.day)
            )
            bday_df = df_birth[bday_mask]

            if not bday_df.empty:
                st.markdown("### 🎂 تنبيهات أعياد الميلاد اليوم")
                for _, row in bday_df.iterrows():
                    name = str(row.get("Nom & Prénom", "")).strip()
                    phone_norm = normalize_tn_phone(row.get("Téléphone", ""))
                    phone_display = format_display_phone(phone_norm)

                    st.success(f"اليوم عيد ميلاد: **{name}** — {phone_display}")

                    default_msg = (
                        f"🎂 عيد ميلاد سعيد {name}! "
                        "كامل فريق Mega Formation يتمنّى لك سنة مليانة نجاح وتوفيق 🤍"
                    )
                    if phone_norm:
                        wa_url = (
                            f"https://wa.me/{phone_norm}?text="
                            f"{urllib.parse.quote(default_msg)}"
                        )
                        st.markdown(f"[📲 بعث تهنئة على واتساب]({wa_url})")
    except Exception as e:
        st.warning(f"تعذّر حساب أعياد الميلاد: {e}")

    # نسخة للعمل على الفلترة
    df_emp = df_emp_raw.copy()
    df_emp["DateAjout_dt"] = pd.to_datetime(
        df_emp["Date ajout"], dayfirst=True, errors="coerce"
    )
    df_emp = df_emp.dropna(subset=["DateAjout_dt"])
    df_emp["Mois"] = df_emp["DateAjout_dt"].dt.strftime("%m-%Y")
    month_options = sorted(df_emp["Mois"].dropna().unique(), reverse=True)
    month_filter = st.selectbox("🗓️ اختر شهر الإضافة", month_options)

    filtered_df = df_emp[df_emp["Mois"] == month_filter].copy()

    # ===== فلترة بالتكوين =====
    st.markdown("#### 🔎 فلترة حسب التكوين")
    if filtered_df.empty:
        st.info("لا توجد بيانات لهذا الشهر.")
    else:
        formations = sorted(
            [
                f
                for f in filtered_df["Formation"]
                .fillna("")
                .astype(str)
                .str.strip()
                .unique()
                if f
            ]
        )
        form_choice = st.selectbox(
            "اختر التكوين", ["(الكل)"] + formations, index=0
        )
        if form_choice and form_choice != "(الكل)":
            filtered_df = filtered_df[
                filtered_df["Formation"].astype(str).str.strip() == form_choice
            ]

    # ===== عرض قائمة العملاء =====
    def render_table(df_disp: pd.DataFrame):
        if df_disp.empty:
            st.info("لا توجد بيانات.")
            return
        _df = df_disp.copy()
        _df["Alerte"] = _df.get("Alerte_view", "")
        styled = (
            _df[[c for c in EXPECTED_HEADERS if c in _df.columns]]
            .style.apply(highlight_inscrit_row, axis=1)
            .applymap(mark_alert_cell, subset=["Alerte"])
            .applymap(color_tag, subset=["Tag"])
        )
        st.dataframe(styled, use_container_width=True)

    st.markdown("### 📋 قائمة العملاء")
    render_table(filtered_df)

    # --- عرض العملاء الذين لديهم تنبيهات ---
    if (not filtered_df.empty) and st.checkbox("🔴 عرض العملاء الذين لديهم تنبيهات"):
        _df_alerts = filtered_df.copy()
        _df_alerts["Alerte"] = _df_alerts.get("Alerte_view", "")
        alerts_df = _df_alerts[
            _df_alerts["Alerte"].fillna("").astype(str).str.strip() != ""
        ]
        st.markdown("### 🚨 عملاء مع تنبيهات")
        render_table(alerts_df)

    # ================== ➕ أضف عميل جديد (للموظّف) ==================
    st.markdown("### ➕ أضف عميل جديد")
    with st.form(f"emp_add_client_form::{employee}"):
        col1, col2 = st.columns(2)
        with col1:
            nom_emp = st.text_input(
                "👤 الاسم و اللقب", key=f"emp_add_nom::{employee}"
            )
            tel_emp = st.text_input(
                "📞 رقم الهاتف", key=f"emp_add_tel::{employee}"
            )
            formation_emp = st.text_input(
                "📚 التكوين", key=f"emp_add_form::{employee}"
            )
            inscription_emp = st.selectbox(
                "🟢 التسجيل",
                ["Pas encore", "Inscrit"],
                key=f"emp_add_insc::{employee}",
            )
        with col2:
            type_contact_emp = st.selectbox(
                "📞 نوع الاتصال",
                ["Visiteur", "Appel téléphonique", "WhatsApp", "Social media"],
                key=f"emp_add_type::{employee}",
            )
            birthday_emp = st.date_input(
                "🎂 تاريخ الميلاد", key=f"emp_add_birth::{employee}"
            )
            date_ajout_emp = st.date_input(
                "🕓 تاريخ الإضافة",
                value=date.today(),
                key=f"emp_add_dt_add::{employee}",
            )
            date_suivi_emp = st.date_input(
                "📆 تاريخ المتابعة",
                value=date.today(),
                key=f"emp_add_dt_suivi::{employee}",
            )

        remarque_emp = st.text_area(
            "🗒️ ملاحظة (اختياري)", key=f"emp_add_rem::{employee}"
        )

        submitted_add_emp = st.form_submit_button("📥 أضف العميل")

    if submitted_add_emp:
        try:
            tel_norm = normalize_tn_phone(tel_emp)
            if not (nom_emp and tel_norm and formation_emp):
                st.error("❌ حقول أساسية ناقصة (الاسم، الهاتف، التكوين).")
            elif tel_norm in ALL_PHONES:
                st.warning("⚠️ الرقم موجود مسبقًا في قاعدة البيانات.")
            else:
                insc_val = "Oui" if inscription_emp == "Inscrit" else "Pas encore"
                row_to_append = [
                    nom_emp.strip(),
                    tel_norm,
                    fmt_date(birthday_emp),  # Date de naissance
                    type_contact_emp,
                    formation_emp.strip(),
                    remarque_emp.strip(),
                    fmt_date(date_ajout_emp),
                    fmt_date(date_suivi_emp),
                    "",
                    insc_val,
                    employee,
                    "",
                ]
                sh = get_spreadsheet()
                ws_emp = sh.worksheet(employee)
                header = ws_emp.row_values(1) or []
                if not header or header[: len(EXPECTED_HEADERS)] != EXPECTED_HEADERS:
                    ws_emp.update("1:1", [EXPECTED_HEADERS])
                ws_emp.append_row(row_to_append)
                st.success("✅ تم إضافة العميل بنجاح.")
                st.cache_data.clear()
                st.rerun()
        except Exception as e:
            st.error(f"❌ خطأ أثناء الإضافة: {e}")

    # ================== ✏️ تعديل عميل ==================
    st.markdown("### ✏️ تعديل بيانات عميل")
    df_emp_edit = df_emp_raw.copy()
    df_emp_edit["Téléphone_norm"] = df_emp_edit["Téléphone"].apply(normalize_tn_phone)
    options = {
        f"[{i}] {r['Nom & Prénom']} — {format_display_phone(r['Téléphone_norm'])}": r[
            "Téléphone_norm"
        ]
        for i, r in df_emp_edit.iterrows()
        if str(r.get("Téléphone", "")).strip() != ""
    }

    if options:
        chosen_key = st.selectbox(
            "اختر العميل (بالاسم/الهاتف)", list(options.keys())
        )
        chosen_phone = options[chosen_key]
        cur_row = df_emp_edit[df_emp_edit["Téléphone_norm"] == chosen_phone].iloc[0]

        with st.form(f"edit_client_form::{employee}"):
            col1, col2 = st.columns(2)
            with col1:
                new_name = st.text_input(
                    "👤 الاسم و اللقب", value=str(cur_row["Nom & Prénom"])
                )
                new_phone_raw = st.text_input(
                    "📞 رقم الهاتف", value=str(cur_row["Téléphone"])
                )
                new_formation = st.text_input(
                    "📚 التكوين", value=str(cur_row["Formation"])
                )
            with col2:
                raw_birth = str(cur_row.get("Date de naissance", "")).strip()
                if raw_birth:
                    dt_birth = pd.to_datetime(
                        raw_birth, dayfirst=True, errors="coerce"
                    )
                    default_birth = (
                        dt_birth.date() if pd.notna(dt_birth) else date.today()
                    )
                else:
                    default_birth = date.today()

                new_birth = st.date_input(
                    "🎂 تاريخ الميلاد", value=default_birth
                )

                new_ajout = st.date_input(
                    "🕓 تاريخ الإضافة",
                    value=pd.to_datetime(
                        cur_row["Date ajout"], dayfirst=True, errors="coerce"
                    ).date(),
                )

                new_suivi = st.date_input(
                    "📆 تاريخ المتابعة",
                    value=(
                        pd.to_datetime(
                            cur_row["Date de suivi"],
                            dayfirst=True,
                            errors="coerce",
                        ).date()
                        if str(cur_row["Date de suivi"]).strip()
                        else date.today()
                    ),
                )

                new_insc = st.selectbox(
                    "🟢 التسجيل",
                    ["Pas encore", "Inscrit"],
                    index=(
                        1
                        if str(cur_row["Inscription"])
                        .strip()
                        .lower()
                        == "oui"
                        else 0
                    ),
                )

            extra_note = st.text_area(
                "➕ أضف ملاحظة جديدة (طابع زمني)",
                placeholder="اكتب ملاحظة لإلحاقها…",
            )
            submitted = st.form_submit_button("💾 حفظ التعديلات")

        if submitted:
            try:
                ws = get_spreadsheet().worksheet(employee)
                values = ws.get_all_values()
                header = values[0] if values else []
                tel_idx = header.index("Téléphone")
                row_idx = None
                for i, r in enumerate(values[1:], start=2):
                    if len(r) > tel_idx and normalize_tn_phone(r[tel_idx]) == chosen_phone:
                        row_idx = i
                        break
                if not row_idx:
                    st.error("❌ تعذّر إيجاد الصف.")
                    st.stop()

                col_map = {
                    h: (EXPECTED_HEADERS.index(h) + 1)
                    for h in [
                        "Nom & Prénom",
                        "Téléphone",
                        "Date de naissance",
                        "Formation",
                        "Date ajout",
                        "Date de suivi",
                        "Inscription",
                        "Remarque",
                    ]
                }

                new_phone_norm = normalize_tn_phone(new_phone_raw)
                if not new_name.strip():
                    st.error("❌ الاسم مطلوب.")
                    st.stop()
                if not new_phone_norm.strip():
                    st.error("❌ الهاتف مطلوب.")
                    st.stop()

                phones_except = set(df_all["Téléphone_norm"]) - {
                    normalize_tn_phone(chosen_phone)
                }
                if new_phone_norm in phones_except:
                    st.error("⚠️ الرقم موجود مسبقًا.")
                    st.stop()

                ws.update_cell(row_idx, col_map["Nom & Prénom"], new_name.strip())
                ws.update_cell(row_idx, col_map["Téléphone"], new_phone_norm)
                ws.update_cell(
                    row_idx,
                    col_map["Date de naissance"],
                    fmt_date(new_birth),
                )
                ws.update_cell(
                    row_idx, col_map["Formation"], new_formation.strip()
                )
                ws.update_cell(row_idx, col_map["Date ajout"], fmt_date(new_ajout))
                ws.update_cell(row_idx, col_map["Date de suivi"], fmt_date(new_suivi))
                ws.update_cell(
                    row_idx,
                    col_map["Inscription"],
                    "Oui" if new_insc == "Inscrit" else "Pas encore",
                )

                if extra_note.strip():
                    old_rem = (
                        ws.cell(row_idx, col_map["Remarque"]).value or ""
                    )
                    stamp = datetime.now().strftime("%d/%m/%Y %H:%M")
                    appended = (
                        old_rem + "\n" if old_rem else ""
                    ) + f"[{stamp}] {extra_note.strip()}"
                    ws.update_cell(row_idx, col_map["Remarque"], appended)

                st.success("✅ تم حفظ التعديلات")
                st.cache_data.clear()
            except Exception as e:
                st.error(f"❌ خطأ: {e}")

    # ================== 🎨 Tag لون ==================
    st.markdown("### 🎨 Tag لون")
    scope_df = filtered_df if not filtered_df.empty else df_emp_raw
    scope_df = scope_df.copy()
    scope_df["Téléphone_norm"] = scope_df["Téléphone"].apply(normalize_tn_phone)
    tel_key2 = st.selectbox(
        "اختر العميل للتلوين",
        [
            f"{r['Nom & Prénom']} — {format_display_phone(normalize_tn_phone(r['Téléphone']))}"
            for _, r in scope_df.iterrows()
        ],
        key="tag_select",
    )
    tel_color = normalize_tn_phone(tel_key2.split("—")[-1])
    hex_color = st.color_picker(
        "اللون", value=st.session_state.get("last_color", "#00AA88")
    )
    if st.button("🖌️ تلوين"):
        try:
            ws = get_spreadsheet().worksheet(employee)
            values = ws.get_all_values()
            header = values[0] if values else []
            tel_idx = header.index("Téléphone")
            row_idx = None
            for i, r in enumerate(values[1:], start=2):
                if len(r) > tel_idx and normalize_tn_phone(r[tel_idx]) == tel_color:
                    row_idx = i
                    break
            if not row_idx:
                st.error("❌ لم يتم إيجاد العميل.")
            else:
                st.session_state["last_color"] = hex_color
                color_col = EXPECTED_HEADERS.index("Tag") + 1
                ws.update_cell(row_idx, color_col, hex_color)
                st.success("✅ تم التلوين")
                st.cache_data.clear()
        except Exception as e:
            st.error(f"❌ خطأ: {e}")

    # ================== واتساب مع العميل ==================
    st.markdown("### 💬 تواصل WhatsApp مع العميل")
    try:
        scope_for_wa = (filtered_df if not filtered_df.empty else df_emp_raw).copy()
        wa_pick = st.selectbox(
            "اختر العميل لفتح واتساب",
            [
                f"{r['Nom & Prénom']} — {format_display_phone(normalize_tn_phone(r['Téléphone']))}"
                for _, r in scope_for_wa.iterrows()
            ],
            key="wa_pick",
        )
        default_msg = (
            "سلام! معاك Mega Formation. بخصوص التكوين، نحبّوا ننسّقو معاك موعد المتابعة. 👍"
        )
        wa_msg = st.text_area(
            "الرسالة (WhatsApp)", value=default_msg, key="wa_msg"
        )
        if st.button("📲 فتح واتساب"):
            raw_tel = wa_pick.split("—")[-1]
            tel_norm = normalize_tn_phone(raw_tel)
            url = f"https://wa.me/{tel_norm}?text={urllib.parse.quote(wa_msg)}"
            st.markdown(f"[افتح المحادثة الآن]({url})")
            st.info("اضغط على الرابط لفتح واتساب.")
    except Exception as e:
        st.warning(f"WhatsApp: {e}")

    # ================== تقرير يومي للإدارة ==================
    st.markdown("### 📤 تقرير يومي للإدارة (WhatsApp)")
    try:
        today = datetime.now().date()

        df_emp_daily = df_emp_raw.copy()
        df_emp_daily["DateAjout_dt"] = pd.to_datetime(
            df_emp_daily["Date ajout"], dayfirst=True, errors="coerce"
        )
        df_emp_daily["DateSuivi_dt"] = pd.to_datetime(
            df_emp_daily["Date de suivi"], dayfirst=True, errors="coerce"
        )

        # العملاء المضافين اليوم
        today_rows = df_emp_daily[
            df_emp_daily["DateAjout_dt"].dt.date == today
        ].copy()
        total_today = len(today_rows)

        inscrits_today = int(
            today_rows["Inscription"]
            .fillna("")
            .astype(str)
            .str.strip()
            .str.lower()
            .isin(["oui", "inscrit"])
            .sum()
        )

        alerts_today = int(
            today_rows.get("Alerte_view", today_rows.get("Alerte", ""))
            .fillna("")
            .astype(str)
            .str.strip()
            .ne("")
            .sum()
        )

        # العملاء اللي عندهم متابعة/تواصل اليوم
        contacts_today = df_emp_daily[
            df_emp_daily["DateSuivi_dt"].dt.date == today
        ].copy()

        # تفصيل حسب التكوين للعملاء المضافين اليوم
        if not today_rows.empty:
            by_form = (
                today_rows.groupby("Formation")["Nom & Prénom"]
                .count()
                .reset_index()
            )
        else:
            by_form = pd.DataFrame(columns=["Formation", "Nom & Prénom"])

        lines = [
            f"تقرير يومي لموظف: {employee}",
            f"التاريخ: {today.strftime('%d/%m/%Y')}",
            "",
            f"- عدد العملاء المضافين اليوم: {total_today}",
            f"- عدد المسجلين اليوم: {inscrits_today}",
            f"- عدد العملاء مع تنبيهات: {alerts_today}",
        ]

        if not by_form.empty:
            lines.append("")
            lines.append("تفصيل حسب التكوين:")
            for _, r in by_form.iterrows():
                lines.append(f"• {r['Formation']}: {int(r['Nom & Prénom'])} عميل")

        # العملاء المضافون اليوم + ملاحظاتهم (كاملة)
        if not today_rows.empty:
            lines.append("")
            lines.append("العملاء المضافون اليوم (مع الملاحظات):")
            for _, r in today_rows.iterrows():
                name = str(r.get("Nom & Prénom", "")).strip()
                phone = str(r.get("Téléphone", "")).strip()
                form = str(r.get("Formation", "")).strip()
                note = str(r.get("Remarque", "")).strip()

                line = f"- {name} ({form}) — {phone}"
                if note:
                    line += f"\n  ملاحظات: {note}"

                lines.append(line)

        # العملاء اللي صار معاهم تواصل اليوم (Date de suivi = اليوم) + ملاحظاتهم
        if not contacts_today.empty:
            lines.append("")
            lines.append("العملاء اللي صار معاهم تواصل اليوم (متابعة):")
            for _, r in contacts_today.iterrows():
                name = str(r.get("Nom & Prénom", "")).strip()
                phone = str(r.get("Téléphone", "")).strip()
                form = str(r.get("Formation", "")).strip()
                t_contact = str(r.get("Type de contact", "")).strip()
                note = str(r.get("Remarque", "")).strip()

                line = f"- {name} ({form}) — {phone}"
                if t_contact:
                    line += f" | نوع التواصل: {t_contact}"
                if note:
                    line += f"\n  ملاحظات: {note}"

                lines.append(line)

        report_text = "\n".join(lines)

        st.text_area(
            "معاينة التقرير الذي سيُرسل", value=report_text, height=260
        )

        if st.button("📲 إرسال تقرير اليوم إلى 22423590 عبر WhatsApp"):
            wa_admin_number = "21622423590"
            url = (
                f"https://wa.me/{wa_admin_number}?text="
                f"{urllib.parse.quote(report_text)}"
            )
            st.markdown(f"[اضغط هنا لإرسال التقرير عبر WhatsApp]({url})")
            st.info(
                "إضغط على الرابط، يتفتحلك WhatsApp Web / التطبيق وتبعث التقرير."
            )
    except Exception as e:
        st.warning(f"تعذر تجهيز التقرير اليومي: {e}")

    # ================== نقل عميل بين الموظفين ==================
    st.markdown("### 🔁 نقل عميل بين الموظفين")
    if all_employes:
        colRA, colRB = st.columns(2)
        src_emp = colRA.selectbox("من موظّف", all_employes, key="reassign_src")
        dst_emp = colRB.selectbox(
            "إلى موظّف",
            [e for e in all_employes if e != src_emp],
            key="reassign_dst",
        )
        df_src = df_all[df_all["__sheet_name"] == src_emp].copy()
        if df_src.empty:
            st.info("❕ لا يوجد عملاء عند هذا الموظّف.")
        else:
            pick = st.selectbox(
                "اختر العميل للنقل",
                [
                    f"{r['Nom & Prénom']} — {format_display_phone(r['Téléphone'])}"
                    for _, r in df_src.iterrows()
                ],
                key="reassign_pick",
            )
            phone_pick = normalize_tn_phone(pick.split("—")[-1])
            if st.button("🚚 نقل الآن"):
                try:
                    sh = get_spreadsheet()
                    ws_src = sh.worksheet(src_emp)
                    ws_dst = sh.worksheet(dst_emp)
                    values = ws_src.get_all_values()
                    header = values[0] if values else []
                    tel_idx = header.index("Téléphone")
                    row_idx = None
                    for i, r in enumerate(values[1:], start=2):
                        if len(r) > tel_idx and normalize_tn_phone(r[tel_idx]) == phone_pick:
                            row_idx = i
                            break
                    if not row_idx:
                        st.error("❌ لم يتم العثور على هذا العميل.")
                        st.stop()
                    row_values = ws_src.row_values(row_idx)
                    if len(row_values) < len(EXPECTED_HEADERS):
                        row_values += [""] * (
                            len(EXPECTED_HEADERS) - len(row_values)
                        )
                    row_values = row_values[: len(EXPECTED_HEADERS)]
                    row_values[EXPECTED_HEADERS.index("Employe")] = dst_emp
                    ws_dst.append_row(row_values)
                    ws_src.delete_rows(row_idx)
                    wslog = ensure_ws(REASSIGN_LOG_SHEET, REASSIGN_LOG_HEADERS)
                    wslog.append_row(
                        [
                            datetime.now(timezone.utc).isoformat(),
                            employee,
                            src_emp,
                            dst_emp,
                            row_values[0],
                            normalize_tn_phone(row_values[1]),
                        ]
                    )
                    st.success(
                        f"✅ نقل ({row_values[0]}) من {src_emp} إلى {dst_emp}"
                    )
                    st.cache_data.clear()
                except Exception as e:
                    st.error(f"❌ خطأ أثناء النقل: {e}")

# ============ تبويب الأرشيف ============
if tab_choice == "أرشيف" and role == "موظف" and employee:
    emp_lock_ui(employee, ns="archive")
    if not emp_unlocked(employee):
        st.info("🔒 أدخل كلمة سرّ الموظّف لفتح الأرشيف.")
        st.stop()

    st.subheader(f"🗂️ أرشيف — {employee}")
    ARCHIVE_SHEET = f"{employee}_Archive"
    ws_arch = ensure_ws(ARCHIVE_SHEET, EXPECTED_HEADERS)
    vals_arch = ws_arch.get_all_values()
    df_arch = (
        pd.DataFrame(vals_arch[1:], columns=vals_arch[0])
        if vals_arch and len(vals_arch) > 1
        else pd.DataFrame(columns=EXPECTED_HEADERS)
    )

    if df_arch.empty:
        st.info("لا يوجد عملاء في الأرشيف حالياً.")
    else:
        df_arch["Téléphone_norm"] = df_arch["Téléphone"].apply(normalize_tn_phone)
        df_arch["Alerte_view"] = df_arch.get("Alerte", "")
        st.dataframe(
            df_arch[[c for c in EXPECTED_HEADERS if c in df_arch.columns]]
            .style.apply(highlight_inscrit_row, axis=1)
            .applymap(mark_alert_cell, subset=["Alerte"]),
            use_container_width=True,
        )

    st.markdown("---")
    st.subheader("🔁 نقل/استرجاع")

    df_emp_all = df_all[df_all["__sheet_name"] == employee].copy()
    if df_emp_all.empty:
        st.caption("لا يوجد عملاء نشطين لنقلهم.")
    else:
        move_opt = st.selectbox(
            "اختر عميل للنقل إلى الأرشيف",
            [
                f"{r['Nom & Prénom']} — {format_display_phone(r['Téléphone'])}"
                for _, r in df_emp_all.iterrows()
            ],
        )
        if st.button("📦 نقل إلى الأرشيف"):
            try:
                sh = get_spreadsheet()
                ws_emp = sh.worksheet(employee)
                vals = ws_emp.get_all_values()
                header = vals[0] if vals else []
                tel_idx = (
                    header.index("Téléphonique")
                    if "Téléphonique" in header
                    else header.index("Téléphone")
                )
                phone_pick = normalize_tn_phone(move_opt.split("—")[-1])
                row_idx = None
                for i, r in enumerate(vals[1:], start=2):
                    if len(r) > tel_idx and normalize_tn_phone(r[tel_idx]) == phone_pick:
                        row_idx = i
                        break
                if not row_idx:
                    st.error("❌ لم يتم العثور على هذا العميل.")
                    st.stop()
                row_values = ws_emp.row_values(row_idx)
                if len(row_values) < len(EXPECTED_HEADERS):
                    row_values += [""] * (len(EXPECTED_HEADERS) - len(row_values))
                row_values = row_values[: len(EXPECTED_HEADERS)]
                ws_arch.append_row(row_values)
                ws_emp.delete_rows(row_idx)
                st.success("✅ تم النقل للأرشيف")
                st.cache_data.clear()
                st.rerun()
            except Exception as e:
                st.error(f"❌ خطأ: {e}")

    if df_arch.empty:
        st.caption("لا يوجد عملاء بالأرشيف للاسترجاع.")
    else:
        restore_opt = st.selectbox(
            "اختر عميل للاسترجاع",
            [
                f"{r['Nom & Prénom']} — {format_display_phone(r['Téléphone'])}"
                for _, r in df_arch.iterrows()
            ],
            key="restore_pick",
        )
        if st.button("♻️ استرجاع للورقة"):
            try:
                sh = get_spreadsheet()
                ws_emp = sh.worksheet(employee)
                valsA = ws_arch.get_all_values()
                headerA = valsA[0] if valsA else []
                tel_idxA = headerA.index("Téléphone")
                phone_pick = normalize_tn_phone(restore_opt.split("—")[-1])
                row_idx = None
                for i, r in enumerate(valsA[1:], start=2):
                    if len(r) > tel_idxA and normalize_tn_phone(r[tel_idxA]) == phone_pick:
                        row_idx = i
                        break
                if not row_idx:
                    st.error("❌ لم يتم العثور عليه في الأرشيف.")
                    st.stop()
                row_values = ws_arch.row_values(row_idx)
                if len(row_values) < len(EXPECTED_HEADERS):
                    row_values += [""] * (len(EXPECTED_HEADERS) - len(row_values))
                row_values = row_values[: len(EXPECTED_HEADERS)]
                ws_emp.append_row(row_values)
                ws_arch.delete_rows(row_idx)
                st.success("✅ تم الاسترجاع")
                st.cache_data.clear()
                st.rerun()
            except Exception as e:
                st.error(f"❌ خطأ: {e}")

# ============ صفحة الأدمِن ============
if role == "أدمن":
    st.markdown("## 👑 لوحة الأدمِن")
    if not admin_unlocked():
        st.info("🔐 أدخل كلمة سرّ الأدمِن من اليسار لفتح الصفحة.")
    else:
        colA, colB, colC = st.columns(3)

        # --- إضافة موظّف ---
        with colA:
            st.subheader("➕ إضافة موظّف")
            new_emp = st.text_input("اسم الموظّف الجديد")
            if st.button("إنشاء ورقة"):
                try:
                    sh = get_spreadsheet()
                    titles = [w.title for w in sh.worksheets()]
                    if not new_emp or new_emp in titles:
                        st.warning("⚠️ الاسم فارغ أو موجود.")
                    else:
                        sh.add_worksheet(title=new_emp, rows="1000", cols="20")
                        sh.worksheet(new_emp).update("1:1", [EXPECTED_HEADERS])
                        st.success("✔️ تم الإنشاء")
                        st.cache_data.clear()
                except Exception as e:
                    st.error(f"❌ خطأ: {e}")

        # --- إضافة عميل لأي موظف ---
        with colB:
            st.subheader("➕ إضافة عميل (لأي موظّف)")
            sh = get_spreadsheet()
            target_emp = st.selectbox(
                "اختر الموظّف", all_employes, key="admin_add_emp"
            )
            with st.form("admin_add_client_form"):
                nom_a = st.text_input("👤 الاسم و اللقب")
                tel_a = st.text_input("📞 الهاتف")
                date_naiss_a = st.date_input("🎂 تاريخ الميلاد")
                formation_a = st.text_input("📚 التكوين")
                type_contact_a = st.selectbox(
                    "نوع التواصل",
                    ["Visiteur", "Appel téléphonique", "WhatsApp", "Social media"],
                )
                inscription_a = st.selectbox(
                    "التسجيل", ["Pas encore", "Inscrit"]
                )
                date_ajout_a = st.date_input(
                    "تاريخ الإضافة", value=date.today()
                )
                suivi_date_a = st.date_input(
                    "تاريخ المتابعة", value=date.today()
                )
                remarque_a = st.text_area("🗒️ ملاحظة (اختياري)")
                sub_admin = st.form_submit_button("📥 أضف")

            if sub_admin:
                try:
                    if not (nom_a and tel_a and formation_a and target_emp):
                        st.error("❌ حقول ناقصة.")
                        st.stop()
                    tel_norm = normalize_tn_phone(tel_a)
                    if tel_norm in set(df_all["Téléphone_norm"]):
                        st.warning("⚠️ الرقم موجود.")
                    else:
                        insc_val = "Oui" if inscription_a == "Inscrit" else "Pas encore"
                        ws = sh.worksheet(target_emp)
                        ws.append_row(
                            [
                                nom_a,
                                tel_norm,
                                fmt_date(date_naiss_a),
                                type_contact_a,
                                formation_a,
                                remarque_a.strip(),
                                fmt_date(date_ajout_a),
                                fmt_date(suivi_date_a),
                                "",
                                insc_val,
                                target_emp,
                                "",
                            ]
                        )
                        st.success("✅ تمت الإضافة")
                        st.cache_data.clear()
                except Exception as e:
                    st.error(f"❌ خطأ: {e}")

        # --- حذف موظف ---
        with colC:
            st.subheader("🗑️ حذف موظّف")
            emp_to_delete = st.selectbox(
                "اختر الموظّف", all_employes, key="admin_del_emp"
            )
            if st.button("❗ حذف الورقة كاملة"):
                try:
                    sh = get_spreadsheet()
                    sh.del_worksheet(sh.worksheet(emp_to_delete))
                    st.success("تم الحذف")
                    st.cache_data.clear()
                except Exception as e:
                    st.error(f"❌ خطأ: {e}")

        st.markdown("---")
        st.subheader("📜 سجلّ نقل العملاء")
        wslog = ensure_ws(REASSIGN_LOG_SHEET, REASSIGN_LOG_HEADERS)
        vals = wslog.get_all_values()
        if vals and len(vals) > 1:
            df_log = pd.DataFrame(vals[1:], columns=vals[0])

            def _fmt_ts(x):
                try:
                    return (
                        datetime.fromisoformat(x)
                        .astimezone()
                        .strftime("%Y-%m-%d %H:%M")
                    )
                except Exception:
                    return x

            if "timestamp" in df_log.columns:
                df_log["وقت"] = df_log["timestamp"].apply(_fmt_ts)

            show_cols = [
                "وقت",
                "moved_by",
                "src_employee",
                "dst_employee",
                "client_name",
                "phone",
            ]
            show_cols = [c for c in show_cols if c in df_log.columns]
            st.dataframe(
                df_log[show_cols].sort_values(show_cols[0], ascending=False),
                use_container_width=True,
            )
        else:
            st.caption("لا يوجد سجلّ نقل.")
