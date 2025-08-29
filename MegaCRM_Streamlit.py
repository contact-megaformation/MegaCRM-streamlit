# MegaCRM_Streamlit_App.py — نسخة جاهزة للويب (Streamlit Cloud)

import json
import streamlit as st
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime, date
import urllib.parse

# -------- إعداد صفحة ستريمليت --------
st.set_page_config(page_title="MegaCRM", layout="wide")

# -------- Google Sheets Auth عبر Secrets (ويب) أو ملف محلي (ديف) --------
SCOPE = ["https://www.googleapis.com/auth/spreadsheets"]

def make_client():
    # على الكلاود: نقرأ من Secrets
    if "gcp_service_account" in st.secrets:
        sa = st.secrets["gcp_service_account"]
        if isinstance(sa, str):
            sa = json.loads(sa)
        creds = Credentials.from_service_account_info(sa, scopes=SCOPE)
        client = gspread.authorize(creds)
        sheet_id = st.secrets["SPREADSHEET_ID"]
        return client, sheet_id
    # محليًا (تطوير): fallback للملف
    creds = Credentials.from_service_account_file("service_account.json", scopes=SCOPE)
    client = gspread.authorize(creds)
    sheet_id = st.secrets.get("SPREADSHEET_ID", "1DV0KyDRYHofWR60zdx63a9BWBywTFhLavGAExPIa6LI")
    return client, sheet_id

client, SPREADSHEET_ID = make_client()

EXPECTED_HEADERS = [
    "Nom & Prénom","Téléphone","Type de contact","Formation",
    "Remarque","Date ajout","Date de suivi","Alerte",
    "Inscription","Employe","Tag"
]

# -------- تحميل كل أوراق الموظفين (مع فرض هيدر صحيح) --------
@st.cache_data(ttl=120)
def load_all_data(spreadsheet_id: str):
    sh = client.open_by_key(spreadsheet_id)
    sheets = sh.worksheets()
    all_data, all_employees = [], []
    for ws in sheets:
        all_employees.append(ws.title)
        rows = ws.get_all_values()
        # لو الورقة فارغة تمامًا، نجهّز الهيدر
        if not rows:
            ws.append_row(EXPECTED_HEADERS)
            rows = ws.get_all_values()
        header = rows[0]
        # لو الهيدر ناقص/فيه فراغات → نفرض القياسي
        if (len(header) < len(EXPECTED_HEADERS)) or any((h is None) or (str(h).strip() == "") for h in header):
            ws.update("1:1", [EXPECTED_HEADERS])

        recs = ws.get_all_records(expected_headers=EXPECTED_HEADERS)
        df = pd.DataFrame(recs) if recs else pd.DataFrame(columns=EXPECTED_HEADERS)
        df["Employe"] = ws.title
        all_data.append(df)

    big = pd.concat(all_data, ignore_index=True) if all_data else pd.DataFrame(columns=EXPECTED_HEADERS)
    return big, all_employees

# -------- واجهة المستخدم --------
st.title("📊 MegaCRM - إدارة العملاء")

df, all_employees = load_all_data(SPREADSHEET_ID)

# -------- معالجة التاريخ والتنبيه بأمان --------
if "Date ajout" in df.columns:
    df["Date ajout"] = pd.to_datetime(df["Date ajout"], dayfirst=True, errors="coerce")
else:
    df["Date ajout"] = pd.NaT
df["Mois"] = df["Date ajout"].dt.strftime("%m")

def compute_alerte(row):
    current = str(row.get("Alerte", "") or "").strip()
    d = pd.to_datetime(row.get("Date de suivi"), dayfirst=True, errors="coerce")
    if pd.notna(d) and d.date() == date.today():
        return "⏰ متابعة اليوم"
    return current

df["Alerte"] = df.apply(compute_alerte, axis=1)

# -------- الفلاتر --------
st.sidebar.header("🎛️ فلترة")
selected_employe = st.sidebar.selectbox("👤 الموظف", options=["الكل"] + all_employees)
selected_month = st.sidebar.selectbox("📅 الشهر", options=["الكل"] + [f"{i:02d}" for i in range(1, 13)])
alert_only = st.sidebar.checkbox("🚨 عرض العملاء الذين لديهم تنبيه فقط")
search_term = st.sidebar.text_input("🔍 بحث (تكوين أو رقم الهاتف)")

filtered_df = df.copy()
if selected_employe != "الكل":
    filtered_df = filtered_df[filtered_df["Employe"] == selected_employe]
if selected_month != "الكل":
    filtered_df = filtered_df[filtered_df["Mois"] == selected_month]
if alert_only:
    filtered_df = filtered_df[filtered_df["Alerte"].fillna("").astype(str).str.strip() != ""]
if search_term:
    q = search_term.strip()
    filtered_df = filtered_df[
        filtered_df["Formation"].fillna("").astype(str).str.contains(q, case=False) |
        filtered_df["Téléphone"].fillna("").astype(str).str.contains(q)
    ]

# -------- عرض العملاء --------
st.subheader("📋 قائمة العملاء")
if filtered_df.empty:
    st.info("⚠️ لا توجد نتائج حسب الفلتر الحالي.")
else:
    st.write(f"👥 عدد العملاء: {len(filtered_df)}")
    for i, row in filtered_df.reset_index(drop=True).iterrows():
        alerte_txt = str(row.get("Alerte", "") or "").strip()
        color = "#FFCCCC" if alerte_txt else "#f9f9f9"

        # صياغة تاريخ الإضافة للعرض
        raw_added = row.get("Date ajout")
        date_ajout_str = "—"
        if isinstance(raw_added, pd.Timestamp) and pd.notna(raw_added):
            date_ajout_str = raw_added.strftime("%d/%m/%Y")
        elif isinstance(raw_added, str) and raw_added.strip():
            tmp = pd.to_datetime(raw_added, dayfirst=True, errors="coerce")
            if pd.notna(tmp):
                date_ajout_str = tmp.strftime("%d/%m/%Y")

        with st.expander(f"{row.get('Nom & Prénom','')} - {row.get('Téléphone','')}", expanded=False):
            remarque_html = ""
            if pd.notna(row.get("Remarque")):
                remarque_html = str(row["Remarque"]).replace("\n", "<br>")

            st.markdown(f"""
                <div style='background-color:{color}; padding:10px; border-radius:8px; line-height:1.7'>
                - 📞 نوع التواصل: {row.get('Type de contact','')}<br>
                - 📚 التكوين: {row.get('Formation','')}<br>
                - 🗒️ الملاحظات:<br>{remarque_html}<br>
                - 🕓 تاريخ الإضافة: {date_ajout_str}<br>
                - 📆 المتابعة: {row.get('Date de suivi','')}<br>
                - 🚨 التنبيه: <b>{alerte_txt}</b><br>
                - ✅ التسجيل: {row.get('Inscription','')}<br>
                - 🎨 التاغ: {row.get('Tag','')}<br>
                </div>
            """, unsafe_allow_html=True)

            # ملاحظة جديدة
            new_note = st.text_area("📝 ملاحظة جديدة", key=f"note_{i}")
            if st.button("📌 أضف الملاحظة", key=f"add_note_{i}"):
                if not new_note.strip():
                    st.error("❌ الملاحظة فارغة")
                else:
                    try:
                        ws = client.open_by_key(SPREADSHEET_ID).worksheet(row["Employe"])
                        cell = ws.find(str(row["Téléphone"]))
                        old_remark = ws.cell(cell.row, 5).value or ""
                        stamp = datetime.now().strftime("%d/%m/%Y %H:%M")
                        updated = (old_remark + "\n" if old_remark else "") + f"[{stamp}] {new_note.strip()}"
                        ws.update_cell(cell.row, 5, updated)
                        st.success("✅ تمت إضافة الملاحظة")
                        st.cache_data.clear()
                    except Exception as e:
                        st.error(f"❌ خطأ أثناء حفظ الملاحظة: {e}")

            # إصلاح تاريخ الإضافة لو ناقص
            if date_ajout_str == "—":
                if st.button("🛠️ تثبيت تاريخ الإضافة (اليوم)", key=f"fix_date_{i}"):
                    try:
                        ws = client.open_by_key(SPREADSHEET_ID).worksheet(row["Employe"])
                        phone_cell = ws.find(str(row["Téléphone"]))
                        ws.update_cell(phone_cell.row, 6, datetime.now().strftime("%d/%m/%Y"))
                        st.success("✅ تم تثبيت التاريخ")
                        st.cache_data.clear()
                    except Exception as e:
                        st.error(f"❌ تعذّر تثبيت التاريخ: {e}")

            # التاغ
            tag_val = st.selectbox("🎨 التاغ", ["","Follow-up","Won","Ignored","Custom"], key=f"tag_{i}")
            if st.button("🎯 حفظ التاغ", key=f"save_tag_{i}"):
                try:
                    ws = client.open_by_key(SPREADSHEET_ID).worksheet(row["Employe"])
                    cell = ws.find(str(row["Téléphone"]))
                    ws.update_cell(cell.row, 11, tag_val)  # Tag
                    ws.update_cell(cell.row, 9, tag_val)   # Inscription copy
                    st.success("✅ تم حفظ التاغ")
                    st.cache_data.clear()
                except Exception as e:
                    st.error(f"❌ خطأ أثناء حفظ التاغ: {e}")

            # رابط واتساب (أحسن من webbrowser على الكلاود)
            if st.button("📲 إنشاء رابط واتساب", key=f"whatsapp_{i}"):
                msg = urllib.parse.quote(
                    f"Bonjour {row.get('Nom & Prénom','')}, c'est MegaFormation. "
                    f"Nous vous contactons pour le suivi de votre formation."
                )
                whatsapp_url = f"https://wa.me/{row.get('Téléphone','')}?text={msg}"
                st.link_button("فتح الدردشة في واتساب", whatsapp_url)

# -------- Dashboard للإداري --------
st.subheader("📊 لوحة التحكم الإدارية")
if not df.empty:
    stats = df.groupby("Employe", dropna=False).agg({
        "Nom & Prénom": "count",
        "Inscription": lambda x: (x.fillna("").astype(str).str.lower().isin(["oui","yes","1","true"]).sum())
    }).rename(columns={"Nom & Prénom": "Clients", "Inscription": "Inscrits"})
    stats["% تسجيل"] = (stats["Inscrits"] / stats["Clients"]).replace([pd.NA, pd.NaT, float("inf")], 0).fillna(0) * 100
    stats["% تسجيل"] = stats["% تسجيل"].round(2)
    st.dataframe(stats, use_container_width=True)
else:
    st.info("لا توجد بيانات بعد لعرض الإحصائيات.")

# -------- إضافة موظف جديد --------
st.subheader("👨‍💼 إدارة الموظفين")
with st.form("add_employee_form"):
    new_emp = st.text_input("👤 اسم الموظف الجديد")
    add_emp = st.form_submit_button("➕ أضف الموظف")
    if add_emp:
        if not new_emp.strip():
            st.warning("⚠️ أدخل اسمًا صالحًا")
        else:
            try:
                sh = client.open_by_key(SPREADSHEET_ID)
                if new_emp not in [ws.title for ws in sh.worksheets()]:
                    sh.add_worksheet(title=new_emp, rows=200, cols=12)
                    ws = sh.worksheet(new_emp)
                    ws.update("1:1", [EXPECTED_HEADERS])
                    st.success("✅ تمت إضافة الموظف")
                    st.cache_data.clear()
                else:
                    st.warning("⚠️ الموظف موجود مسبقًا")
            except Exception as e:
                st.error(f"❌ خطأ أثناء إنشاء ورقة الموظف: {e}")

# -------- إضافة عميل جديد --------
st.subheader("➕ إضافة عميل جديد")
with st.form("add_client_form"):
    col1, col2 = st.columns(2)
    with col1:
        nom = st.text_input("👤 الاسم الكامل")
        tel = st.text_input("📞 الهاتف")
        formation = st.text_input("📚 التكوين")
    with col2:
        contact_type = st.selectbox("📞 نوع التواصل", ["Visiteur","WhatsApp","Appel téléphonique","Social media"])
        suivi_date = st.date_input("📆 تاريخ المتابعة", value=date.today())
        employee_choice = st.selectbox("👨‍💼 الموظف", all_employees)

    submitted = st.form_submit_button("📥 أضف العميل")
    if submitted:
        if not (nom.strip() and tel.strip() and formation.strip()):
            st.error("❌ الرجاء ملء جميع الحقول الأساسية")
        else:
            try:
                ws = client.open_by_key(SPREADSHEET_ID).worksheet(employee_choice)
                values = ws.get_all_values()
                phones = set(str(r[1]).strip() for r in values[1:] if len(r) > 1 and str(r[1]).strip())
                if tel.strip() in phones:
                    st.error("❌ رقم الهاتف موجود مسبقًا")
                else:
                    today_str = date.today().strftime("%d/%m/%Y")
                    suivi_str  = suivi_date.strftime("%d/%m/%Y")
                    ws.append_row([nom.strip(), tel.strip(), contact_type, formation.strip(),
                                   "", today_str, suivi_str, "", "", employee_choice, ""])
                    st.success("✅ تم إضافة العميل")
                    st.cache_data.clear()
            except Exception as e:
                st.error(f"❌ خطأ أثناء إضافة العميل: {e}")
