
# MegaCRM_Streamlit_App.py

import streamlit as st
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime, date
import urllib.parse
import webbrowser

# ========== إعداد الاتصال بـ Google Sheets ==========
SCOPE = ["https://www.googleapis.com/auth/spreadsheets"]
CREDS = Credentials.from_service_account_file("service_account.json", scopes=SCOPE)
client = gspread.authorize(CREDS)
SPREADSHEET_ID = "1DV0KyDRYHofWR60zdx63a9BWBywTFhLavGAExPIa6LI"

# ========== تحميل كل أوراق الموظفين ==========
@st.cache_data(ttl=60)
def load_all_data():
    sh = client.open_by_key(SPREADSHEET_ID)
    sheets = sh.worksheets()
    all_data = []
    all_employees = []
    for ws in sheets:
        all_employees.append(ws.title)
        data = ws.get_all_records()
        df = pd.DataFrame(data)
        if df.empty:
            df = pd.DataFrame(columns=["Nom & Prénom","Téléphone","Type de contact","Formation","Remarque","Date ajout","Date de suivi","Alerte","Inscription","Employe","Tag"])
        df["Employe"] = ws.title
        all_data.append(df)
    return pd.concat(all_data, ignore_index=True), all_employees

# ========== واجهة المستخدم ==========
st.set_page_config(layout="wide")
st.title("📊 MegaCRM - إدارة العملاء")

# ========== تحميل البيانات ==========
df, all_employees = load_all_data()

# ========== معالجة التاريخ وإعداد الأعمدة ==========
df["Date ajout"] = pd.to_datetime(df["Date ajout"], errors="coerce")
df["Mois"] = df["Date ajout"].dt.strftime("%m")
df["Alerte"] = df.apply(lambda row: "⛔ متأخر" if pd.to_datetime(row["Date de suivi"], errors='coerce').date() == date.today() else row["Alerte"], axis=1)

# ========== الفلاتر الجانبية ==========
st.sidebar.header("🎛️ فلترة")
selected_employe = st.sidebar.selectbox("👤 الموظف", options=["الكل"] + all_employees)
selected_month = st.sidebar.selectbox("📅 الشهر", options=["الكل"] + [f"{i:02d}" for i in range(1, 13)])
alert_only = st.sidebar.checkbox("🚨 عرض العملاء المتأخرين فقط")
search_term = st.sidebar.text_input("🔍 بحث (تكوين أو رقم الهاتف)")

# ========== تطبيق الفلاتر ==========
filtered_df = df.copy()
if selected_employe != "الكل":
    filtered_df = filtered_df[filtered_df["Employe"] == selected_employe]
if selected_month != "الكل":
    filtered_df = filtered_df[filtered_df["Mois"] == selected_month]
if alert_only:
    filtered_df = filtered_df[filtered_df["Alerte"].str.contains("متأخر", na=False)]
if search_term:
    filtered_df = filtered_df[
        filtered_df["Formation"].astype(str).str.contains(search_term, case=False) |
        filtered_df["Téléphone"].astype(str).str.contains(search_term)
    ]

# ========== عرض العملاء ==========
st.subheader("📋 قائمة العملاء")
if filtered_df.empty:
    st.info("⚠️ لا توجد نتائج حسب الفلتر الحالي.")
else:
    st.write(f"👥 عدد العملاء: {len(filtered_df)}")
    for i, row in filtered_df.iterrows():
        color = "#FFCCCC" if str(row["Alerte"]).strip() == "⛔ متأخر" else "#f9f9f9"
        with st.expander(f"{row['Nom & Prénom']} - {row['Téléphone']}", expanded=False):
            st.markdown(f"""
                <div style='background-color:{color}; padding:10px; border-radius:5px;'>
                - 📞 نوع التواصل: {row['Type de contact']}<br>
                - 📚 التكوين: {row['Formation']}<br>
                - 🗒️ الملاحظات:<br>{row['Remarque'].replace('\n','<br>') if pd.notna(row['Remarque']) else ''}<br>
                - 🕓 تاريخ الإضافة: {row['Date ajout']}<br>
                - 📆 المتابعة: {row['Date de suivi']}<br>
                - 🚨 التنبيه: <b>{row['Alerte']}</b><br>
                - ✅ التسجيل: {row['Inscription']}<br>
                - 🎨 التاغ: {row.get('Tag', '')}<br>
                </div>
            """, unsafe_allow_html=True)

            # ملاحظة
            new_note = st.text_area("📝 ملاحظة جديدة", key=f"note_{i}")
            if st.button("📌 أضف الملاحظة", key=f"add_note_{i}"):
                if not new_note.strip():
                    st.error("❌ الملاحظة فارغة")
                else:
                    try:
                        ws = client.open_by_key(SPREADSHEET_ID).worksheet(row["Employe"])
                        cell = ws.find(str(row["Téléphone"]))
                        old_remark = ws.cell(cell.row, 5).value or ""
                        stamp = datetime.now().strftime("%Y-%m-%d %H:%M")
                        updated = (old_remark + "\n" if old_remark else "") + f"[{stamp}] {new_note.strip()}"
                        ws.update_cell(cell.row, 5, updated)
                        st.success("✅ تمت إضافة الملاحظة")
                    except:
                        st.error("❌ خطأ أثناء حفظ الملاحظة")

            # تلوين العميل
            tag_val = st.selectbox("🎨 التاغ", ["","Follow-up","Won","Ignored","Custom"], key=f"tag_{i}")
            if st.button("🎯 حفظ التاغ", key=f"save_tag_{i}"):
                try:
                    ws = client.open_by_key(SPREADSHEET_ID).worksheet(row["Employe"])
                    cell = ws.find(str(row["Téléphone"]))
                    ws.update_cell(cell.row, 11, tag_val)
                    ws.update_cell(cell.row, 9, tag_val)  # نسخ إلى Inscription
                    st.success("✅ تم حفظ التاغ")
                except:
                    st.error("❌ خطأ أثناء حفظ التاغ")

            # واتساب
            if st.button("📲 إرسال تنبيه واتساب", key=f"whatsapp_{i}"):
                msg = urllib.parse.quote(f"Bonjour {row['Nom & Prénom']}, c'est MegaFormation. On vous contacte pour le suivi de votre formation.")
                whatsapp_url = f"https://wa.me/{row['Téléphone']}?text={msg}"
                webbrowser.open_new_tab(whatsapp_url)

# ========== Dashboard للإداري ==========
st.subheader("📊 لوحة التحكم الإدارية")
stats = df.groupby("Employe").agg({
    "Nom & Prénom": "count",
    "Inscription": lambda x: (x == "Oui").sum()
}).rename(columns={"Nom & Prénom": "Clients", "Inscription": "Inscrits"})
stats["% تسجيل"] = round(stats["Inscrits"] / stats["Clients"] * 100, 2)
st.dataframe(stats)

# ========== إضافة موظف جديد (ورقة جديدة) ==========
st.subheader("👨‍💼 إدارة الموظفين")
with st.form("add_employee_form"):
    new_emp = st.text_input("👤 اسم الموظف الجديد")
    add_emp = st.form_submit_button("➕ أضف الموظف")
    if add_emp and new_emp:
        try:
            sh = client.open_by_key(SPREADSHEET_ID)
            if new_emp not in [ws.title for ws in sh.worksheets()]:
                sh.add_worksheet(title=new_emp, rows=100, cols=12)
                ws = sh.worksheet(new_emp)
                headers = ["Nom & Prénom","Téléphone","Type de contact","Formation","Remarque","Date ajout","Date de suivi","Alerte","Inscription","Employe","Tag"]
                ws.append_row(headers)
                st.success("✅ تمت إضافة الموظف")
            else:
                st.warning("⚠️ الموظف موجود مسبقًا")
        except:
            st.error("❌ خطأ أثناء إنشاء ورقة الموظف")

# ========== إضافة عميل جديد من قبل الإداري ==========
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
        if not (nom and tel and formation):
            st.error("❌ الرجاء ملء جميع الحقول الأساسية")
        else:
            ws = client.open_by_key(SPREADSHEET_ID).worksheet(employee_choice)
            phones = [str(r[1]) for r in ws.get_all_values()[1:] if r]
            if tel in phones:
                st.error("❌ رقم الهاتف موجود مسبقًا")
            else:
                ws.append_row([nom, tel, contact_type, formation, "", date.today().strftime("%Y-%m-%d"), str(suivi_date), "", "", employee_choice, ""])
                st.success("✅ تم إضافة العميل")
git init
git branch -M main
git add .
git commit -m "Initial MegaCRM Streamlit app"
git remote add origin https://github.com/contact-megaformation/MegaCRM.git
git push -u origin main

