# MegaCRM_Streamlit_App.py — نسخة مُحدّثة (Cloud + Local)

import json
import streamlit as st
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime, date
from PIL import Image

st.set_page_config(page_title="MegaCRM", layout="wide")

# ===== إعداد الاتصال بـ Google Sheets (Secrets أولاً، ثم ملف محلي كنسخة احتياطية) =====
SCOPE = ["https://www.googleapis.com/auth/spreadsheets"]

def make_client_and_sheet_id():
    # نحاول نقرأ من Secrets (صيغة TOML table أو JSON string)
    try:
        sa = st.secrets["gcp_service_account"]
        if hasattr(sa, "keys"):      # TOML table
            sa_info = dict(sa)
        elif isinstance(sa, str):    # JSON كنص
            sa_info = json.loads(sa)
        else:
            raise ValueError("صيغة gcp_service_account غير مدعومة")

        creds = Credentials.from_service_account_info(sa_info, scopes=SCOPE)
        client = gspread.authorize(creds)
        sheet_id = st.secrets["SPREADSHEET_ID"]
        return client, sheet_id
    except Exception:
        # تشغيل محلي: ملف JSON
        creds = Credentials.from_service_account_file("service_account.json", scopes=SCOPE)
        client = gspread.authorize(creds)
        # بدّل هذا لو حبيت ID آخر محليًا
        sheet_id = "1DV0KyDRYHofWR60zdx63a9BWBywTFhLavGAExPIa6LI"
        return client, sheet_id

client, SPREADSHEET_ID = make_client_and_sheet_id()

EXPECTED_HEADERS = [
    "Nom & Prénom","Téléphone","Type de contact","Formation",
    "Remarque","Date ajout","Date de suivi","Alerte",
    "Inscription","Employe","Tag"
]

# 🧠 تحميل كل أوراق الموظفين — نسخة آمنة (بدون get_all_records)
@st.cache_data(ttl=600)
def load_all_data():
    sh = client.open_by_key(SPREADSHEET_ID)
    worksheets = sh.worksheets()
    all_dfs, all_employes = [], []

    for ws in worksheets:
        all_employes.append(ws.title)

        # اقرأ القيم الخام
        rows = ws.get_all_values()  # list[list[str]]

        # لو الورقة فاضية: ثبّت الهيدر القياسي
        if not rows:
            ws.update("1:1", [EXPECTED_HEADERS])
            rows = ws.get_all_values()

        # ثبّت صفّ الهيدر لتفادي الدمج/النقص/التكرار
        try:
            ws.update("1:1", [EXPECTED_HEADERS])
            rows = ws.get_all_values()
        except Exception:
            # لو ما نجمش يحدّث (صلاحيات/حماية)، نكمّل بالقيم الموجودة
            pass

        # صفوف البيانات تحت الهيدر
        data_rows = rows[1:] if len(rows) > 1 else []

        # طوّل/قصّر كل صف لطول EXPECTED_HEADERS
        fixed_rows = []
        for r in data_rows:
            r = list(r) if r is not None else []
            if len(r) < len(EXPECTED_HEADERS):
                r = r + [""] * (len(EXPECTED_HEADERS) - len(r))
            else:
                r = r[:len(EXPECTED_HEADERS)]
            fixed_rows.append(r)

        # ابنِ DataFrame بأعمدة ثابتة
        df = pd.DataFrame(fixed_rows, columns=EXPECTED_HEADERS)
        df["__sheet_name"] = ws.title
        all_dfs.append(df)

    big = pd.concat(all_dfs, ignore_index=True) if all_dfs else pd.DataFrame(columns=EXPECTED_HEADERS + ["__sheet_name"])
    return big, all_employes

df_all, all_employes = load_all_data()

# 🎛️ اختيار الموظف أو المسؤول
# (تحذير قديم use_column_width → استعمل use_container_width)
try:
    st.sidebar.image(Image.open("logo.png"), use_container_width=True)
except Exception:
    pass

role = st.sidebar.selectbox("الدور", ["موظف", "أدمن"])
employee = st.sidebar.selectbox("اختر اسمك", all_employes) if role == "موظف" else None

# 📌 لوحة تحكم الأدمن: إضافة/حذف موظف
if role == "أدمن":
    st.subheader("📋 إدارة الموظفين")
    st.markdown("### ➕ إضافة موظف")
    new_emp = st.text_input("اسم الموظف الجديد")
    if st.button("إنشاء ورقة جديدة"):
        try:
            sh = client.open_by_key(SPREADSHEET_ID)
            if new_emp and new_emp not in [w.title for w in sh.worksheets()]:
                sh.add_worksheet(title=new_emp, rows="1000", cols="20")
                sh.worksheet(new_emp).update("1:1", [EXPECTED_HEADERS])
                st.success("✔️ تم إنشاء الموظف بنجاح")
                st.cache_data.clear()
            else:
                st.warning("⚠️ الاسم فارغ أو الموظف موجود مسبقًا")
        except Exception as e:
            st.error(f"❌ خطأ: {e}")

    st.markdown("### 🗑️ حذف موظف")
    emp_to_delete = st.selectbox("اختر موظفًا للحذف", all_employes)
    if st.button("❗ احذف هذا الموظف"):
        st.warning("⚠️ لا يمكن الحذف مباشرة عبر Streamlit لأسباب أمنية. احذف يدويًا من Google Sheets.")

# 📊 عرض بيانات الموظف
if role == "موظف" and employee:
    st.title(f"📁 لوحة {employee}")
    df_emp = df_all[df_all["__sheet_name"] == employee].copy()

    # تنبيه إن كانت الشيت فارغة
    if df_emp.empty:
        st.warning("⚠️ لا يوجد أي عملاء بعد. قاعدة البيانات فارغة.")
        st.markdown("### ➕ أضف أول عميل:")
    else:
        # 🔍 فلترة بالشهر (مع تأمين التاريخ)
        if "Date ajout" in df_emp.columns:
            df_emp["Date ajout"] = pd.to_datetime(df_emp["Date ajout"], dayfirst=True, errors="coerce")
        df_emp = df_emp.dropna(subset=["Date ajout"])
        df_emp["Mois"] = df_emp["Date ajout"].dt.strftime("%m-%Y") if "Date ajout" in df_emp.columns else ""
        month_filter = st.selectbox("🗓️ اختر شهر الإضافة", sorted(df_emp["Mois"].dropna().unique(), reverse=True))
        filtered_df = df_emp[df_emp["Mois"] == month_filter].copy()

        # تنبيه باللون إذا Alerte موجود
        def color_alerte(val):
            return 'background-color: red; color: white' if str(val).strip() != "" else ''

        # عرض الجدول مع تلوين Alerte
        if not filtered_df.empty:
            st.dataframe(filtered_df.drop(columns=["Mois", "__sheet_name"]).style.applymap(color_alerte, subset=["Alerte"]))
        else:
            st.info("لا توجد بيانات في هذا الشهر.")

        # 🟢 إضافة ملاحظة
        if not filtered_df.empty:
            st.markdown("### ✏️ أضف ملاحظة:")
            tel_to_update = st.selectbox("اختر رقم الهاتف", filtered_df["Téléphone"])
            new_note = st.text_area("🗒️ ملاحظة جديدة")
            if st.button("💾 حفظ الملاحظة"):
                if new_note.strip() == "":
                    st.warning("⚠️ الملاحظة فارغة!")
                else:
                    try:
                        ws = client.open_by_key(SPREADSHEET_ID).worksheet(employee)
                        rows = ws.get_all_values()
                        header = rows[0] if rows else EXPECTED_HEADERS
                        tel_col = header.index("Téléphone") + 1
                        rem_col = header.index("Remarque") + 1
                        now = datetime.now().strftime("%d/%m/%Y %H:%M")
                        for i, row in enumerate(rows[1:], start=2):
                            if len(row) >= tel_col and row[tel_col - 1] == tel_to_update:
                                current = row[rem_col - 1] if len(row) >= rem_col else ""
                                new_val = f"{current}\n[{now}]: {new_note}" if current else f"[{now}]: {new_note}"
                                ws.update_cell(i, rem_col, new_val)
                                st.success("✅ تمت إضافة الملاحظة")
                                st.cache_data.clear()
                                break
                    except Exception as e:
                        st.error(f"❌ خطأ أثناء حفظ الملاحظة: {e}")

        # ✅ Alerte تلقائي
        try:
            today = datetime.now().strftime("%d/%m/%Y")
            ws = client.open_by_key(SPREADSHEET_ID).worksheet(employee)
            rows = ws.get_all_values()
            header = rows[0] if rows else EXPECTED_HEADERS
            date_suivi_col = header.index("Date de suivi") + 1
            alerte_col = header.index("Alerte") + 1
            for i, row in enumerate(rows[1:], start=2):
                if len(row) >= date_suivi_col and row[date_suivi_col - 1].strip() == today:
                    ws.update_cell(i, alerte_col, "⏰ متابعة اليوم")
        except Exception:
            pass

        # 🔍 فلترة بالـ Alerte
        if not filtered_df.empty and st.checkbox("🔴 عرض العملاء الذين لديهم تنبيهات"):
            df_alerts = filtered_df[filtered_df["Alerte"].fillna("").astype(str).str.strip() != ""]
            if not df_alerts.empty:
                st.dataframe(df_alerts.drop(columns=["Mois", "__sheet_name"]).style.applymap(color_alerte, subset=["Alerte"]))
            else:
                st.info("لا توجد تنبيهات في هذا الفلتر.")

        # 🎨 تلوين الصفوف حسب العميل
        if not filtered_df.empty:
            st.markdown("### 🎨 اختر لون لتمييز العميل:")
            tel_color = st.selectbox("رقم الهاتف", filtered_df["Téléphone"])
            hex_color = st.color_picker("اختر اللون")
            if st.button("🖌️ تلوين"):
                try:
                    header = rows[0] if rows else EXPECTED_HEADERS
                    color_cell = header.index("Tag") + 1
                    tel_col = header.index("Téléphone") + 1
                    for i, row in enumerate(rows[1:], start=2):
                        if len(row) >= tel_col and row[tel_col - 1] == tel_color:
                            ws.update_cell(i, color_cell, hex_color)
                            st.success("✅ تم التلوين")
                            st.cache_data.clear()
                            break
                except Exception as e:
                    st.error(f"❌ خطأ أثناء الحفظ: {e}")

        # ➕ إضافة عميل (تظهر دائمًا)
        st.markdown("### ➕ أضف عميل جديد")
        nom = st.text_input("الاسم و اللقب")
        tel = st.text_input("رقم الهاتف")
        type_contact = st.selectbox("نوع الاتصال", ["Visiteur", "Appel téléphonique", "WhatsApp", "Social media"])
        formation = st.text_input("التكوين")
        if st.button("➕ أضف"):
            if not nom or not tel:
                st.warning("⚠️ الاسم أو الهاتف مفقود")
            else:
                try:
                    ws = client.open_by_key(SPREADSHEET_ID).worksheet(employee)
                    values = ws.get_all_values()
                    # فحص تكرار الهاتف
                    tel_idx = EXPECTED_HEADERS.index("Téléphone")
                    existing = {r[tel_idx].strip() for r in values[1:] if len(r) > tel_idx and r[tel_idx].strip()}
                    if tel in existing:
                        st.warning("⚠️ الرقم موجود مسبقًا")
                    else:
                        date_ajout = datetime.now().strftime("%d/%m/%Y")
                        ws.append_row([nom, tel, type_contact, formation, "", date_ajout, "", "", "", employee, ""])
                        st.success("✅ تم إضافة العميل")
                        st.cache_data.clear()
                except Exception as e:
                    st.error(f"❌ خطأ أثناء الإضافة: {e}")
