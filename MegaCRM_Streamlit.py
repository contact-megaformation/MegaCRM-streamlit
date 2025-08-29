# MegaCRM_Streamlit_App.py — Cloud + Local + Dashboard + Alerts Fix + Editable Dates/Inscription + WhatsApp

import json
import streamlit as st
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime, date
from PIL import Image

st.set_page_config(page_title="MegaCRM", layout="wide")

# ===== Google Sheets Auth (Secrets أولاً ثم ملف محلي) =====
SCOPE = ["https://www.googleapis.com/auth/spreadsheets"]

def make_client_and_sheet_id():
    try:
        sa = st.secrets["gcp_service_account"]
        if hasattr(sa, "keys"):
            sa_info = dict(sa)
        elif isinstance(sa, str):
            sa_info = json.loads(sa)
        else:
            raise ValueError("Bad gcp_service_account format")
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

EXPECTED_HEADERS = [
    "Nom & Prénom","Téléphone","Type de contact","Formation",
    "Remarque","Date ajout","Date de suivi","Alerte",
    "Inscription","Employe","Tag"
]

# ===== Helpers =====
def fmt_date(d: date | None) -> str:
    return d.strftime("%d/%m/%Y") if isinstance(d, date) else ""

def normalize_tn_phone(s: str) -> str:
    """Keep digits only. If 8-digit Tunisian local, prefix 216. If already starts with 216, keep."""
    digits = "".join(ch for ch in str(s) if ch.isdigit())
    if digits.startswith("216"):
        return digits
    if len(digits) == 8:
        return "216" + digits
    return digits

def format_display_phone(s: str) -> str:
    d = "".join(ch for ch in str(s) if ch.isdigit())
    return f"+{d}" if d else ""

def find_row_by_phone(ws, phone_digits: str) -> int | None:
    """Find row index (1-based) by normalized phone."""
    values = ws.get_all_values()
    if not values:
        return None
    header = values[0]
    if "Téléphone" not in header:
        return None
    tel_idx = header.index("Téléphone")
    for i, r in enumerate(values[1:], start=2):
        if len(r) > tel_idx and normalize_tn_phone(r[tel_idx]) == phone_digits:
            return i
    return None

# ===== تحميل كل أوراق الموظفين (آمن) =====
@st.cache_data(ttl=600)
def load_all_data():
    sh = client.open_by_key(SPREADSHEET_ID)
    worksheets = sh.worksheets()
    all_dfs, all_employes = [], []

    for ws in worksheets:
        all_employes.append(ws.title)

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
            r = list(r) if r is not None else []
            if len(r) < len(EXPECTED_HEADERS):
                r = r + [""] * (len(EXPECTED_HEADERS) - len(r))
            else:
                r = r[:len(EXPECTED_HEADERS)]
            fixed_rows.append(r)

        df = pd.DataFrame(fixed_rows, columns=EXPECTED_HEADERS)
        df["__sheet_name"] = ws.title
        all_dfs.append(df)

    big = pd.concat(all_dfs, ignore_index=True) if all_dfs else pd.DataFrame(columns=EXPECTED_HEADERS + ["__sheet_name"])
    return big, all_employes

df_all, all_employes = load_all_data()

# ===== أعمدة مشتقّة (تنبيهات/تواريخ/تليفون) =====
if not df_all.empty:
    df_all["DateAjout_dt"] = pd.to_datetime(df_all["Date ajout"], dayfirst=True, errors="coerce")
    df_all["DateSuivi_dt"] = pd.to_datetime(df_all["Date de suivi"], dayfirst=True, errors="coerce")
    df_all["Mois"] = df_all["DateAjout_dt"].dt.strftime("%m-%Y")
    today = datetime.now().date()
    # Alerte_view = Alerte إذا موجود، وإلا لو تاريخ المتابعة اليوم → "⏰ متابعة اليوم"
    base_alert = df_all["Alerte"].fillna("").astype(str).str.strip()
    due_today = df_all["DateSuivi_dt"].dt.date.eq(today).fillna(False)
    df_all["Alerte_view"] = base_alert
    df_all.loc[base_alert.eq("") & due_today, "Alerte_view"] = "⏰ متابعة اليوم"
    # تليفون بصيغة رقمية دولية لواتساب
    df_all["Téléphone_norm"] = df_all["Téléphone"].apply(normalize_tn_phone)
else:
    df_all["Alerte_view"] = ""
    df_all["Mois"] = ""
    df_all["Téléphone_norm"] = ""

# ===== الشعار =====
try:
    st.sidebar.image(Image.open("logo.png"), use_container_width=True)
except Exception:
    pass

# ===== اختيار الدور =====
role = st.sidebar.selectbox("الدور", ["موظف", "أدمن"])
employee = st.sidebar.selectbox("اختر اسمك", all_employes) if role == "موظف" else None

# ================== Dashboard ==================
st.title("📊 MegaCRM - إدارة العملاء")

df_dash = df_all.copy()
total_clients = len(df_dash)
alerts_today = int(df_dash["Alerte_view"].fillna("").astype(str).str.strip().ne("").sum()) if not df_dash.empty else 0
reg_col = df_dash["Inscription"].fillna("").astype(str).str.strip().str.lower() if not df_dash.empty else pd.Series([], dtype=str)
registered = int((reg_col == "oui").sum()) if not df_dash.empty else 0
rate = round((registered / total_clients) * 100, 2) if total_clients > 0 else 0.0

c1, c2, c3 = st.columns(3)
with c1:
    st.metric("👥 إجمالي العملاء", f"{total_clients}")
with c2:
    st.metric("🚨 عملاء لديهم تنبيهات", f"{alerts_today}")
with c3:
    st.metric("✅ نسبة التسجيل", f"{rate}%")

if not df_dash.empty:
    grp = df_dash.groupby("__sheet_name").agg(
        Clients=("Nom & Prénom", "count"),
        Inscrits=("Inscription", lambda x: (x.astype(str).str.strip().str.lower() == "oui").sum())
    )
    grp["% تسجيل"] = (grp["Inscrits"] / grp["Clients"]).replace([float("inf"), float("nan")], 0) * 100
    grp["% تسجيل"] = grp["% تسجيل"].round(2)
    st.subheader("📈 ملخص حسب الموظّف")
    st.dataframe(grp)

# ================== لوحة الأدمن ==================
if role == "أدمن":
    st.subheader("👨‍💼 إدارة الموظفين")

    # ➕ إضافة موظف
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

    # ➕ إضافة عميل جديد لأي موظّف (من الأدمن) مع تواريخ وتسجيل
    st.markdown("### ➕ إضافة عميل جديد (من الأدمن)")
    with st.form("admin_add_client_form"):
        col1, col2 = st.columns(2)
        with col1:
            nom_a = st.text_input("👤 الاسم و اللقب", key="admin_nom")
            tel_a_raw = st.text_input("📞 رقم الهاتف", key="admin_tel")
            formation_a = st.text_input("📚 التكوين", key="admin_formation")
            inscription_a = st.selectbox("🟢 حالة التسجيل", ["Pas encore", "Inscrit"], key="admin_insc")
        with col2:
            type_contact_a = st.selectbox("📞 نوع التواصل", ["Visiteur", "Appel téléphonique", "WhatsApp", "Social media"], key="admin_type")
            date_ajout_a = st.date_input("🕓 تاريخ الإضافة", value=date.today(), key="admin_date_ajout")
            suivi_date_a = st.date_input("📆 تاريخ المتابعة", value=date.today(), key="admin_suivi")
            employee_choice = st.selectbox("👨‍💼 الموظف", all_employes, key="admin_emp")

        add_admin_client = st.form_submit_button("📥 أضف العميل")
        if add_admin_client:
            if not (nom_a and tel_a_raw and formation_a and employee_choice):
                st.error("❌ الرجاء ملء جميع الحقول الأساسية")
            else:
                try:
                    ws = client.open_by_key(SPREADSHEET_ID).worksheet(employee_choice)
                    values = ws.get_all_values()
                    tel_idx = EXPECTED_HEADERS.index("Téléphone")
                    existing = {normalize_tn_phone(r[tel_idx]) for r in values[1:] if len(r) > tel_idx and r[tel_idx]}
                    tel_a = normalize_tn_phone(tel_a_raw)
                    if tel_a in existing:
                        st.warning("⚠️ رقم الهاتف موجود مسبقًا")
                    else:
                        insc_val = "Oui" if inscription_a == "Inscrit" else "Pas encore"
                        ws.append_row([
                            nom_a, tel_a, type_contact_a, formation_a, "",
                            fmt_date(date_ajout_a), fmt_date(suivi_date_a), "", insc_val, employee_choice, ""
                        ])
                        st.success(f"✅ تم إضافة العميل ({nom_a}) إلى موظّف: {employee_choice}")
                        st.cache_data.clear()
                except Exception as e:
                    st.error(f"❌ خطأ أثناء الإضافة: {e}")

    # 🗑️ حذف موظف (تنبيه فقط)
    st.markdown("### 🗑️ حذف موظف")
    emp_to_delete = st.selectbox("اختر موظفًا للحذف", all_employes, key="delete_emp")
    if st.button("❗ احذف هذا الموظف"):
        st.warning("⚠️ لا يمكن الحذف مباشرة عبر Streamlit لأسباب أمنية. احذف يدويًا من Google Sheets.")

# ================== واجهة الموظّف ==================
if role == "موظف" and employee:
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

    # ===== عرض العملاء مع Alerte_view =====
    def color_alerte(val):
        return 'background-color: red; color: white' if str(val).strip() != "" else ''

    if not filtered_df.empty:
        # استبدل عمود Alerte بالقيمة المحسوبة للعرض
        filtered_df["Alerte"] = filtered_df["Alerte_view"]
        display_cols = [c for c in EXPECTED_HEADERS if c != "Alerte"] + ["Alerte"]
        display_cols = [c for c in display_cols if c in filtered_df.columns]
        st.dataframe(
            filtered_df[display_cols].drop(columns=["Mois"], errors="ignore")
            .style.applymap(color_alerte, subset=["Alerte"])
        )
    else:
        st.info("لا توجد بيانات في هذا الشهر.")

    # ===== فلترة عملاء لديهم تنبيهات (حسب Alerte_view) =====
    if not filtered_df.empty and st.checkbox("🔴 عرض العملاء الذين لديهم تنبيهات"):
        df_alerts = filtered_df[filtered_df["Alerte_view"].fillna("").astype(str).str.strip() != ""].copy()
        if not df_alerts.empty:
            df_alerts["Alerte"] = df_alerts["Alerte_view"]
            st.dataframe(
                df_alerts[[c for c in display_cols if c in df_alerts.columns]]
                .style.applymap(color_alerte, subset=["Alerte"])
            )
        else:
            st.info("لا توجد تنبيهات في هذا الفلتر.")

    # ===== ✏️ تعديل تاريخ الإضافة/المتابعة وحالة التسجيل =====
    if not df_emp.empty:
        st.markdown("### ✏️ تعديل بيانات عميل")
        # لائحة أرقام للانتقاء (مع عرض جميل)
        df_emp["Téléphone_norm"] = df_emp["Téléphone"].apply(normalize_tn_phone)
        phone_choices = {
            f"{row['Nom & Prénom']} — {format_display_phone(row['Téléphone_norm'])}": row["Téléphone_norm"]
            for _, row in df_emp.iterrows()
            if str(row["Téléphone"]).strip() != ""
        }
        chosen_key = st.selectbox("اختر العميل (بالاسم/الهاتف)", list(phone_choices.keys()))
        chosen_phone = phone_choices.get(chosen_key, "")

        # قيم حالية
        cur_row = df_emp[df_emp["Téléphone_norm"] == chosen_phone].iloc[0] if chosen_phone else None
        cur_ajout = pd.to_datetime(cur_row["Date ajout"], dayfirst=True, errors="coerce").date() if cur_row is not None else date.today()
        cur_suivi = pd.to_datetime(cur_row["Date de suivi"], dayfirst=True, errors="coerce").date() if cur_row is not None and pd.notna(cur_row["Date de suivi"]) and str(cur_row["Date de suivi"]).strip() else date.today()
        cur_insc = str(cur_row["Inscription"]).strip().lower() if cur_row is not None else ""

        colE1, colE2, colE3 = st.columns(3)
        with colE1:
            new_ajout = st.date_input("🕓 تاريخ الإضافة", value=cur_ajout, key="edit_ajout")
        with colE2:
            new_suivi = st.date_input("📆 تاريخ المتابعة", value=cur_suivi, key="edit_suivi")
        with colE3:
            new_insc = st.selectbox("🟢 التسجيل", ["Pas encore", "Inscrit"], index=(1 if cur_insc == "oui" else 0), key="edit_insc")

        if st.button("💾 حفظ التعديلات"):
            try:
                ws = client.open_by_key(SPREADSHEET_ID).worksheet(employee)
                row_idx = find_row_by_phone(ws, chosen_phone)
                if not row_idx:
                    st.error("❌ تعذّر إيجاد الصف لهذا الهاتف.")
                else:
                    # أعمدة
                    col_ajout = EXPECTED_HEADERS.index("Date ajout") + 1
                    col_suivi = EXPECTED_HEADERS.index("Date de suivi") + 1
                    col_insc = EXPECTED_HEADERS.index("Inscription") + 1
                    ws.update_cell(row_idx, col_ajout, fmt_date(new_ajout))
                    ws.update_cell(row_idx, col_suivi, fmt_date(new_suivi))
                    ws.update_cell(row_idx, col_insc, ("Oui" if new_insc == "Inscrit" else "Pas encore"))
                    st.success("✅ تم حفظ التعديلات")
                    st.cache_data.clear()
            except Exception as e:
                st.error(f"❌ خطأ أثناء التعديل: {e}")

    # ===== 📝 ملاحظات =====
    if not df_emp.empty:
        st.markdown("### 📝 أضف ملاحظة")
        # اختر هاتف من نفس الفلتر الشهري لو موجودين
        scope_df = filtered_df if not filtered_df.empty else df_emp
        scope_df["Téléphone_norm"] = scope_df["Téléphone"].apply(normalize_tn_phone)
        tel_to_update_key = st.selectbox(
            "اختر العميل",
            [f"{r['Nom & Prénom']} — {format_display_phone(normalize_tn_phone(r['Téléphone']))}" for _, r in scope_df.iterrows()]
        )
        # استخرج الهاتف المختار
        tel_to_update = normalize_tn_phone(tel_to_update_key.split("—")[-1])
        new_note = st.text_area("🗒️ ملاحظة جديدة")
        if st.button("📌 أضف الملاحظة"):
            if new_note.strip() == "":
                st.warning("⚠️ الملاحظة فارغة!")
            else:
                try:
                    ws = client.open_by_key(SPREADSHEET_ID).worksheet(employee)
                    row_idx = find_row_by_phone(ws, tel_to_update)
                    if not row_idx:
                        st.error("❌ لم يتم إيجاد العميل بالهاتف.")
                    else:
                        rem_col = EXPECTED_HEADERS.index("Remarque") + 1
                        old_remark = ws.cell(row_idx, rem_col).value or ""
                        stamp = datetime.now().strftime("%d/%m/%Y %H:%M")
                        updated = (old_remark + "\n" if old_remark else "") + f"[{stamp}] {new_note.strip()}"
                        ws.update_cell(row_idx, rem_col, updated)
                        st.success("✅ تمت إضافة الملاحظة")
                        st.cache_data.clear()
                except Exception as e:
                    st.error(f"❌ خطأ أثناء حفظ الملاحظة: {e}")

    # ===== 🎨 تلوين (Tag) =====
    if not df_emp.empty:
        st.markdown("### 🎨 اختر لون/Tag للعميل")
        scope_df = filtered_df if not filtered_df.empty else df_emp
        scope_df["Téléphone_norm"] = scope_df["Téléphone"].apply(normalize_tn_phone)
        tel_color_key = st.selectbox(
            "اختر العميل",
            [f"{r['Nom & Prénom']} — {format_display_phone(normalize_tn_phone(r['Téléphone']))}" for _, r in scope_df.iterrows()],
            key="tag_select"
        )
        tel_color = normalize_tn_phone(tel_color_key.split("—")[-1])
        hex_color = st.color_picker("اختر اللون")
        if st.button("🖌️ تلوين"):
            try:
                ws = client.open_by_key(SPREADSHEET_ID).worksheet(employee)
                row_idx = find_row_by_phone(ws, tel_color)
                if not row_idx:
                    st.error("❌ لم يتم إيجاد العميل.")
                else:
                    color_cell = EXPECTED_HEADERS.index("Tag") + 1
                    ws.update_cell(row_idx, color_cell, hex_color)
                    st.success("✅ تم التلوين")
                    st.cache_data.clear()
            except Exception as e:
                st.error(f"❌ خطأ أثناء الحفظ: {e}")

    # ===== ➕ إضافة عميل جديد (الموظف) مع تاريخي الإضافة/المتابعة والتسجيل =====
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

        submitted = st.form_submit_button("📥 أضف العميل")
        if submitted:
            if not (nom and tel_raw and formation):
                st.error("❌ الرجاء ملء جميع الحقول الأساسية")
            else:
                try:
                    ws = client.open_by_key(SPREADSHEET_ID).worksheet(employee)
                    values = ws.get_all_values()
                    tel_idx = EXPECTED_HEADERS.index("Téléphone")
                    existing = {normalize_tn_phone(r[tel_idx]) for r in values[1:] if len(r) > tel_idx and r[tel_idx]}
                    tel = normalize_tn_phone(tel_raw)
                    if tel in existing:
                        st.warning("⚠️ الرقم موجود مسبقًا")
                    else:
                        insc_val = "Oui" if inscription == "Inscrit" else "Pas encore"
                        ws.append_row([
                            nom, tel, type_contact, formation, "",
                            fmt_date(date_ajout_in), fmt_date(date_suivi_in), "", insc_val, employee, ""
                        ])
                        st.success("✅ تم إضافة العميل")
                        st.cache_data.clear()
                except Exception as e:
                    st.error(f"❌ خطأ أثناء الإضافة: {e}")

    # ===== WhatsApp زرّ مباشر =====
    st.markdown("### 📲 تواصل عبر واتساب")
    if not df_emp.empty:
        df_emp["Téléphone_norm"] = df_emp["Téléphone"].apply(normalize_tn_phone)
        choice = st.selectbox(
            "اختر العميل",
            [f"{r['Nom & Prénom']} — {format_display_phone(normalize_tn_phone(r['Téléphone']))}" for _, r in df_emp.iterrows()],
            key="wa_select"
        )
        tel_norm = normalize_tn_phone(choice.split("—")[-1])
        default_msg = f"Bonjour, c'est MegaFormation. On vous contacte pour le suivi de votre formation."
        msg = st.text_input("نص الرسالة", value=default_msg)
        if st.button("📤 إرسال واتساب"):
            from urllib.parse import quote
            wa_url = f"https://wa.me/{tel_norm}?text={quote(msg)}"
            st.link_button("فتح واتساب", wa_url)
hide_streamlit_style = """
    <style>
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    header {visibility: hidden;}
    .stAppDeployButton {display: none;}
    </style>
"""
st.markdown(hide_streamlit_style, unsafe_allow_html=True)

    </style>
"""
st.markdown(hide_streamlit_style, unsafe_allow_html=True)

