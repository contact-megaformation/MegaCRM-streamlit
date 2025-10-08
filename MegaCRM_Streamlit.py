# MegaCRM_Streamlit.py
# CRM + Revenus/Dépenses (MB/Bizerte) + Previous payments per client + Reassign log
# آخر تحديث: fix cache refresh, safer sheet reading, per-client history (by phone/name), admin-only monthly summary

import json, time, base64, urllib.parse
import streamlit as st
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime, date, timedelta, timezone
from PIL import Image

# ------------ Page
st.set_page_config(page_title="MegaCRM", layout="wide", initial_sidebar_state="expanded")
st.markdown("<h1 style='text-align:center;'>📊 CRM MEGA FORMATION - إدارة العملاء</h1><hr>", unsafe_allow_html=True)

# ------------ Auth
SCOPE = ["https://www.googleapis.com/auth/spreadsheets"]
def _get_client_and_sid():
    try:
        sa = st.secrets["gcp_service_account"]
        sa_info = dict(sa) if hasattr(sa, "keys") else (json.loads(sa) if isinstance(sa, str) else {})
        creds = Credentials.from_service_account_info(sa_info, scopes=SCOPE)
        client = gspread.authorize(creds)
        return client, st.secrets["SPREADSHEET_ID"]
    except Exception:
        creds = Credentials.from_service_account_file("service_account.json", scopes=SCOPE)
        client = gspread.authorize(creds)
        return client, "1DV0KyDRYHofWR60zdx63a9BWBywTFhLavGAExPIa6LI"
client, SPREADSHEET_ID = _get_client_and_sid()

# ------------ Constants
EXPECTED_HEADERS = ["Nom & Prénom","Téléphone","Type de contact","Formation","Remarque","Date ajout","Date de suivi","Alerte","Inscription","Employe","Tag"]
FIN_REV_COLUMNS = ["Date","Libellé","Prix","Montant_Admin","Montant_Structure","Montant_PreInscription","Montant_Total","Echeance","Reste","Mode","Employé","Catégorie","Note"]
FIN_DEP_COLUMNS = ["Date","Libellé","Montant","Caisse_Source","Mode","Employé","Catégorie","Note"]
FIN_MONTHS_FR = ["Janvier","Février","Mars","Avril","Mai","Juin","Juillet","Aout","Octobre","Novembre","Décembre"]
# ملاحظة: سبتمبر موجود implicit في الشّيتات متاعك، لو ناقص زِد "Septembre"

INTER_NOTES_SHEET = "_InterNotes"
REASSIGN_LOG_SHEET   = "_Reassign_Log"
REASSIGN_LOG_HEADERS = ["timestamp","moved_by","src_employee","dst_employee","client_name","phone"]

def fin_month_title(mois: str, kind: str, branch: str):
    prefix = "Revenue " if kind == "Revenus" else "Dépense "
    short  = "MB" if "Menzel" in branch or "MB" in branch else "BZ"
    return f"{prefix}{mois} ({short})"

def _branch_passwords():
    try:
        b = st.secrets["branch_passwords"]
        return {"Menzel Bourguiba": str(b.get("MB","MB_2025!")), "Bizerte": str(b.get("BZ","BZ_2025!"))}
    except Exception:
        return {"Menzel Bourguiba":"MB_2025!","Bizerte":"BZ_2025!"}

# ------------ Helpers
def fmt_date(d: date|None) -> str: return d.strftime("%d/%m/%Y") if isinstance(d, date) else ""
def normalize_tn_phone(s: str) -> str:
    digits = "".join(ch for ch in str(s) if ch.isdigit())
    if digits.startswith("216"): return digits
    if len(digits) == 8: return "216"+digits
    return digits
def format_display_phone(s: str) -> str:
    d = "".join(ch for ch in str(s) if ch.isdigit())
    return f"+{d}" if d else ""
def color_tag(val):
    if isinstance(val,str) and val.strip().startswith("#") and len(val.strip())==7:
        return f"background-color:{val};color:white;"
    return ""
def mark_alert_cell(val: str):
    s = str(val).strip()
    if not s: return ''
    if "متأخر" in s: return 'background-color:#ffe6b3;color:#7a4e00'
    return 'background-color:#ffcccc;color:#7a0000'
def highlight_inscrit_row(row: pd.Series):
    insc = str(row.get("Inscription","")).strip().lower()
    return ['background-color:#d6f5e8' if insc in ("inscrit","oui") else '' for _ in row.index]

# ------------ Safe sheet open / ensure
def ensure_ws(title: str, columns: list[str]):
    sh = client.open_by_key(SPREADSHEET_ID)
    try:
        ws = sh.worksheet(title)
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title=title, rows="2000", cols=str(max(len(columns),8)))
        ws.update("1:1", [columns]); return ws
    # ما نبدلوش header كان موجود (نصححو فقط كان فارغ)
    rows = ws.get_all_values()
    if not rows:
        ws.update("1:1", [columns])
    return ws

def fin_ensure_ws(title: str, columns: list[str]):
    # نفس ensure_ws لكن للمالية
    return ensure_ws(title, columns)

# ------------ Finance read/write
def _numify(series: pd.Series):
    return (
        series.astype(str)
        .str.replace(" ", "", regex=False)
        .str.replace(",", ".", regex=False)
        .pipe(pd.to_numeric, errors="coerce").fillna(0.0)
    )

def fin_read_df(title: str, kind: str) -> pd.DataFrame:
    cols = FIN_REV_COLUMNS if kind=="Revenus" else FIN_DEP_COLUMNS
    ws = fin_ensure_ws(title, cols)
    values = ws.get_all_values()
    if not values: return pd.DataFrame(columns=cols)
    df = pd.DataFrame(values[1:], columns=values[0])

    if "Date" in df.columns:
        df["Date"] = pd.to_datetime(df["Date"], errors="coerce", dayfirst=True)
    if kind=="Revenus" and "Echeance" in df.columns:
        df["Echeance"] = pd.to_datetime(df["Echeance"], errors="coerce", dayfirst=True)

    if kind=="Revenus":
        for c in ["Prix","Montant_Admin","Montant_Structure","Montant_PreInscription","Montant_Total","Reste"]:
            if c in df.columns: df[c] = _numify(df[c])
        # تنبيه
        if "Echeance" in df.columns and "Reste" in df.columns:
            today_ts = pd.Timestamp.now().normalize()
            ech = pd.to_datetime(df["Echeance"], errors="coerce")
            reste = pd.to_numeric(df["Reste"], errors="coerce").fillna(0.0)
            df["Alert"] = ""
            df.loc[ech.notna() & (ech < today_ts) & (reste > 0), "Alert"] = "⚠️ متأخر"
            df.loc[ech.notna() & (ech.dt.normalize() == today_ts) & (reste > 0), "Alert"] = "⏰ اليوم"
    else:
        if "Montant" in df.columns: df["Montant"] = _numify(df["Montant"])

    return df

def fin_append_row(title: str, row: dict, kind: str):
    cols = FIN_REV_COLUMNS if kind=="Revenus" else FIN_DEP_COLUMNS
    ws = fin_ensure_ws(title, cols)
    header = ws.row_values(1) or cols
    vals = [str(row.get(col, "")) for col in header]
    ws.append_row(vals)

# ----------- Load all CRM (employees only)
@st.cache_data(ttl=600)
def load_all_data():
    sh = client.open_by_key(SPREADSHEET_ID)
    ws_list = sh.worksheets()
    dfs, emps = [], []
    for ws in ws_list:
        t = ws.title.strip()
        if t.endswith("_PAIEMENTS"): continue
        if t.startswith("_"): continue                    # system/logs
        if t.startswith("Revenue ") or t.startswith("Dépense "): continue  # المالية
        # ورقة موظّف:
        emps.append(t)
        vals = ws.get_all_values()
        if not vals: ws.update("1:1", [EXPECTED_HEADERS]); vals = ws.get_all_values()
        data = vals[1:] if len(vals)>1 else []
        rows = []
        for r in data:
            r = list(r or [])
            if len(r)<len(EXPECTED_HEADERS): r += [""]*(len(EXPECTED_HEADERS)-len(r))
            else: r = r[:len(EXPECTED_HEADERS)]
            rows.append(r)
        df = pd.DataFrame(rows, columns=EXPECTED_HEADERS)
        df["__sheet_name"] = t
        dfs.append(df)
    big = pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame(columns=EXPECTED_HEADERS+["__sheet_name"])
    return big, emps

df_all, all_employes = load_all_data()

# ----------- Sidebar
try:
    st.sidebar.image(Image.open("logo.png"), use_container_width=True)
except: pass

st.sidebar.button("🔄 تحديث (Clear cache)", on_click=lambda: st.cache_data.clear())
tab = st.sidebar.radio("📑 اختر تبويب:", ["CRM", "مداخيل/مصاريف (MB/Bizerte)"], index=0)
role = st.sidebar.radio("الدور", ["موظف","أدمن"], horizontal=True)
employee = None
if role=="موظف":
    # نظهر كان أوراق الموظّفين
    employee = st.sidebar.selectbox("👨‍💼 اختر الموظّف", all_employes) if all_employes else None

# ----------- Admin lock
def admin_unlocked() -> bool:
    ok = st.session_state.get("admin_ok", False)
    ts = st.session_state.get("admin_ok_at")
    return bool(ok and ts and (datetime.now()-ts)<=timedelta(minutes=30))

def admin_lock_ui():
    with st.sidebar.expander("🔐 إدارة (Admin)", expanded=(role=="أدمن" and not admin_unlocked())):
        if admin_unlocked():
            if st.button("قفل صفحة الأدمِن"): st.session_state["admin_ok"]=False; st.session_state["admin_ok_at"]=None; st.rerun()
        else:
            pwd = st.text_input("كلمة سرّ الأدمِن", type="password")
            if st.button("فتح صفحة الأدمِن"):
                conf = str(st.secrets.get("admin_password","admin123"))
                if pwd and pwd==conf:
                    st.session_state["admin_ok"]=True; st.session_state["admin_ok_at"]=datetime.now()
                    st.success("تم الفتح لمدّة 30 د.")
                else:
                    st.error("غلط في كلمة السرّ.")
if role=="أدمن": admin_lock_ui()

# ----------- Derived CRM data
df_all = df_all.copy()
if not df_all.empty:
    df_all["DateAjout_dt"] = pd.to_datetime(df_all["Date ajout"], errors="coerce", dayfirst=True)
    df_all["DateSuivi_dt"] = pd.to_datetime(df_all["Date de suivi"], errors="coerce", dayfirst=True)
    df_all["Mois"] = df_all["DateAjout_dt"].dt.strftime("%m-%Y")
    today = datetime.now().date()
    base_alert = df_all["Alerte"].fillna("").astype(str).str.strip()
    dsv = df_all["DateSuivi_dt"].dt.date
    df_all["Alerte_view"] = base_alert
    df_all.loc[base_alert.eq("") & dsv.lt(today).fillna(False), "Alerte_view"] = "⚠️ متابعة متأخرة"
    df_all.loc[base_alert.eq("") & dsv.eq(today).fillna(False), "Alerte_view"] = "⏰ متابعة اليوم"
    df_all["Téléphone_norm"] = df_all["Téléphone"].apply(normalize_tn_phone)
    ALL_PHONES = set(df_all["Téléphone_norm"].dropna().astype(str))
    df_all["Inscription_norm"] = df_all["Inscription"].fillna("").astype(str).str.strip().str.lower()
else:
    ALL_PHONES=set()

# =============== TAB: Finance ==================
if tab=="مداخيل/مصاريف (MB/Bizerte)":
    st.header("💸 المداخيل والمصاريف — (منزل بورقيبة & بنزرت)")
    with st.sidebar:
        st.markdown("---")
        branch = st.selectbox("الفرع", ["Menzel Bourguiba","Bizerte"], key="fin_branch")
        kind_ar = st.radio("النوع", ["مداخيل","مصاريف"], horizontal=True, key="fin_kind")
        kind = "Revenus" if kind_ar=="مداخيل" else "Dépenses"
        mois = st.selectbox("الشهر", FIN_MONTHS_FR, index=min(datetime.now().month-1, len(FIN_MONTHS_FR)-1), key="fin_month")
        # فلترة حسب الموظّف (اختيارية باش ما تطيّحش البيانات)
        flt_by_emp = st.checkbox("فلترة بالقائم (Employé)", value=False)
        emp_filter = st.selectbox("اختر الموظف للفلاتر", all_employes, index=0) if (flt_by_emp and all_employes) else None

        BR = _branch_passwords(); key_pw = f"finance_pw_ok::{branch}"
        if key_pw not in st.session_state: st.session_state[key_pw]=False
        if not st.session_state[key_pw]:
            pw = st.text_input("كلمة سرّ الفرع", type="password")
            if st.button("دخول الفرع"):
                if pw and pw==BR.get(branch,""): st.session_state[key_pw]=True; st.success("تم الدخول ✅")
                else: st.error("كلمة سرّ غير صحيحة ❌")
    if not st.session_state.get(f"finance_pw_ok::{branch}", False):
        st.info("⬅️ أدخل كلمة السرّ من اليسار للمتابعة."); st.stop()

    title = fin_month_title(mois, kind, branch)
    df_fin = fin_read_df(title, kind)
    if flt_by_emp and emp_filter and "Employé" in df_fin.columns:
        df_fin = df_fin[df_fin["Employé"].fillna("").str.strip().str.lower() == emp_filter.strip().lower()]

    with st.expander("🔎 فلاتر"):
        c1,c2,c3 = st.columns(3)
        d_from = c1.date_input("من تاريخ", value=None)
        d_to   = c2.date_input("إلى تاريخ", value=None)
        search = c3.text_input("بحث (Libellé/Mode/Catégorie/Note/Employé)")
        if "Date" in df_fin.columns:
            if d_from: df_fin = df_fin[df_fin["Date"] >= pd.to_datetime(d_from)]
            if d_to:   df_fin = df_fin[df_fin["Date"] <= pd.to_datetime(d_to)]
        if search:
            m = pd.Series([False]*len(df_fin))
            for c in [col for col in ["Libellé","Mode","Employé","Catégorie","Note","Caisse_Source"] if col in df_fin.columns]:
                m |= df_fin[c].fillna("").astype(str).str.contains(search, case=False, na=False)
            df_fin = df_fin[m]

    st.subheader(f"📄 {title}")
    if kind=="Revenus":
        cols_show = [c for c in ["Date","Libellé","Prix","Montant_Admin","Montant_Structure","Montant_PreInscription","Montant_Total","Echeance","Reste","Alert","Mode","Employé","Catégorie","Note"] if c in df_fin.columns]
    else:
        cols_show = [c for c in ["Date","Libellé","Montant","Caisse_Source","Mode","Employé","Catégorie","Note"] if c in df_fin.columns]
    st.dataframe(df_fin[cols_show] if not df_fin.empty else pd.DataFrame(columns=cols_show), use_container_width=True)

    # Admin-only monthly summary
    if role=="أدمن" and admin_unlocked():
        with st.expander("📊 ملخّص شهري (Admin only)"):
            rev_df = fin_read_df(fin_month_title(mois,"Revenus",branch), "Revenus")
            dep_df = fin_read_df(fin_month_title(mois,"Dépenses",branch), "Dépenses")
            sum_admin  = rev_df["Montant_Admin"].sum() if "Montant_Admin" in rev_df else 0.0
            sum_struct = rev_df["Montant_Structure"].sum() if "Montant_Structure" in rev_df else 0.0
            sum_preins = rev_df["Montant_PreInscription"].sum() if "Montant_PreInscription" in rev_df else 0.0
            sum_total_as = rev_df["Montant_Total"].sum() if "Montant_Total" in rev_df else (sum_admin+sum_struct)
            sum_reste = rev_df["Reste"].sum() if "Reste" in rev_df else 0.0
            if not dep_df.empty and "Caisse_Source" in dep_df.columns and "Montant" in dep_df.columns:
                dep_admin  = dep_df.loc[dep_df["Caisse_Source"]=="Caisse_Admin","Montant"].sum()
                dep_struct = dep_df.loc[dep_df["Caisse_Source"]=="Caisse_Structure","Montant"].sum()
                dep_inscr  = dep_df.loc[dep_df["Caisse_Source"]=="Caisse_Inscription","Montant"].sum()
            else:
                dep_admin=dep_struct=dep_inscr=0.0
            reste_admin  = float(sum_admin)  - float(dep_admin)
            reste_struct = float(sum_struct) - float(dep_struct)
            reste_inscr  = float(sum_preins) - float(dep_inscr)
            a1,a2,a3 = st.columns(3); a1.metric("Income Admin",f"{sum_admin:,.2f}"); a2.metric("Expense Admin",f"{dep_admin:,.2f}"); a3.metric("Reste Admin",f"{reste_admin:,.2f}")
            b1,b2,b3 = st.columns(3); b1.metric("Income Structure",f"{sum_struct:,.2f}"); b2.metric("Expense Structure",f"{dep_struct:,.2f}"); b3.metric("Reste Structure",f"{reste_struct:,.2f}")
            c1,c2,c3 = st.columns(3); c1.metric("Income Inscription",f"{sum_preins:,.2f}"); c2.metric("Expense Inscription",f"{dep_inscr:,.2f}"); c3.metric("Reste Inscription",f"{reste_inscr:,.2f}")
            st.caption(f"Total (A+S): {sum_total_as:,.2f} — Reste dû: {sum_reste:,.2f}")

    st.markdown("---"); st.subheader("➕ إضافة عملية جديدة")
    # اختيار عميل (للمداخيل)
    selected_client_info = None
    default_lib = ""
    default_emp = (employee or "")
    if kind=="Revenus":
        st.markdown("#### 👤 اربط الدفعة بعميل مُسجَّل (اختياري)")
        reg = df_all.copy()
        reg["Inscription_norm"] = reg["Inscription"].fillna("").astype(str).str.strip().str.lower()
        reg = reg[reg["Inscription_norm"].isin(["oui","inscrit"])]
        if role=="موظف" and employee: reg = reg[reg["__sheet_name"]==employee]
        options=[]
        if not reg.empty:
            def _opt(r):
                return f"{r['Nom & Prénom']} — {format_display_phone(r['Téléphone'])} — {r['Formation']}  [{r['__sheet_name']}]"
            options=[_opt(r) for _,r in reg.iterrows()]
        pick = st.selectbox("اختر عميلًا", ["— بدون اختيار —"]+options)
        if pick and pick!="— بدون اختيار —":
            idx = options.index(pick); row = reg.iloc[idx]
            selected_client_info = {"name":str(row["Nom & Prénom"]).strip(),
                                    "tel": normalize_tn_phone(row["Téléphone"]),
                                    "formation": str(row["Formation"]).strip(),
                                    "emp": str(row["__sheet_name"]).strip()}
            default_lib = f"Paiement {selected_client_info['formation']} - {selected_client_info['name']}"
            if not default_emp: default_emp = selected_client_info["emp"]

            # 🧾 دفعات سابقة (السنة الحالية) بالـ phone أو الاسم (Libellé/Note) وعبر الأشهر
            year = datetime.now().year
            prev_all=[]
            months = ["Janvier","Février","Mars","Avril","Mai","Juin","Juillet","Aout","Septembre","Octobre","Novembre","Décembre"]
            for m in months:
                t = fin_month_title(m,"Revenus",branch)
                try:
                    dfm = fin_read_df(t,"Revenus")
                except Exception:
                    dfm = pd.DataFrame(columns=FIN_REV_COLUMNS)
                if dfm.empty: continue
                cond_phone = dfm.get("Note","").astype(str).str.contains(selected_client_info["tel"], na=False)
                cond_name  = dfm.get("Libellé","").astype(str).str.contains(selected_client_info["name"], case=False, na=False) | \
                             dfm.get("Note","").astype(str).str.contains(selected_client_info["name"], case=False, na=False)
                sub = dfm[cond_phone | cond_name].copy()
                if not sub.empty:
                    sub["__sheet_title"]=t; sub["__mois"]=m; prev_all.append(sub)
            if prev_all:
                prev_df = pd.concat(prev_all, ignore_index=True)
                st.markdown("#### 💾 دفعات سابقة")
                show = [c for c in ["__mois","Date","Libellé","Montant_Admin","Montant_Structure","Montant_PreInscription","Montant_Total","Reste","Mode","Employé","Catégorie","Note"] if c in prev_df.columns]
                st.dataframe(prev_df[show].sort_values("__mois"), use_container_width=True, height=220)

                # تعديل دفعة
                st.markdown("### ✏️ تعديل دفعة موجودة")
                # نبني label مفهوم
                def _lbl(r):
                    d = r["Date"].strftime("%d/%m/%Y") if isinstance(r["Date"], pd.Timestamp) else str(r["Date"])
                    return f"[{r['__mois']}] {d} — Tot:{r.get('Montant_Total',0)} / Reste:{r.get('Reste',0)}"
                choices=[_lbl(r) for _,r in prev_df.iterrows()]
                sel = st.selectbox("اختر الدفعة", choices) if choices else None
                if sel:
                    r = prev_df.iloc[choices.index(sel)]
                    # فورم تعديل
                    with st.form("edit_payment_form"):
                        c1,c2,c3 = st.columns(3)
                        new_date = c1.date_input("Date", value=(r["Date"].date() if isinstance(r["Date"], pd.Timestamp) else date.today()))
                        new_mode = c2.selectbox("Mode", ["Espèces","Virement","Carte","Chèque","Autre"], index=0)
                        new_emp  = c3.text_input("Employé", value=str(r.get("Employé","")))
                        n1,n2,n3 = st.columns(3)
                        a = n1.number_input("Montant Admin", min_value=0.0, value=float(r.get("Montant_Admin",0) or 0.0), step=10.0)
                        s = n2.number_input("Montant Structure", min_value=0.0, value=float(r.get("Montant_Structure",0) or 0.0), step=10.0)
                        p = n3.number_input("Montant Pré-Inscription", min_value=0.0, value=float(r.get("Montant_PreInscription",0) or 0.0), step=10.0)
                        tot = a+s
                        r1,r2 = st.columns(2)
                        reste = r1.number_input("Reste", min_value=0.0, value=float(r.get("Reste",0) or 0.0), step=10.0)
                        cat   = r2.text_input("Catégorie", value=str(r.get("Catégorie","Revenus")))
                        note  = st.text_area("Note", value=str(r.get("Note","")))
                        ok = st.form_submit_button("💾 حفظ التعديل")
                    if ok:
                        try:
                            ws = fin_ensure_ws(str(r["__sheet_title"]), FIN_REV_COLUMNS)
                            rows = ws.get_all_values(); header = rows[0] if rows else []
                            idx_lib = header.index("Libellé"); idx_dt = header.index("Date")
                            # نلقاو الصفّ بنفس Libellé + تاريخ قديم (أو الجديد)
                            target_idx=None
                            for i,row in enumerate(rows[1:], start=2):
                                if len(row)>max(idx_lib,idx_dt):
                                    if row[idx_lib].strip()==str(r["Libellé"]).strip():
                                        target_idx=i; break
                            if not target_idx: st.error("❌ الصفّ ما تلقاش."); st.stop()
                            colmap = {h: header.index(h)+1 for h in header}
                            def _upd(h,v):
                                if h in colmap: ws.update_cell(target_idx, colmap[h], v)
                            _upd("Date", fmt_date(new_date)); _upd("Libellé", str(r["Libellé"]))
                            _upd("Montant_Admin", f"{float(a):.2f}"); _upd("Montant_Structure", f"{float(s):.2f}")
                            _upd("Montant_PreInscription", f"{float(p):.2f}"); _upd("Montant_Total", f"{float(tot):.2f}")
                            _upd("Reste", f"{float(reste):.2f}"); _upd("Mode", new_mode); _upd("Employé", new_emp); _upd("Catégorie", cat); _upd("Note", note)
                            st.success("تمّ الحفظ ✅"); st.cache_data.clear(); st.rerun()
                        except Exception as e:
                            st.error(f"خطأ أثناء التعديل: {e}")

    # فورم الإضافة
    with st.form("fin_add_form"):
        d1,d2,d3 = st.columns(3)
        dt = d1.date_input("Date", value=date.today())
        lib = d2.text_input("Libellé", value=(default_lib if kind=="Revenus" else ""))
        emp = d3.text_input("Employé", value=(default_emp if default_emp else ""))

        if kind=="Revenus":
            r1,r2,r3 = st.columns(3)
            prix  = r1.number_input("💰 Prix", min_value=0.0, step=10.0)
            adm   = r2.number_input("🏢 Montant Admin", min_value=0.0, step=10.0)
            stru  = r3.number_input("🏫 Montant Structure", min_value=0.0, step=10.0)
            r4,r5 = st.columns(2)
            prei  = r4.number_input("📝 Montant Pré-Inscription", min_value=0.0, step=10.0)
            tot   = float(adm)+float(stru)
            e1,e2,e3 = st.columns(3)
            ech  = e1.date_input("⏰ Echeance", value=date.today())
            mode = e2.selectbox("Mode", ["Espèces","Virement","Carte","Chèque","Autre"])
            cat  = e3.text_input("Catégorie", value="Revenus")
            note_default = f"Client: {selected_client_info['name']} | Tel:{selected_client_info['tel']}" if selected_client_info else ""
            note = st.text_area("Note", value=note_default)

            # Reste = Prix - (Total Admin+Structure) considering past same Libellé this month
            curr = fin_read_df(title, "Revenus")
            paid_so_far = 0.0
            if not curr.empty and "Libellé" in curr.columns and "Montant_Total" in curr.columns:
                same = curr[curr["Libellé"].fillna("").str.strip().str.lower() == lib.strip().lower()]
                paid_so_far = float(same["Montant_Total"].sum()) if not same.empty else 0.0
            reste = max(float(prix) - (paid_so_far + float(tot)), 0.0)
            st.caption(f"Total الآن: {tot:.2f} — مدفوع سابقًا لنفس Libellé (هذا الشهر): {paid_so_far:.2f} — Reste: {reste:.2f}")

            ok = st.form_submit_button("✅ حفظ العملية")
            if ok:
                if not lib.strip(): st.error("Libellé مطلوب."); st.stop()
                if prix<=0:         st.error("Prix مطلوب."); st.stop()
                if tot<=0 and prei<=0: st.error("المبلغ لازم > 0."); st.stop()
                fin_append_row(
                    title,
                    {"Date":fmt_date(dt),"Libellé":lib.strip(),"Prix":f"{float(prix):.2f}",
                     "Montant_Admin":f"{float(adm):.2f}","Montant_Structure":f"{float(stru):.2f}",
                     "Montant_PreInscription":f"{float(prei):.2f}","Montant_Total":f"{float(tot):.2f}",
                     "Echeance":fmt_date(ech),"Reste":f"{float(reste):.2f}","Mode":mode,
                     "Employé":emp.strip(),"Catégorie":cat.strip(),"Note":note.strip()},
                    "Revenus"
                )
                st.success("تمّ الحفظ ✅"); st.cache_data.clear(); st.rerun()
        else:
            r1,r2,r3 = st.columns(3)
            mnt  = r1.number_input("Montant", min_value=0.0, step=10.0)
            caisse = r2.selectbox("Caisse_Source", ["Caisse_Admin","Caisse_Structure","Caisse_Inscription"])
            mode = r3.selectbox("Mode", ["Espèces","Virement","Carte","Chèque","Autre"])
            c2,c3 = st.columns(2)
            cat = c2.text_input("Catégorie", value="Achat")
            note = c3.text_area("Note (اختياري)")
            ok = st.form_submit_button("✅ حفظ العملية")
            if ok:
                if not lib.strip(): st.error("Libellé مطلوب."); st.stop()
                if mnt<=0:         st.error("المبلغ لازم > 0."); st.stop()
                fin_append_row(
                    title,
                    {"Date":fmt_date(dt),"Libellé":lib.strip(),"Montant":f"{float(mnt):.2f}",
                     "Caisse_Source":caisse,"Mode":mode,"Employé":emp.strip(),"Catégorie":cat.strip(),"Note":note.strip()},
                    "Dépenses"
                )
                st.success("تمّ الحفظ ✅"); st.cache_data.clear(); st.rerun()
    st.stop()

# =============== TAB: CRM ==================
st.subheader("لوحة إحصائيات سريعة")
df_dash = df_all.copy()
if df_dash.empty:
    st.info("ما فماش داتا.")
else:
    df_dash["DateAjout_dt"] = pd.to_datetime(df_dash["Date ajout"], errors="coerce", dayfirst=True)
    df_dash["DateSuivi_dt"] = pd.to_datetime(df_dash["Date de suivi"], errors="coerce", dayfirst=True)
    today = datetime.now().date()
    df_dash["Inscription_norm"] = df_dash["Inscription"].fillna("").astype(str).str.strip().str.lower()
    df_dash["Alerte_norm"] = df_dash["Alerte_view"].fillna("").astype(str).str.strip()
    added_today = df_dash["DateAjout_dt"].dt.date.eq(today)
    reg_today   = df_dash["Inscription_norm"].isin(["oui","inscrit"]) & added_today
    alerts_now  = df_dash["Alerte_norm"].ne("")
    total_clients=len(df_dash); added=int(added_today.sum()); reg=int(reg_today.sum()); alerts=int(alerts_now.sum())
    reg_total=int((df_dash["Inscription_norm"]=="oui").sum()); rate=round((reg_total/total_clients)*100,2) if total_clients else 0.0
    c1,c2,c3,c4,c5 = st.columns(5)
    c1.metric("👥 إجمالي",f"{total_clients}"); c2.metric("🆕 اليوم",f"{added}"); c3.metric("✅ اليوم",f"{reg}"); c4.metric("🚨 تنبيهات",f"{alerts}"); c5.metric("📈 نسبة التسجيل",f"{rate}%")

# شهري/موظفين/تكوين
if not df_all.empty and "DateAjout_dt" in df_all.columns:
    df_all["MonthStr"] = df_all["DateAjout_dt"].dt.strftime("%Y-%m")
    months = sorted(df_all["MonthStr"].dropna().unique(), reverse=True)
    pick = st.selectbox("اختر شهر", months)
    if pick:
        filt = df_all[df_all["MonthStr"]==pick].copy()
        total=len(filt); ins=int((filt["Inscription_norm"]=="oui").sum()); alerts=int(filt["Alerte_view"].fillna("").astype(str).str.strip().ne("").sum())
        rate=round((ins/total)*100,2) if total else 0.0
        a,b,c,d = st.columns(4); a.metric("👥 عملاء",f"{total}"); b.metric("✅ مسجّلون",f"{ins}"); c.metric("🚨 تنبيهات",f"{alerts}"); d.metric("📈 نسبة",f"{rate}%")
        st.markdown("#### 👨‍💼 حسب الموظّف")
        grp = (filt.groupby("__sheet_name").agg(Clients=("Nom & Prénom","count"),
                                                Inscrits=("Inscription_norm", lambda x:(x=="oui").sum()),
                                                Alerts=("Alerte_view", lambda x:(x.fillna('').astype(str).str.strip()!='').sum()))
               .reset_index().rename(columns={"__sheet_name":"الموظف"}))
        grp["% تسجيل"] = ((grp["Inscrits"]/grp["Clients"]).replace([float("inf"),float("nan")],0)*100).round(2)
        st.dataframe(grp.sort_values(["Inscrits","Clients"], ascending=False), use_container_width=True)
        st.markdown("#### 📚 حسب التكوين")
        grp2 = (filt.groupby("Formation").agg(Clients=("Nom & Prénom","count"),
                                              Inscrits=("Inscription_norm", lambda x:(x=="oui").sum()))
                .reset_index().rename(columns={"Formation":"التكوين"}))
        grp2["% تسجيل"] = ((grp2["Inscrits"]/grp2["Clients"]).replace([float("inf"),float("nan")],0)*100).round(2)
        st.dataframe(grp2.sort_values(["Inscrits","Clients"], ascending=False), use_container_width=True)

# بحث عالمي بالهاتف
st.subheader("🔎 بحث عام برقم الهاتف")
q_phone = st.text_input("أكتب رقم الهاتف")
if q_phone.strip():
    qn = normalize_tn_phone(q_phone)
    dd = df_all.copy(); dd["Alerte"] = dd.get("Alerte_view","")
    res = dd[dd["Téléphone_norm"]==qn]
    if res.empty: st.info("ما لقيتش.")
    else:
        cols = [c for c in EXPECTED_HEADERS if c in res.columns]
        st.dataframe(res[cols], use_container_width=True)

# منطقة الموظف: نفس اللي قبل (عرض/تعديل/ملاحظات/Tag/إضافة/نقل)
if role=="موظف" and employee:
    def _get_emp_pwd(emp): 
        try:
            mp = st.secrets["employee_passwords"]; return str(mp.get(emp, mp.get("_default","1234")))
        except: return "1234"
    def _emp_open(emp):
        ok = st.session_state.get(f"emp_ok::{emp}", False)
        ts = st.session_state.get(f"emp_ok_at::{emp}")
        return bool(ok and ts and (datetime.now()-ts)<=timedelta(minutes=15))
    with st.expander(f"🔐 حماية ورقة {employee}", expanded=not _emp_open(employee)):
        if _emp_open(employee):
            if st.button("قفل الآن"): st.session_state[f"emp_ok::{employee}"]=False; st.session_state[f"emp_ok_at::{employee}"]=None
        else:
            pw = st.text_input("كلمة سرّ", type="password")
            if st.button("فتح"):
                if pw==_get_emp_pwd(employee):
                    st.session_state[f"emp_ok::{employee}"]=True; st.session_state[f"emp_ok_at::{employee}"]=datetime.now()
                    st.success("تم الفتح 15 د.")
                else: st.error("غلط في كلمة السرّ.")
    if not _emp_open(employee): st.stop()

    st.subheader(f"📁 لوحة {employee}")
    emp_df = df_all[df_all["__sheet_name"]==employee].copy()
    if emp_df.empty: st.warning("ما فماش بيانات."); st.stop()
    emp_df["DateAjout_dt"] = pd.to_datetime(emp_df["Date ajout"], errors="coerce", dayfirst=True)
    emp_df = emp_df.dropna(subset=["DateAjout_dt"])
    emp_df["Mois"] = emp_df["DateAjout_dt"].dt.strftime("%m-%Y")
    month_filter = st.selectbox("🗓️ اختر شهر الإضافة", sorted(emp_df["Mois"].dropna().unique(), reverse=True))
    view = emp_df[emp_df["Mois"]==month_filter].copy()

    def render(df):
        if df.empty: st.info("لا توجد بيانات."); return
        df2 = df.copy(); df2["Alerte"]=df2.get("Alerte_view","")
        cols = [c for c in EXPECTED_HEADERS if c in df2.columns]
        st.dataframe(df2[cols].style.apply(highlight_inscrit_row, axis=1).applymap(mark_alert_cell, subset=["Alerte"]).applymap(color_tag, subset=["Tag"]), use_container_width=True)
    st.markdown("### 📋 قائمة العملاء"); render(view)

    # تعديل عميل — (نحافظ على نفس النموذج متاعك)
    st.markdown("### ✏️ تعديل بيانات عميل")
    edit_df = emp_df.copy(); edit_df["Téléphone_norm"]=edit_df["Téléphone"].apply(normalize_tn_phone)
    choices={f"[{i}] {r['Nom & Prénom']} — {format_display_phone(r['Téléphone_norm'])}": r["Téléphone_norm"] for i,r in edit_df.iterrows() if str(r["Téléphone"]).strip()!=""}
    if choices:
        chosen_key = st.selectbox("اختر العميل", list(choices.keys()))
        chosen_phone = choices[chosen_key]
        cur = edit_df[edit_df["Téléphone_norm"]==chosen_phone].iloc[0]
        col1,col2 = st.columns(2)
        with col1:
            new_name = st.text_input("👤 الاسم و اللقب", value=str(cur["Nom & Prénom"]))
            new_tel  = st.text_input("📞 رقم الهاتف", value=str(cur["Téléphone"]))
            new_form = st.text_input("📚 التكوين", value=str(cur["Formation"]))
        with col2:
            new_aj  = st.date_input("🕓 تاريخ الإضافة", value=pd.to_datetime(cur["Date ajout"], dayfirst=True, errors="coerce").date())
            new_sv  = st.date_input("📆 تاريخ المتابعة", value=(pd.to_datetime(cur["Date de suivi"], dayfirst=True, errors="coerce").date() if str(cur["Date de suivi"]).strip() else date.today()))
            new_ins = st.selectbox("🟢 التسجيل", ["Pas encore","Inscrit"], index=(1 if str(cur["Inscription"]).strip().lower()=="oui" else 0))
        new_rem_full = st.text_area("🗒️ ملاحظة (استبدال كامل)", value=str(cur.get("Remarque","")))
        extra_note   = st.text_area("➕ أضف ملاحظة (طابع زمني)", placeholder="اكتب ملاحظة…")
        if st.button("💾 حفظ التعديلات"):
            try:
                ws = client.open_by_key(SPREADSHEET_ID).worksheet(employee)
                vals = ws.get_all_values(); header = vals[0] if vals else []
                tel_idx = header.index("Téléphone") if "Téléphone" in header else None
                row_idx=None
                if tel_idx is not None:
                    for i,row in enumerate(vals[1:], start=2):
                        if len(row)>tel_idx and normalize_tn_phone(row[tel_idx])==chosen_phone:
                            row_idx=i; break
                if not row_idx: st.error("❌ الصفّ ما تلقاش."); st.stop()
                colmap = {h: EXPECTED_HEADERS.index(h)+1 for h in ["Nom & Prénom","Téléphone","Formation","Date ajout","Date de suivi","Inscription","Remarque"]}
                new_tel_norm = normalize_tn_phone(new_tel)
                if not new_name.strip() or not new_tel_norm: st.error("حقول أساسية ناقصة."); st.stop()
                # منع تكرار رقم
                dup = set(df_all["Téléphone_norm"]) - {chosen_phone}
                if new_tel_norm in dup: st.error("⚠️ الرقم مستعمل."); st.stop()
                ws.update_cell(row_idx, colmap["Nom & Prénom"], new_name.strip())
                ws.update_cell(row_idx, colmap["Téléphone"], new_tel_norm)
                ws.update_cell(row_idx, colmap["Formation"], new_form.strip())
                ws.update_cell(row_idx, colmap["Date ajout"], fmt_date(new_aj))
                ws.update_cell(row_idx, colmap["Date de suivi"], fmt_date(new_sv))
                ws.update_cell(row_idx, colmap["Inscription"], "Oui" if new_ins=="Inscrit" else "Pas encore")
                if new_rem_full.strip()!=str(cur.get("Remarque","")).strip():
                    ws.update_cell(row_idx, colmap["Remarque"], new_rem_full.strip())
                if extra_note.strip():
                    old = ws.cell(row_idx, colmap["Remarque"]).value or ""
                    stamp = datetime.now().strftime("%d/%m/%Y %H:%M")
                    ws.update_cell(row_idx, colmap["Remarque"], (old+"\n" if old else "")+f"[{stamp}] {extra_note.strip()}")
                st.success("تمّ الحفظ ✅"); st.cache_data.clear()
            except Exception as e:
                st.error(f"خطأ: {e}")

    # نقل عميل + سجلّ
    st.markdown("### 🔁 نقل عميل بين الموظفين")
    if all_employes:
        cA,cB = st.columns(2)
        src = cA.selectbox("من موظّف", all_employes, index=all_employes.index(employee) if employee in all_employes else 0)
        dst = cB.selectbox("إلى موظّف", [e for e in all_employes if e!=src])
        df_src = df_all[df_all["__sheet_name"]==src]
        if df_src.empty: st.info("لا يوجد عملاء.")
        else:
            pick = st.selectbox("اختر العميل", [f"{r['Nom & Prénom']} — {format_display_phone(r['Téléphone'])}" for _,r in df_src.iterrows()])
            phone_pick = normalize_tn_phone(pick.split("—")[-1])
            if st.button("🚚 نقل الآن"):
                try:
                    sh = client.open_by_key(SPREADSHEET_ID)
                    ws_src, ws_dst = sh.worksheet(src), sh.worksheet(dst)
                    vals = ws_src.get_all_values(); header = vals[0] if vals else []
                    row_idx=None
                    if "Téléphone" in header:
                        tel_idx = header.index("Téléphone")
                        for i,r in enumerate(vals[1:], start=2):
                            if len(r)>tel_idx and normalize_tn_phone(r[tel_idx])==phone_pick:
                                row_idx=i; break
                    if not row_idx: st.error("العميل ما تلقاش."); st.stop()
                    row_vals = ws_src.row_values(row_idx)
                    if len(row_vals)<len(EXPECTED_HEADERS): row_vals += [""]*(len(EXPECTED_HEADERS)-len(row_vals))
                    row_vals = row_vals[:len(EXPECTED_HEADERS)]
                    row_vals[EXPECTED_HEADERS.index("Employe")] = dst
                    ws_dst.append_row(row_vals); ws_src.delete_rows(row_idx)
                    # Log
                    wslog = ensure_ws(REASSIGN_LOG_SHEET, REASSIGN_LOG_HEADERS)
                    wslog.append_row([datetime.now(timezone.utc).isoformat(), employee or "Admin", src, dst, row_vals[0], normalize_tn_phone(row_vals[1])])
                    st.success("تمّ النقل ✅"); st.cache_data.clear()
                except Exception as e:
                    st.error(f"خطأ: {e}")

# Admin: إضافة/حذف موظف + سجلّ نقل
if role=="أدمن":
    st.markdown("## 👑 لوحة الأدمِن")
    if not admin_unlocked():
        st.info("🔐 افتح القفل من اليسار.")
    else:
        colA,colB,colC = st.columns(3)
        with colA:
            st.subheader("➕ إضافة موظّف")
            new_emp = st.text_input("اسم الموظّف")
            if st.button("إنشاء ورقة"):
                try:
                    sh = client.open_by_key(SPREADSHEET_ID)
                    if not new_emp or new_emp in [w.title for w in sh.worksheets()]:
                        st.warning("الاسم فارغ أو موجود.")
                    else:
                        sh.add_worksheet(title=new_emp, rows="1000", cols="20")
                        sh.worksheet(new_emp).update("1:1", [EXPECTED_HEADERS])
                        st.success("تمّ الإنشاء"); st.cache_data.clear()
                except Exception as e: st.error(e)
        with colB:
            st.subheader("➕ إضافة عميل (سريع)")
            sh = client.open_by_key(SPREADSHEET_ID)
            tgt = st.selectbox("اختر الموظّف", all_employes)
            nom = st.text_input("الاسم و اللقب"); tel = st.text_input("الهاتف"); form = st.text_input("التكوين")
            typ = st.selectbox("نوع التواصل", ["Visiteur","Appel téléphonique","WhatsApp","Social media"])
            ins = st.selectbox("التسجيل", ["Pas encore","Inscrit"])
            d1 = st.date_input("تاريخ الإضافة", value=date.today()); d2 = st.date_input("تاريخ المتابعة", value=date.today())
            if st.button("📥 أضف"):
                try:
                    if not (nom and tel and form and tgt): st.error("❌ حقول ناقصة."); st.stop()
                    teln = normalize_tn_phone(tel)
                    if teln in set(df_all["Téléphone_norm"]): st.warning("⚠️ الرقم موجود.")
                    else:
                        ws = sh.worksheet(tgt)
                        ws.append_row([nom, teln, typ, form, "", fmt_date(d1), fmt_date(d2), "", ("Oui" if ins=="Inscrit" else "Pas encore"), tgt, ""])
                        st.success("✅ تمت الإضافة"); st.cache_data.clear()
                except Exception as e: st.error(e)
        with colC:
            st.subheader("🗑️ حذف موظّف")
            to_del = st.selectbox("اختر", all_employes)
            if st.button("❗ حذف الورقة"):
                try:
                    sh = client.open_by_key(SPREADSHEET_ID); sh.del_worksheet(sh.worksheet(to_del))
                    st.success("تمّ الحذف"); st.cache_data.clear()
                except Exception as e: st.error(e)

        st.markdown("---"); st.subheader("📜 Reassign Log")
        wslog = ensure_ws(REASSIGN_LOG_SHEET, REASSIGN_LOG_HEADERS)
        vals = wslog.get_all_values()
        if vals and len(vals)>1:
            log = pd.DataFrame(vals[1:], columns=vals[0])
            if "timestamp" in log.columns:
                def _ts(x):
                    try: return datetime.fromisoformat(x).astimezone().strftime("%Y-%m-%d %H:%M")
                    except: return x
                log["وقت"] = log["timestamp"].apply(_ts)
            disp = ["وقت","moved_by","src_employee","dst_employee","client_name","phone"]
            disp = [c for c in disp if c in log.columns]
            st.dataframe(log[disp].sort_values(disp[0], ascending=False), use_container_width=True)
        else:
            st.caption("لا يوجد سجلّ نقل.")
