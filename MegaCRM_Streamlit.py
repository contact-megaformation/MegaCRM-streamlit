# ======================================================================
#                                   CRM: منطقة الموظف + نقل + واتساب
# ======================================================================

def render_table(df_disp: pd.DataFrame):
    if df_disp.empty:
        st.info("لا توجد بيانات.")
        return
    _df = df_disp.copy()
    _df["Alerte"] = _df.get("Alerte_view", "")
    display_cols = [c for c in EXPECTED_HEADERS if c in _df.columns]
    styled = (
        _df[display_cols]
        .style.apply(highlight_inscrit_row, axis=1)
        .applymap(mark_alert_cell, subset=["Alerte"])
        .applymap(color_tag, subset=["Tag"])
    )
    st.dataframe(styled, use_container_width=True)

if role == "موظف" and employee:
    # ---------- حماية ورقة الموظف ----------
    def _get_emp_password(emp_name: str) -> str:
        try:
            mp = st.secrets["employee_passwords"]
            return str(mp.get(emp_name, mp.get("_default", "1234")))
        except Exception:
            return "1234"

    def _emp_unlocked(emp_name: str) -> bool:
        ok = st.session_state.get(f"emp_ok::{emp_name}", False)
        ts = st.session_state.get(f"emp_ok_at::{emp_name}")
        return bool(ok and ts and (datetime.now() - ts) <= timedelta(minutes=15))

    def _emp_lock_ui(emp_name: str):
        with st.expander(f"🔐 حماية ورقة الموظّف: {emp_name}", expanded=not _emp_unlocked(emp_name)):
            if _emp_unlocked(emp_name):
                c1, c2 = st.columns(2)
                with c1:
                    st.success("مفتوح (15 دقيقة).")
                with c2:
                    if st.button("قفل الآن"):
                        st.session_state[f"emp_ok::{emp_name}"] = False
                        st.session_state[f"emp_ok_at::{emp_name}"] = None
                        st.info("تم القفل.")
            else:
                pwd_try = st.text_input("أدخل كلمة السرّ", type="password", key=f"emp_pwd_{emp_name}")
                if st.button("فتح"):
                    if pwd_try and pwd_try == _get_emp_password(emp_name):
                        st.session_state[f"emp_ok::{emp_name}"] = True
                        st.session_state[f"emp_ok_at::{emp_name}"] = datetime.now()
                        st.success("تم الفتح لمدة 15 دقيقة.")
                    else:
                        st.error("كلمة سرّ غير صحيحة.")

    _emp_lock_ui(employee)
    if not _emp_unlocked(employee):
        st.info("🔒 أدخل كلمة سرّ الموظّف لفتح الورقة.")
        st.stop()

    # ---------- بيانات الموظّف ----------
    st.subheader(f"📁 لوحة {employee}")
    df_emp = df_all[df_all["__sheet_name"] == employee].copy()

    if df_emp.empty:
        st.warning("⚠️ لا يوجد أي عملاء لهذا الموظف بعد.")
        st.stop()

    # اختيار شهر → filtered_df
    df_emp["DateAjout_dt"] = pd.to_datetime(df_emp["Date ajout"], dayfirst=True, errors="coerce")
    df_emp = df_emp.dropna(subset=["DateAjout_dt"])
    df_emp["Mois"] = df_emp["DateAjout_dt"].dt.strftime("%m-%Y")
    month_options = sorted(df_emp["Mois"].dropna().unique(), reverse=True)
    month_filter = st.selectbox("🗓️ اختر شهر الإضافة", month_options)
    filtered_df = df_emp[df_emp["Mois"] == month_filter].copy()

    st.markdown("### 📋 قائمة العملاء")
    render_table(filtered_df)

    # ===================== (A) 🚨 عرض العملاء الذين لديهم تنبيهات =====================
    if not filtered_df.empty and st.checkbox("🔴 عرض العملاء الذين لديهم تنبيهات"):
        _df_alerts = filtered_df.copy()
        _df_alerts["Alerte"] = _df_alerts.get("Alerte_view", "")
        alerts_df = _df_alerts[_df_alerts["Alerte"].fillna("").astype(str).str.strip() != ""]
        st.markdown("### 🚨 عملاء مع تنبيهات")
        if alerts_df.empty:
            st.info("لا توجد تنبيهات حاليًا ضمن الفلترة.")
        else:
            render_table(alerts_df)

    st.divider()

    # ===================== (B) 📝 ملاحظات سريعة (ختم زمني) =====================
    st.markdown("### 📝 أضف ملاحظة (سريعة)")
    scope_df = filtered_df if not filtered_df.empty else df_emp
    scope_df = scope_df.copy()
    scope_df["Téléphone_norm"] = scope_df["Téléphone"].apply(normalize_tn_phone)

    tel_to_update_key = st.selectbox(
        "اختر العميل",
        [
            f"{r['Nom & Prénom']} — {format_display_phone(normalize_tn_phone(r['Téléphone']))}"
            for _, r in scope_df.iterrows()
            if str(r.get('Téléphone','')).strip() != ""
        ],
        key="note_quick_pick"
    )
    tel_to_update = normalize_tn_phone(tel_to_update_key.split("—")[-1]) if tel_to_update_key else ""
    new_note_quick = st.text_area("🗒️ ملاحظة جديدة (سيضاف لها طابع زمني)", key="note_quick_txt")

    if st.button("📌 أضف الملاحظة", key="note_quick_btn"):
        try:
            ws = client.open_by_key(SPREADSHEET_ID).worksheet(employee)
            values = ws.get_all_values()
            header = values[0] if values else []
            if "Téléphone" in header:
                tel_idx = header.index("Téléphone")
                row_idx = None
                for i, r in enumerate(values[1:], start=2):
                    if len(r) > tel_idx and normalize_tn_phone(r[tel_idx]) == tel_to_update:
                        row_idx = i
                        break
                if not row_idx:
                    st.error("❌ الهاتف غير موجود.")
                else:
                    rem_col = EXPECTED_HEADERS.index("Remarque") + 1
                    old_remark = ws.cell(row_idx, rem_col).value or ""
                    stamp = datetime.now().strftime("%d/%m/%Y %H:%M")
                    updated = (old_remark + "\n" if old_remark else "") + f"[{stamp}] {new_note_quick.strip()}"
                    ws.update_cell(row_idx, rem_col, updated)
                    st.success("✅ تمت إضافة الملاحظة")
                    st.cache_data.clear()
        except Exception as e:
            st.error(f"❌ خطأ: {e}")

    st.divider()

    # ===================== (C) ✏️ تعديل بيانات عميل =====================
    st.markdown("### ✏️ تعديل بيانات عميل")
    df_emp_edit = df_emp.copy()
    df_emp_edit["Téléphone_norm"] = df_emp_edit["Téléphone"].apply(normalize_tn_phone)

    phone_choices = {
        f"[{i}] {row['Nom & Prénom']} — {format_display_phone(row['Téléphone_norm'])}": row["Téléphone_norm"]
        for i, row in df_emp_edit.iterrows()
        if str(row.get("Téléphone","")).strip() != ""
    }

    if phone_choices:
        chosen_key   = st.selectbox("اختر العميل (بالاسم/الهاتف)", list(phone_choices.keys()), key="edit_pick")
        chosen_phone = phone_choices.get(chosen_key, "")
        cur_row = df_emp_edit[df_emp_edit["Téléphone_norm"] == chosen_phone].iloc[0] if chosen_phone else None

        cur_name      = str(cur_row.get("Nom & Prénom","")) if cur_row is not None else ""
        cur_tel_raw   = str(cur_row.get("Téléphone",""))    if cur_row is not None else ""
        cur_formation = str(cur_row.get("Formation",""))    if cur_row is not None else ""
        cur_remark    = str(cur_row.get("Remarque",""))     if cur_row is not None else ""
        cur_ajout = (
            pd.to_datetime(cur_row.get("Date ajout",""), dayfirst=True, errors="coerce").date()
            if cur_row is not None else date.today()
        )
        cur_suivi = (
            pd.to_datetime(cur_row.get("Date de suivi",""), dayfirst=True, errors="coerce").date()
            if cur_row is not None and str(cur_row.get("Date de suivi","")).strip()
            else date.today()
        )
        cur_insc  = str(cur_row.get("Inscription","")).strip().lower() if cur_row is not None else ""

        # مفاتيح ديناميكية
        name_key   = f"edit_name_txt::{chosen_phone}"
        phone_key  = f"edit_phone_txt::{chosen_phone}"
        form_key   = f"edit_formation_txt::{chosen_phone}"
        ajout_key  = f"edit_ajout_dt::{chosen_phone}"
        suivi_key  = f"edit_suivi_dt::{chosen_phone}"
        insc_key   = f"edit_insc_sel::{chosen_phone}"
        remark_key = f"edit_remark_txt::{chosen_phone}"
        note_key   = f"append_note_txt::{chosen_phone}"

        col1, col2 = st.columns(2)
        with col1:
            new_name      = st.text_input("👤 الاسم و اللقب", value=cur_name, key=name_key)
            new_phone_raw = st.text_input("📞 رقم الهاتف", value=cur_tel_raw, key=phone_key)
            new_formation = st.text_input("📚 التكوين", value=cur_formation, key=form_key)
        with col2:
            new_ajout = st.date_input("🕓 تاريخ الإضافة", value=cur_ajout, key=ajout_key)
            new_suivi = st.date_input("📆 تاريخ المتابعة", value=cur_suivi, key=suivi_key)
            new_insc  = st.selectbox("🟢 التسجيل", ["Pas encore", "Inscrit"], index=(1 if cur_insc == "oui" else 0), key=insc_key)

        new_remark_full = st.text_area("🗒️ ملاحظة (استبدال كامل)", value=cur_remark, key=remark_key)
        extra_note      = st.text_area("➕ أضف ملاحظة جديدة (طابع زمني)", placeholder="اكتب ملاحظة لإلحاقها…", key=note_key)

        def _find_row_by_phone(ws, phone_digits: str) -> int | None:
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

        if st.button("💾 حفظ التعديلات", key="save_all_edits"):
            try:
                ws = client.open_by_key(SPREADSHEET_ID).worksheet(employee)
                row_idx = _find_row_by_phone(ws, normalize_tn_phone(chosen_phone))
                if not row_idx:
                    st.error("❌ تعذّر إيجاد الصف لهذا الهاتف.")
                else:
                    col_map = {h: (EXPECTED_HEADERS.index(h) + 1) for h in [
                        "Nom & Prénom","Téléphone","Formation","Date ajout","Date de suivi","Inscription","Remarque"
                    ]}
                    new_phone_norm = normalize_tn_phone(new_phone_raw)
                    if not new_name.strip():
                        st.error("❌ الاسم و اللقب إجباري.")
                        st.stop()
                    if not new_phone_norm.strip():
                        st.error("❌ رقم الهاتف إجباري.")
                        st.stop()

                    phones_except_current = (set(df_all["Téléphone_norm"].astype(str)) - {normalize_tn_phone(chosen_phone)})
                    if new_phone_norm in phones_except_current:
                        st.error("⚠️ الرقم موجود مسبقًا لعميل آخر.")
                        st.stop()

                    ws.update_cell(row_idx, col_map["Nom & Prénom"], new_name.strip())
                    ws.update_cell(row_idx, col_map["Téléphone"],   new_phone_norm)
                    ws.update_cell(row_idx, col_map["Formation"],   new_formation.strip())
                    ws.update_cell(row_idx, col_map["Date ajout"],  fmt_date(new_ajout))
                    ws.update_cell(row_idx, col_map["Date de suivi"], fmt_date(new_suivi))
                    ws.update_cell(row_idx, col_map["Inscription"], "Oui" if new_insc == "Inscrit" else "Pas encore")

                    if new_remark_full.strip() != cur_remark.strip():
                        ws.update_cell(row_idx, col_map["Remarque"], new_remark_full.strip())

                    if extra_note.strip():
                        old_rem = ws.cell(row_idx, col_map["Remarque"]).value or ""
                        stamp = datetime.now().strftime("%d/%m/%Y %H:%M")
                        appended = (old_rem + "\n" if old_rem else "") + f"[{stamp}] {extra_note.strip()}"
                        ws.update_cell(row_idx, col_map["Remarque"], appended)

                    st.success("✅ تم حفظ التعديلات")
                    st.cache_data.clear()
            except Exception as e:
                st.error(f"❌ خطأ أثناء التعديل: {e}")

    st.divider()

    # ===================== (D) 🎨 تلوين/Tag =====================
    st.markdown("### 🎨 اختر لون/Tag للعميل")
    scope_df2 = filtered_df if not filtered_df.empty else df_emp
    scope_df2 = scope_df2.copy()
    scope_df2["Téléphone_norm"] = scope_df2["Téléphone"].apply(normalize_tn_phone)

    tel_color_key = st.selectbox(
        "اختر العميل",
        [
            f"{r['Nom & Prénom']} — {format_display_phone(normalize_tn_phone(r['Téléphone']))}"
            for _, r in scope_df2.iterrows()
            if str(r.get('Téléphone','')).strip() != ""
        ],
        key="tag_select"
    )
    tel_color = normalize_tn_phone(tel_color_key.split("—")[-1]) if tel_color_key else ""
    hex_color = st.color_picker("اختر اللون", value=st.session_state.get("last_color", "#00AA88"))
    if st.button("🖌️ تلوين", key="tag_apply_btn"):
        try:
            ws = client.open_by_key(SPREADSHEET_ID).worksheet(employee)
            values = ws.get_all_values()
            header = values[0] if values else []
            row_idx = None
            if "Téléphone" in header:
                tel_idx = header.index("Téléphone")
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

    st.divider()

    # ===================== (E) 🔁 نقل عميل + Log =====================
    st.markdown("### 🔁 نقل عميل بين الموظفين")
    if all_employes:
        colRA, colRB = st.columns(2)
        with colRA:
            src_emp = st.selectbox("من موظّف", all_employes, key="reassign_src")
        with colRB:
            dst_emp = st.selectbox("إلى موظّف", [e for e in all_employes if e != src_emp], key="reassign_dst")

        df_src = df_all[df_all["__sheet_name"] == src_emp].copy()
        if df_src.empty:
            st.info("❕ لا يوجد عملاء عند هذا الموظّف.")
        else:
            pick = st.selectbox(
                "اختر العميل للنقل",
                [f"{r['Nom & Prénom']} — {format_display_phone(r['Téléphone'])}" for _, r in df_src.iterrows()],
                key="reassign_pick"
            )
            phone_pick = normalize_tn_phone(pick.split("—")[-1])
            mover = employee  # شكون عامل النقل (الموظف الحالي)
            if st.button("🚚 نقل الآن"):
                try:
                    sh = client.open_by_key(SPREADSHEET_ID)
                    ws_src, ws_dst = sh.worksheet(src_emp), sh.worksheet(dst_emp)
                    values = ws_src.get_all_values()
                    header = values[0] if values else []
                    row_idx = None
                    if "Téléphone" in header:
                        tel_idx = header.index("Téléphone")
                        for i, r in enumerate(values[1:], start=2):
                            if len(r) > tel_idx and normalize_tn_phone(r[tel_idx]) == phone_pick:
                                row_idx = i
                                break
                    if not row_idx:
                        st.error("❌ لم يتم العثور على هذا العميل.")
                    else:
                        row_values = ws_src.row_values(row_idx)
                        if len(row_values) < len(EXPECTED_HEADERS):
                            row_values += [""] * (len(EXPECTED_HEADERS) - len(row_values))
                        row_values = row_values[:len(EXPECTED_HEADERS)]
                        row_values[EXPECTED_HEADERS.index("Employe")] = dst_emp
                        ws_dst.append_row(row_values)
                        ws_src.delete_rows(row_idx)

                        # ✅ Log "شكون حرّك"
                        wslog = ensure_ws(REASSIGN_LOG_SHEET, REASSIGN_LOG_HEADERS)
                        wslog.append_row([
                            datetime.now(timezone.utc).isoformat(),
                            mover, src_emp, dst_emp,
                            row_values[0],
                            normalize_tn_phone(row_values[1])
                        ])

                        st.success(f"✅ نقل ({row_values[0]}) من {src_emp} إلى {dst_emp}")
                        st.cache_data.clear()
                except Exception as e:
                    st.error(f"❌ خطأ أثناء النقل: {e}")

    st.divider()

    # ===================== (F) 💬 WhatsApp =====================
    st.markdown("### 💬 تواصل WhatsApp")
    wa_pick = st.selectbox(
        "اختر العميل لفتح واتساب",
        [f"{r['Nom & Prénom']} — {format_display_phone(r['Téléphone'])}" for _, r in (filtered_df if not filtered_df.empty else df_emp).iterrows()],
        key="wa_pick"
    )
    default_msg = "سلام! معاك Mega Formation. بخصوص التكوين، نحبّوا ننسّقو معاك موعد المتابعة. 👍"
    wa_msg = st.text_area("الرسالة (WhatsApp)", value=default_msg, key="wa_msg")
    if st.button("📲 فتح WhatsApp"):
        try:
            raw_tel = wa_pick.split("—")[-1]
            tel_norm = normalize_tn_phone(raw_tel)
            url = f"https://wa.me/{tel_norm}?text={urllib.parse.quote(wa_msg)}"
            st.markdown(f"[افتح المحادثة الآن]({url})")
            st.info("اضغط على الرابط لفتح واتساب.")
        except Exception as e:
            st.error(f"❌ تعذّر إنشاء رابط واتساب: {e}")

    st.divider()

    # ===================== (G) ➕ إضافة عميل جديد =====================
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

        if st.form_submit_button("📥 أضف العميل"):
            try:
                ws = client.open_by_key(SPREADSHEET_ID).worksheet(employee)
                tel = normalize_tn_phone(tel_raw)
                if not (nom and tel and formation):
                    st.error("❌ حقول أساسية ناقصة.")
                    st.stop()
                if tel in ALL_PHONES:
                    st.warning("⚠️ الرقم موجود مسبقًا.")
                    st.stop()
                insc_val = "Oui" if inscription == "Inscrit" else "Pas encore"
                ws.append_row([
                    nom, tel, type_contact, formation, "",
                    fmt_date(date_ajout_in), fmt_date(date_suivi_in),
                    "", insc_val, employee, ""
                ])
                st.success("✅ تم إضافة العميل")
                st.cache_data.clear()
            except Exception as e:
                st.error(f"❌ خطأ أثناء الإضافة: {e}")
