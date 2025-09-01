# ===== ✏️ تعديل بيانات عميل (يشمل الاسم والهاتف والتواريخ والتسجيل) =====
if not df_emp.empty:
    st.markdown("### ✏️ تعديل بيانات عميل")
    df_emp["Téléphone_norm"] = df_emp["Téléphone"].apply(normalize_tn_phone)

    # قائمة العملاء بالاسم + الهاتف
    phone_choices = {
        f"{row['Nom & Prénom']} — {format_display_phone(row['Téléphone_norm'])}": row["Téléphone_norm"]
        for _, row in df_emp.iterrows()
        if str(row["Téléphone"]).strip() != ""
    }

    if phone_choices:
        chosen_key = st.selectbox("اختر العميل (بالاسم/الهاتف)", list(phone_choices.keys()), key="edit_pick")
        chosen_phone = phone_choices.get(chosen_key, "")

        cur_row = df_emp[df_emp["Téléphone_norm"] == chosen_phone].iloc[0] if chosen_phone else None
        cur_name = str(cur_row["Nom & Prénom"]) if cur_row is not None else ""
        cur_tel_raw = str(cur_row["Téléphone"]) if cur_row is not None else ""
        cur_ajout = pd.to_datetime(cur_row["Date ajout"], dayfirst=True, errors="coerce").date() if cur_row is not None else date.today()
        cur_suivi = pd.to_datetime(cur_row["Date de suivi"], dayfirst=True, errors="coerce").date() if cur_row is not None and str(cur_row["Date de suivi"]).strip() else date.today()
        cur_insc = str(cur_row["Inscription"]).strip().lower() if cur_row is not None else ""

        colN1, colN2 = st.columns(2)
        with colN1:
            new_name = st.text_input("👤 الاسم و اللقب", value=cur_name, key="edit_name_txt")
        with colN2:
            new_phone_raw = st.text_input("📞 رقم الهاتف (8 أرقام أو 216XXXXXXXX)", value=cur_tel_raw, key="edit_phone_txt")

        colE1, colE2, colE3 = st.columns(3)
        with colE1:
            new_ajout = st.date_input("🕓 تاريخ الإضافة", value=cur_ajout, key="edit_ajout_dt")
        with colE2:
            new_suivi = st.date_input("📆 تاريخ المتابعة", value=cur_suivi, key="edit_suivi_dt")
        with colE3:
            new_insc = st.selectbox("🟢 التسجيل", ["Pas encore", "Inscrit"], index=(1 if cur_insc == "oui" else 0), key="edit_insc_sel")

        if st.button("💾 حفظ التعديلات", key="save_all_edits"):
            try:
                ws = client.open_by_key(SPREADSHEET_ID).worksheet(employee)
                row_idx = find_row_by_phone(ws, chosen_phone)
                if not row_idx:
                    st.error("❌ تعذّر إيجاد الصف لهذا الهاتف.")
                else:
                    # تحضير الإندكسات (أعمدة الشيت)
                    col_name = EXPECTED_HEADERS.index("Nom & Prénom") + 1
                    col_tel  = EXPECTED_HEADERS.index("Téléphone") + 1
                    col_ajout = EXPECTED_HEADERS.index("Date ajout") + 1
                    col_suivi = EXPECTED_HEADERS.index("Date de suivi") + 1
                    col_insc = EXPECTED_HEADERS.index("Inscription") + 1

                    # تطبيع/تحقق رقم الهاتف الجديد
                    new_phone_norm = normalize_tn_phone(new_phone_raw)

                    if not new_name.strip():
                        st.error("❌ الاسم و اللقب إجباري.")
                        st.stop()
                    if not new_phone_norm.strip():
                        st.error("❌ رقم الهاتف إجباري.")
                        st.stop()

                    # منع التكرار على كامل النظام (اسمح بتغيير رقمك الحالي)
                    phones_except_current = set(ALL_PHONES) - {chosen_phone}
                    if new_phone_norm in phones_except_current:
                        st.error("⚠️ الرقم موجود مسبقًا في النظام. رجاءً اختر رقمًا آخر.")
                        st.stop()

                    # تحديث القيم
                    ws.update_cell(row_idx, col_name, new_name.strip())
                    ws.update_cell(row_idx, col_tel, new_phone_norm)

                    ws.update_cell(row_idx, col_ajout, fmt_date(new_ajout))
                    ws.update_cell(row_idx, col_suivi, fmt_date(new_suivi))
                    ws.update_cell(row_idx, col_insc, ("Oui" if new_insc == "Inscrit" else "Pas encore"))

                    st.success("✅ تم حفظ التعديلات (الاسم/الهاتف/التواريخ/التسجيل)")
                    st.cache_data.clear()
            except Exception as e:
                st.error(f"❌ خطأ أثناء التعديل: {e}")
