# ===== إعادة تسمية موظّف (تحديث اسم الورقة + عمود Employe) =====
with st.expander("✏️ إعادة تسمية موظّف", expanded=False):
    if all_employes:
        old_emp = st.selectbox("اختر الموظّف الحالي", all_employes, key="rename_old_emp")
        new_emp = st.text_input("الاسم الجديد", key="rename_new_emp")
        do_fix_employe_col = st.checkbox("تحديث عمود Employe داخل الورقة إلى الاسم الجديد", value=True)
        if st.button("إعادة التسمية الآن"):
            try:
                sh = client.open_by_key(SPREADSHEET_ID)
                ws = sh.worksheet(old_emp)
                # 1) نبدل عنوان الورقة
                ws.update_title(new_emp)

                if do_fix_employe_col:
                    # 2) نحدّث عمود Employe داخل نفس الورقة
                    values = ws.get_all_values()
                    if values:
                        header = values[0]
                        if "Employe" in header:
                            emp_col_idx = header.index("Employe")  # صفرّي
                            data = values[1:]
                            # نبني باكج تحديثات خفيفة
                            updates = []
                            for i, row in enumerate(data, start=2):
                                if len(row) <= emp_col_idx:
                                    continue
                                if row[emp_col_idx] != new_emp:
                                    # A1 range للخلية (صف i, عمود emp_col_idx+1)
                                    cell_range = gspread.utils.rowcol_to_a1(i, emp_col_idx+1)
                                    updates.append({
                                        "range": f"{cell_range}",
                                        "values": [[new_emp]]
                                    })
                            if updates:
                                ws.batch_update([{"range": u["range"], "values": u["values"]} for u in updates])
                st.success(f"✅ تمّت إعادة التسمية: {old_emp} → {new_emp}")
                st.cache_data.clear()
                st.rerun()
            except Exception as e:
                st.error(f"❌ خطأ أثناء إعادة التسمية: {e}")
    else:
        st.info("لا توجد أوراق موظفين حالياً.")
