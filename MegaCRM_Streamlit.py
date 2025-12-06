import streamlit as st
import pandas as pd
from pathlib import Path
from datetime import datetime

# ================== إعدادات عامة ==================
st.set_page_config(page_title="MegaEdu - منصة تعليمية", layout="wide")

DATA_DIR = Path("data_megaedu")
USERS_FILE = DATA_DIR / "users.csv"
COURSES_FILE = DATA_DIR / "courses.csv"
LESSONS_FILE = DATA_DIR / "lessons.csv"
ENROLLMENTS_FILE = DATA_DIR / "enrollments.csv"
PROGRESS_FILE = DATA_DIR / "progress.csv"

def ensure_data_dir():
    DATA_DIR.mkdir(exist_ok=True)
    if not USERS_FILE.exists():
        df = pd.DataFrame(
            [
                {"email": "admin@mega.tn", "password": "admin123", "role": "admin", "full_name": "Admin Mega"},
                {"email": "formateur@mega.tn", "password": "123456", "role": "trainer", "full_name": "Khoulah"},
                {"email": "student@mega.tn", "password": "123456", "role": "student", "full_name": "Etudiant Test"},
            ]
        )
        df.to_csv(USERS_FILE, index=False)

    for f in [COURSES_FILE, LESSONS_FILE, ENROLLMENTS_FILE, PROGRESS_FILE]:
        if not f.exists():
            pd.DataFrame().to_csv(f, index=False)

def load_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    try:
        df = pd.read_csv(path)
    except Exception:
        df = pd.DataFrame()
    return df

def save_csv(df: pd.DataFrame, path: Path):
    df.to_csv(path, index=False)

def login(email, password):
    users = load_csv(USERS_FILE)
    if users.empty:
        return None
    user = users[(users["email"] == email) & (users["password"] == password)]
    if user.empty:
        return None
    return user.iloc[0].to_dict()

def get_next_id(df: pd.DataFrame, col: str = "id") -> int:
    if df.empty or col not in df.columns:
        return 1
    try:
        return int(df[col].max()) + 1
    except Exception:
        return 1

def admin_dashboard(user):
    st.subheader("📊 لوحة تحكم الأدمن")
    col1, col2, col3, col4 = st.columns(4)
    courses = load_csv(COURSES_FILE)
    lessons = load_csv(LESSONS_FILE)
    enroll = load_csv(ENROLLMENTS_FILE)
    students = load_csv(USERS_FILE)
    with col1:
        st.metric("عدد التكوينات", 0 if courses.empty else len(courses))
    with col2:
        st.metric("عدد الدروس", 0 if lessons.empty else len(lessons))
    with col3:
        st.metric("عدد التسجيلات", 0 if enroll.empty else len(enroll))
    with col4:
        st.metric("عدد الطلبة", 0 if students.empty else len(students[students["role"] == "student"]))
    st.write("من هنا تنجم تمشي لإدارة التكوينات، الدروس، و المستخدمين من القائمة على اليسار 👈")

def page_manage_courses():
    st.subheader("📚 إدارة التكوينات / الكورسات")
    courses = load_csv(COURSES_FILE)

    with st.expander("➕ إضافة تكوين جديد", expanded=True):
        title = st.text_input("عنوان التكوين")
        description = st.text_area("وصف قصير")
        level = st.selectbox("المستوى", ["A1", "A2", "B1", "B2", "Débutant", "Intermédiaire", "Avancé"])
        btn_add = st.button("حفظ التكوين")
        if btn_add and title.strip():
            cid = get_next_id(courses, "id")
            new_row = {
                "id": cid,
                "title": title.strip(),
                "description": description.strip(),
                "level": level,
                "created_at": datetime.now().isoformat(timespec="seconds"),
            }
            courses = pd.concat([courses, pd.DataFrame([new_row])], ignore_index=True)
            save_csv(courses, COURSES_FILE)
            st.success("✅ تم إضافة التكوين بنجاح")

    st.markdown("### 📋 قائمة التكوينات")
    courses = load_csv(COURSES_FILE)
    if courses.empty:
        st.info("مازال ما ثماش تكوينات. زيد واحد من الفوق.")
    else:
        st.dataframe(courses)

def page_manage_lessons():
    st.subheader("🎥 إدارة الدروس")
    courses = load_csv(COURSES_FILE)
    lessons = load_csv(LESSONS_FILE)

    if courses.empty:
        st.warning("لازم على الأقل تكوين واحد قبل ما تزيد دروس.")
        return

    with st.expander("➕ إضافة درس جديد", expanded=True):
        course_title_map = {f'{row["title"]} (ID: {row["id"]})': row["id"] for _, row in courses.iterrows()}
        course_label = st.selectbox("إختر التكوين", list(course_title_map.keys()))
        course_id = course_title_map[course_label]
        lesson_title = st.text_input("عنوان الدرس")
        video_url = st.text_input("رابط الفيديو (YouTube, Drive...)")
        attached_file = st.text_input("رابط ملف (PDF, PPT...) - إختياري")
        btn_lesson = st.button("حفظ الدرس")
        if btn_lesson and lesson_title.strip():
            lid = get_next_id(lessons, "id")
            new_row = {
                "id": lid,
                "course_id": course_id,
                "title": lesson_title.strip(),
                "video_url": video_url.strip(),
                "file_url": attached_file.strip(),
                "created_at": datetime.now().isoformat(timespec="seconds"),
            }
            lessons = pd.concat([lessons, pd.DataFrame([new_row])], ignore_index=True)
            save_csv(lessons, LESSONS_FILE)
            st.success("✅ تم إضافة الدرس")

    st.markdown("### 📋 قائمة الدروس")
    lessons = load_csv(LESSONS_FILE)
    if lessons.empty:
        st.info("مازال ما ثماش دروس.")
    else:
        df = lessons.merge(courses[["id", "title"]], left_on="course_id", right_on="id", how="left", suffixes=("", "_course"))
        df = df[["id", "title_course", "title", "video_url", "file_url", "created_at"]]
        df.rename(columns={"title_course": "course_title"}, inplace=True)
        st.dataframe(df)

def page_manage_enrollments():
    st.subheader("🧑‍🎓 تسجيل الطلبة في التكوينات")
    users = load_csv(USERS_FILE)
    courses = load_csv(COURSES_FILE)
    enroll = load_csv(ENROLLMENTS_FILE)

    students = users[users["role"] == "student"] if not users.empty else pd.DataFrame()

    if students.empty or courses.empty:
        st.warning("يلزم يكون فما طلبة وتكوينات باش تسجل.")
        return

    with st.expander("➕ تسجيل طالب في تكوين", expanded=True):
        student_map = {f'{row["full_name"]} ({row["email"]})': row["email"] for _, row in students.iterrows()}
        course_map = {f'{row["title"]} (ID: {row["id"]})': row["id"] for _, row in courses.iterrows()}
        student_label = st.selectbox("إختر الطالب", list(student_map.keys()))
        course_label = st.selectbox("إختر التكوين", list(course_map.keys()))
        btn_enroll = st.button("تسجيل")

        if btn_enroll:
            student_email = student_map[student_label]
            course_id = course_map[course_label]
            if not enroll.empty and ((enroll["student_email"] == student_email) & (enroll["course_id"] == course_id)).any():
                st.warning("هذا الطالب مسجل من قبل في هذا التكوين.")
            else:
                new_row = {
                    "id": get_next_id(enroll, "id"),
                    "student_email": student_email,
                    "course_id": course_id,
                    "created_at": datetime.now().isoformat(timespec="seconds"),
                }
                enroll = pd.concat([enroll, pd.DataFrame([new_row])], ignore_index=True)
                save_csv(enroll, ENROLLMENTS_FILE)
                st.success("✅ تم التسجيل بنجاح")

    st.markdown("### 📋 قائمة التسجيلات")
    enroll = load_csv(ENROLLMENTS_FILE)
    if enroll.empty:
        st.info("مازال ما ثماش تسجيلات.")
    else:
        df = enroll.merge(courses[["id", "title"]], left_on="course_id", right_on="id", how="left", suffixes=("", "_course"))
        df = df.merge(users[["email", "full_name"]], left_on="student_email", right_on="email", how="left", suffixes=("", "_student"))
        df = df[["id", "full_name", "student_email", "title", "created_at"]]
        df.rename(columns={"title": "course_title"}, inplace=True)
        st.dataframe(df)

def page_student_my_courses(user):
    st.subheader("📚 التكوينات متاعي")
    enroll = load_csv(ENROLLMENTS_FILE)
    courses = load_csv(COURSES_FILE)
    lessons = load_csv(LESSONS_FILE)
    progress = load_csv(PROGRESS_FILE)

    if enroll.empty:
        st.info("مازال ما ثماش تسجيلات.")
        return

    my_enroll = enroll[enroll["student_email"] == user["email"]]
    if my_enroll.empty:
        st.info("موش مسجل في حتى تكوين. إسأل الإدارة باش يسجلوك.")
        return

    my_courses = my_enroll.merge(courses, left_on="course_id", right_on="id", how="left", suffixes=("", "_course"))
    course_options = {row["title"]: row["course_id"] for _, row in my_courses.iterrows()}
    course_title = st.selectbox("إختر تكوين باش تشوف الدروس", list(course_options.keys()))
    course_id = course_options[course_title]

    course_lessons = lessons[lessons["course_id"] == course_id]
    if course_lessons.empty:
        st.warning("مازال ما ثماش دروس في التكوين هذا.")
        return

    st.markdown(f"### 🎓 دروس التكوين: {course_title}")

    for _, row in course_lessons.iterrows():
        lid = row["id"]
        done = False
        if not progress.empty:
            done = ((progress["student_email"] == user["email"]) & (progress["lesson_id"] == lid)).any()

        with st.expander(f'{"✅" if done else "⬜"} {row["title"]}', expanded=False):
            if row.get("video_url"):
                st.markdown(f"[🎥 فتح الفيديو]({row['video_url']})")
            if row.get("file_url"):
                st.markdown(f"[📄 تحميل الملف]({row['file_url']})")

            if done:
                st.success("كملت الدرس هذا ✔")
            else:
                if st.button("✅ نعلّم الدرس كمّل", key=f"done_{lid}"):
                    prog = progress if not progress.empty else pd.DataFrame(columns=["student_email", "lesson_id", "done_at"])
                    new_row = {
                        "student_email": user["email"],
                        "lesson_id": lid,
                        "done_at": datetime.now().isoformat(timespec="seconds"),
                    }
                    prog = pd.concat([prog, pd.DataFrame([new_row])], ignore_index=True)
                    save_csv(prog, PROGRESS_FILE)
                    st.success("تم حفظ التقدم ✅")
                    st.experimental_rerun()

def page_trainer_lessons(user):
    st.subheader("👨‍🏫 دروس المكوّن")
    courses = load_csv(COURSES_FILE)
    lessons = load_csv(LESSONS_FILE)
    if courses.empty or lessons.empty:
        st.info("مازال ما ثماش تكوينات أو دروس.")
        return

    df = lessons.merge(courses[["id", "title"]], left_on="course_id", right_on="id", how="left", suffixes=("", "_course"))
    df = df[["id", "title_course", "title", "video_url", "file_url", "created_at"]]
    df.rename(columns={"title_course": "course_title"}, inplace=True)
    st.dataframe(df)

def main():
    ensure_data_dir()
    st.markdown(
        """
        <div style='text-align:center'>
          <h1>🎓 MegaEdu - منصة تعليمية بسيطة</h1>
          <p>نسخة أولية باش تستعملها مع Mega Formation: كورسات، دروس، طلبة و متابعة التقدم.</p>
        </div>
        <hr/>
        """,
        unsafe_allow_html=True,
    )

    if "user" not in st.session_state:
        st.session_state.user = None

    if st.session_state.user is None:
        tab1, tab2 = st.tabs(["تسجيل الدخول", "مستخدم تجريبي / Info"])
        with tab1:
            email = st.text_input("الإيميل", value="admin@mega.tn")
            password = st.text_input("كلمة السر", type="password", value="admin123")
            if st.button("دخول"):
                user = login(email.strip(), password.strip())
                if user is None:
                    st.error("الإيميل أو كلمة السر غالطين.")
                else:
                    st.session_state.user = user
                    st.experimental_rerun()
        with tab2:
            st.info(
                """
                تنجم تجرب المنصة بالحسابات الجاهزة:
                - أدمن: admin@mega.tn / admin123
                - مكوّن: formateur@mega.tn / 123456
                - طالب: student@mega.tn / 123456
                """
            )
        return

    user = st.session_state.user
    st.sidebar.markdown(f"**مربوط باسم:** {user['full_name']}  \n**الدور:** {user['role']}")
    if st.sidebar.button("🚪 خروج"):
        st.session_state.user = None
        st.experimental_rerun()

    role = user["role"]

    if role == "admin":
        menu = st.sidebar.radio(
            "القائمة",
            ["لوحة التحكّم", "إدارة التكوينات", "إدارة الدروس", "تسجيل الطلبة"],
        )
        if menu == "لوحة التحكّم":
            admin_dashboard(user)
        elif menu == "إدارة التكوينات":
            page_manage_courses()
        elif menu == "إدارة الدروس":
            page_manage_lessons()
        elif menu == "تسجيل الطلبة":
            page_manage_enrollments()

    elif role == "student":
        menu = st.sidebar.radio("القائمة", ["التكوينات متاعي"])
        if menu == "التكوينات متاعي":
            page_student_my_courses(user)

    elif role == "trainer":
        menu = st.sidebar.radio("القائمة", ["دروسي"])
        if menu == "دروسي":
            page_trainer_lessons(user)

    else:
        st.error("دور المستخدم موش معروف.")

if __name__ == "__main__":
    main()
