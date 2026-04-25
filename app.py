import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text

# --- НАСТРОЙКИ ---
DB_USER = 'root'
DB_PASSWORD = ''
DB_HOST = 'localhost'
DB_NAME = 'training_centre'

engine = create_engine(f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}/{DB_NAME}")


# Функция для логирования изменений
def log_change(conn, customer_id, field, old, new):
    if str(old) != str(new):  # Логируем только если значение реально изменилось
        sql = text("""
            INSERT INTO audit_trail (customer_id, field_name, old_value, new_value)
            VALUES (:cid, :field, :old, :new)
        """)
        conn.execute(sql, {"cid": customer_id, "field": field, "old": str(old), "new": str(new)})


# --- ЗАГРУЗКА ДАННЫХ ---
query = """
SELECT 
    c.num, c.firstname, c.surname, c.dob, c.gender, c.social, 
    c.origin_country, c.location, c.Language, c.education, 
    c.school, c.MarStatus, c.job, ce.email, cp.phone 
FROM training_centre.customer as c
LEFT JOIN training_centre.customer_email as ce on c.num=ce.customer_id
LEFT JOIN training_centre.customer_phone as cp on c.num=cp.customer_id
WHERE 
    c.firstname IS NULL OR c.surname IS NULL OR c.dob IS NULL OR
    c.gender IS NULL OR c.social IS NULL OR cp.phone IS NULL OR ce.email IS NULL 
ORDER BY c.num DESC;
"""

df = pd.read_sql(query, engine)

st.title("🗂 Data Entry & Audit Dashboard")

if not df.empty:
    options = df.apply(lambda x: f"{x['num']} | {x['firstname']} {x['surname']}", axis=1).tolist()
    selected_option = st.selectbox("Выберите клиента:", options)
    selected_id = int(selected_option.split(" | ")[0])
    row = df[df['num'] == selected_id].iloc[0]

    with st.form("audit_form"):
        col1, col2 = st.columns(2)
        with col1:
            new_fname = st.text_input("Имя", value=row['firstname'] if pd.notnull(row['firstname']) else "")
            new_email = st.text_input("Email", value=row['email'] if pd.notnull(row['email']) else "")
        with col2:
            new_sname = st.text_input("Фамилия", value=row['surname'] if pd.notnull(row['surname']) else "")
            new_phone = st.text_input("Телефон", value=row['phone'] if pd.notnull(row['phone']) else "")

        submitted = st.form_submit_button("Сохранить с аудитом")

        if submitted:
            try:
                with engine.begin() as conn:
                    # Логируем изменения перед обновлением
                    log_change(conn, selected_id, "firstname", row['firstname'], new_fname)
                    log_change(conn, selected_id, "surname", row['surname'], new_sname)
                    log_change(conn, selected_id, "email", row['email'], new_email)
                    log_change(conn, selected_id, "phone", row['phone'], new_phone)

                    # Само обновление
                    conn.execute(text("UPDATE customer SET firstname=:fn, surname=:sn WHERE num=:id"),
                                 {"fn": new_fname, "sn": new_sname, "id": selected_id})

                    if new_email:
                        conn.execute(text(
                            "INSERT INTO customer_email (customer_id, email) VALUES (:id, :em) ON DUPLICATE KEY UPDATE email=:em"),
                                     {"id": selected_id, "em": new_email})
                    if new_phone:
                        conn.execute(text(
                            "INSERT INTO customer_phone (customer_id, phone) VALUES (:id, :ph) ON DUPLICATE KEY UPDATE phone=:ph"),
                                     {"id": selected_id, "ph": new_phone})

                st.success("Данные сохранены, история изменений обновлена.")
                st.rerun()
            except Exception as e:
                st.error(f"Ошибка: {e}")

# Секция просмотра истории (внизу дашборда)
st.divider()
st.subheader("📜 Последние изменения (Audit Log)")
audit_df = pd.read_sql("SELECT * FROM audit_trail ORDER BY changed_at DESC LIMIT 10", engine)
st.table(audit_df)