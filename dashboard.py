import streamlit as st
import pandas as pd
import psycopg2
import ollama
import requests
import hashlib
import os
from pathlib import Path
from src.config import settings

st.set_page_config(
    page_title="Apple Watch Analytics & AI Coach",
    page_icon="⌚",
    layout="wide",
    initial_sidebar_state="expanded"
)

st.markdown("""
<style>
    .stApp {
        background-color: #0d0f12;
        color: #e2e8f0;
    }
    div[data-testid="stMetricValue"] {
        font-size: 2.2rem;
        font-weight: 700;
        color: #10b981;
    }
    div[data-testid="stMetricLabel"] {
        color: #94a3b8;
    }
    .main-header {
        font-family: 'SF Pro Display', -apple-system, BlinkMacSystemFont, Roboto, sans-serif;
        background: linear-gradient(135deg, #ff5e62 0%, #ff9966 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        font-size: 2.8rem;
        font-weight: 800;
        margin-bottom: 0.2rem;
    }
    .sub-header {
        color: #64748b;
        font-size: 1.1rem;
        margin-bottom: 2rem;
    }
    .card {
        background-color: #1e293b;
        padding: 1.5rem;
        border-radius: 1rem;
        border: 1px solid #334155;
        margin-bottom: 1rem;
    }
    .ai-badge {
        background: linear-gradient(135deg, #a855f7 0%, #6366f1 100%);
        color: white;
        padding: 0.2rem 0.6rem;
        border-radius: 0.5rem;
        font-size: 0.8rem;
        font-weight: 600;
    }
</style>
""", unsafe_allow_html=True)


def get_db_conn():
    try:
        conn = psycopg2.connect(
            host=settings.db_host,
            port=settings.db_port,
            database=settings.db_name,
            user=settings.db_user,
            password=settings.db_password,
        )
        return conn
    except Exception as e:
        st.error(f"Error connecting to database: {e}")
        return None


def query_db(sql: str, params=None):
    conn = get_db_conn()
    if conn is None:
        return pd.DataFrame()
    try:
        df = pd.read_sql_query(sql, conn, params=params)
        return df
    except Exception as e:
        st.error(f"Database Query Error: {e}")
        return pd.DataFrame()
    finally:
        conn.close()


def call_llm(provider, system_content, user_content, ollama_url, model_name, groq_key, groq_model):
    if provider == "Local Ollama":
        client = ollama.Client(host=ollama_url)
        response = client.chat(
            model=model_name,
            messages=[
                {"role": "system", "content": system_content},
                {"role": "user", "content": user_content}
            ]
        )
        return response['message']['content']

    elif provider == "Groq API (Free Cloud)":
        if not groq_key:
            raise ValueError("Please enter your Groq API Key in the sidebar.")
        url = "https://api.groq.com/openai/v1/chat/completions"
        headers = {
            "Authorization": f"Bearer {groq_key}",
            "Content-Type": "application/json"
        }
        payload = {
            "model": groq_model,
            "messages": [
                {"role": "system", "content": system_content},
                {"role": "user", "content": user_content}
            ],
            "temperature": 0.3
        }
        resp = requests.post(url, json=payload, headers=headers)
        if resp.status_code != 200:
            raise Exception(f"Groq API Error ({resp.status_code}): {resp.text}")
        return resp.json()['choices'][0]['message']['content']


def hash_password(password: str) -> str:
    return hashlib.sha256(password.encode()).hexdigest()


def register_user(username, password):
    conn = get_db_conn()
    if not conn:
        return False, "Database connection error."
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT username FROM gold.users WHERE username = %s", (username,))
            if cur.fetchone():
                return False, "Username already exists."
            h = hash_password(password)
            cur.execute("INSERT INTO gold.users (username, password_hash) VALUES (%s, %s)", (username, h))
            conn.commit()
            return True, "User registered successfully!"
    except Exception as e:
        conn.rollback()
        return False, f"Error registering user: {e}"
    finally:
        conn.close()


def authenticate_user(username, password):
    conn = get_db_conn()
    if not conn:
        return False
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT password_hash FROM gold.users WHERE username = %s", (username,))
            row = cur.fetchone()
            if row and row[0] == hash_password(password):
                return True
            return False
    except Exception:
        return False
    finally:
        conn.close()


if "user_id" not in st.session_state:
    st.session_state.user_id = None

if not st.session_state.user_id:
    st.markdown('<div class="main-header" style="text-align: center;">Welcome to Health Coach MVP</div>', unsafe_allow_html=True)
    st.markdown('<div class="sub-header" style="text-align: center;">Please register or login to view your personal dashboard</div>', unsafe_allow_html=True)

    mode = st.radio("Choose Action", ["Login", "Register"], horizontal=True)
    username = st.text_input("Username")
    password = st.text_input("Password", type="password")

    if st.button("Submit"):
        if not username or not password:
            st.error("Please fill in all fields.")
        elif mode == "Register":
            success, msg = register_user(username, password)
            if success:
                st.success(msg)
            else:
                st.error(msg)
        elif mode == "Login":
            if authenticate_user(username, password):
                st.session_state.user_id = username
                st.success(f"Logged in as {username}!")
                st.rerun()
            else:
                st.error("Invalid username or password.")
    st.stop()


with st.sidebar:
    st.image("https://img.icons8.com/fluent/144/000000/apple-watch.png", width=70)
    st.markdown("### Data Product Hub")
    st.markdown(f"Logged in as: `{st.session_state.user_id}`")

    if st.button("Logout"):
        st.session_state.user_id = None
        st.session_state.messages = []
        st.rerun()

    st.markdown("---")
    st.markdown("### Upload Health Export")
    uploaded_file = st.file_uploader("Upload export.zip", type=["zip"])
    if uploaded_file is not None:
        if st.button("Process Data"):
            temp_dir = Path("data/tmp_uploads")
            temp_dir.mkdir(parents=True, exist_ok=True)
            temp_zip_path = temp_dir / f"{st.session_state.user_id}_export.zip"

            with open(temp_zip_path, "wb") as f:
                f.write(uploaded_file.getbuffer())

            with st.spinner("Processing Apple Watch data..."):
                try:
                    from src.bronze.raw_loader import load_bronze
                    from src.silver.xml_parser import process_silver
                    from src.gold.aggregator import process_gold

                    st.info("Ingesting raw ZIP...")
                    load_bronze(st.session_state.user_id, str(temp_zip_path))

                    st.info("Parsing XML (Bronze -> Silver)...")
                    process_silver(st.session_state.user_id)

                    st.info("Aggregating daily summaries (Silver -> Gold)...")
                    process_gold(st.session_state.user_id)

                    st.success("Data processed successfully!")
                    st.rerun()
                except Exception as e:
                    st.error(f"Error processing data: {e}")
                finally:
                    if temp_zip_path.exists():
                        os.remove(temp_zip_path)

    st.markdown("---")
    st.markdown("### AI Engine Settings")
    ai_provider = st.radio("Select AI Provider", ["Local Ollama", "Groq API (Free Cloud)"], index=1)

    ollama_url = "http://localhost:11434"
    model_name = "llama3"
    env_groq_key = os.getenv("GROQ_API") or os.getenv("GROQ_API_KEY") or ""
    groq_key = env_groq_key
    groq_model = "llama-3.3-70b-versatile"

    if ai_provider == "Local Ollama":
        ollama_url = st.text_input("Ollama Endpoint", value="http://localhost:11434")
        model_name = st.text_input("LLM Model Name", value="llama3")
    else:
        groq_key = st.text_input("Groq API Key", value=env_groq_key, type="password", help="Get a free key at console.groq.com")
        st.markdown("[Get a free Groq API Key](https://console.groq.com/keys)")
        groq_model = st.selectbox("Groq Model", [
            "llama-3.3-70b-versatile", 
            "llama-3.1-8b-instant", 
            "mixtral-8x7b-32768", 
            "gemma2-9b-it"
        ])


st.markdown('<div class="main-header">Apple Watch Health Warehouse</div>', unsafe_allow_html=True)
st.markdown('<div class="sub-header">Premium analytics dashboard with Cloud Groq or Local Ollama AI Health Insights</div>', unsafe_allow_html=True)

df_sleep = query_db("SELECT * FROM gold.daily_sleep_summary WHERE user_id = %s ORDER BY date DESC", (st.session_state.user_id,))
df_activity = query_db("SELECT * FROM gold.daily_activity_summary WHERE user_id = %s ORDER BY date DESC", (st.session_state.user_id,))
df_workouts = query_db("SELECT * FROM gold.workouts_summary WHERE user_id = %s ORDER BY start_date DESC", (st.session_state.user_id,))
df_cardio = query_db("SELECT * FROM gold.daily_cardiovascular_summary WHERE user_id = %s ORDER BY date DESC", (st.session_state.user_id,))
df_resp = query_db("SELECT * FROM gold.daily_respiratory_summary WHERE user_id = %s ORDER BY date DESC", (st.session_state.user_id,))
df_metrics = query_db("SELECT * FROM gold.daily_metrics_summary WHERE user_id = %s ORDER BY date DESC", (st.session_state.user_id,))

tab_overview, tab_sleep, tab_activity, tab_cardio_resp, tab_workouts, tab_ai = st.tabs([
    "📊 Overview & KPIs", 
    "💤 Sleep & Recovery", 
    "🏃 Activity Trends", 
    "❤️ Cardio & Respiratory", 
    "🏋️ Workouts log", 
    "🧠 AI Health Coach"
])

with tab_overview:
    if not df_activity.empty:
        latest_act = df_activity.iloc[0]
        steps = int(latest_act['step_count'])
        active_kcal = float(latest_act['active_energy_kcal'])
        dist = float(latest_act['distance_km'])

        sleep_dur = 0.0
        sleep_status = "No Data"
        if not df_sleep.empty:
            latest_sleep = df_sleep.iloc[0]
            sleep_dur = float(latest_sleep['total_sleep_minutes']) / 60.0
            sleep_status = f"{sleep_dur:.1f} hrs"

        resting_hr = 0.0
        if not df_cardio.empty:
            latest_cardio = df_cardio.iloc[0]
            if latest_cardio['avg_resting_heart_rate'] is not None:
                resting_hr = float(latest_cardio['avg_resting_heart_rate'])

        st.markdown("### Latest Daily Metrics Summary")
        col1, col2, col3, col4 = st.columns(4)

        with col1:
            st.metric(
                label="Steps Today", 
                value=f"{steps:,}", 
                delta=f"{steps - 8000:+,} vs Goal (8k)" if steps > 0 else None
            )
        with col2:
            st.metric(
                label="Calories Burned (Active)", 
                value=f"{active_kcal:.1f} kcal", 
                delta=f"{active_kcal - 500:.1f} vs Goal (500)" if active_kcal > 0 else None
            )
        with col3:
            st.metric(label="Sleep Duration", value=sleep_status)
        with col4:
            st.metric(
                label="Avg Resting Heart Rate", 
                value=f"{resting_hr:.1f} bpm" if resting_hr > 0 else "N/A",
                delta="Elevated" if resting_hr > 75 else "Good (<70)" if resting_hr > 0 else None,
                delta_color="inverse"
            )

        st.markdown("### Weekly Performance Comparison")
        c1, c2 = st.columns(2)
        with c1:
            st.markdown("#### Step Count Trend (Last 14 Days)")
            df_chart_steps = df_activity.head(14).copy()
            df_chart_steps = df_chart_steps.sort_values('date')
            st.bar_chart(data=df_chart_steps, x='date', y='step_count', color="#10b981")

        with c2:
            st.markdown("#### Sleep Stages Trend (Last 14 Days)")
            if not df_sleep.empty:
                df_chart_sleep = df_sleep.head(14).copy()
                df_chart_sleep = df_chart_sleep.sort_values('date')
                df_chart_sleep['date'] = df_chart_sleep['date'].astype(str)
                st.bar_chart(
                    data=df_chart_sleep, 
                    x='date', 
                    y=['deep_sleep_minutes', 'light_sleep_minutes', 'rem_sleep_minutes', 'awake_minutes'],
                    stack=True
                )
            else:
                st.info("No sleep stages data available.")
    else:
        st.warning("No activity records available. Please upload your export.zip file in the sidebar to populate the database.")


with tab_sleep:
    st.markdown("### Sleep Stages & Recovery Quality")
    if not df_sleep.empty:
        col_s1, col_s2 = st.columns([1, 2])
        with col_s1:
            st.markdown("#### Average Sleep Statistics")
            avg_total = df_sleep['total_sleep_minutes'].mean() / 60.0
            avg_deep = df_sleep['deep_sleep_minutes'].mean()
            avg_light = df_sleep['light_sleep_minutes'].mean()
            avg_rem = df_sleep['rem_sleep_minutes'].mean()
            avg_awake = df_sleep['awake_minutes'].mean()

            st.metric("Avg Sleep Duration", f"{avg_total:.1f} hours")
            st.write(f"- 🛌 **Deep Sleep:** {avg_deep:.1f} minutes ({avg_deep / 60:.1f} hrs)")
            st.write(f"- 💤 **Light Sleep:** {avg_light:.1f} minutes ({avg_light / 60:.1f} hrs)")
            st.write(f"- 🧠 **REM Sleep:** {avg_rem:.1f} minutes ({avg_rem / 60:.1f} hrs)")
            st.write(f"- ⏰ **Awake Time:** {avg_awake:.1f} minutes ({avg_awake / 60:.1f} hrs)")

            if avg_total >= 7.5:
                st.success("Target sleep met! You're consistently getting 7.5+ hours of sleep.")
            else:
                st.warning("Sleep duration is below recommended 7-8 hours. Focus on sleep hygiene.")

        with col_s2:
            st.markdown("#### Sleep Stages Ratio Over Time")

            df_sleep_pct = df_sleep.copy()
            df_sleep_pct = df_sleep_pct.sort_values('date')
            df_sleep_pct['date'] = df_sleep_pct['date'].astype(str)
            st.line_chart(
                data=df_sleep_pct, 
                x='date', 
                y=['deep_sleep_minutes', 'light_sleep_minutes', 'rem_sleep_minutes']
            )

        st.markdown("#### Sleep Details Table")
        st.dataframe(df_sleep, use_container_width=True)
    else:
        st.warning("No sleep records available.")


with tab_activity:
    st.markdown("### Daily Activity & Energy Burned")
    if not df_activity.empty:
        col_a1, col_a2 = st.columns(2)
        with col_a1:
            st.markdown("#### Daily Energy Balance (Active vs Basal)")
            df_activity_sorted = df_activity.sort_values('date')
            df_activity_sorted['date'] = df_activity_sorted['date'].astype(str)
            st.line_chart(
                data=df_activity_sorted, 
                x='date', 
                y=['active_energy_kcal', 'basal_energy_kcal']
            )
        with col_a2:
            st.markdown("#### Distance Covered (km)")
            st.area_chart(
                data=df_activity_sorted,
                x='date',
                y='distance_km',
                color="#0066cc"
            )

        st.markdown("#### Physical Measurements & Stand Statistics")
        if not df_metrics.empty:
            st.dataframe(df_metrics, use_container_width=True)
        else:
            st.info("No detailed physical metrics summary table populated.")
    else:
        st.warning("No activity records available.")


with tab_cardio_resp:
    st.markdown("### Cardiovascular & Respiratory Analytics")

    col_c1, col_c2 = st.columns(2)
    with col_c1:
        st.markdown("#### Heart Rate Variability (SDNN) & Resting HR")
        if not df_cardio.empty:
            df_cardio_sorted = df_cardio.sort_values('date')
            df_cardio_sorted['date'] = df_cardio_sorted['date'].astype(str)
            st.line_chart(
                data=df_cardio_sorted,
                x='date',
                y=['avg_resting_heart_rate', 'avg_hrv_sdnn']
            )
        else:
            st.info("No cardiovascular data available.")

    with col_c2:
        st.markdown("#### Blood Oxygen Saturation & VO2 Max")
        if not df_cardio.empty and 'avg_blood_oxygen' in df_cardio.columns:
            st.line_chart(
                data=df_cardio_sorted,
                x='date',
                y=['avg_blood_oxygen', 'avg_vo2_max']
            )
        else:
            st.info("No blood oxygen or VO2 Max data available.")

    st.markdown("#### Respiratory Status (Breathing & Sleeping Disturbances)")
    if not df_resp.empty:
        df_resp_sorted = df_resp.sort_values('date')
        df_resp_sorted['date'] = df_resp_sorted['date'].astype(str)
        col_r1, col_r2 = st.columns([2, 1])
        with col_r1:
            st.line_chart(
                data=df_resp_sorted,
                x='date',
                y=['avg_respiratory_rate', 'avg_breathing_disturbances']
            )
        with col_r2:
            st.dataframe(df_resp.head(15), use_container_width=True)
    else:
        st.info("No respiratory summary records available.")


with tab_workouts:
    st.markdown("### Historical Workouts & Training")
    if not df_workouts.empty:
        col_w1, col_w2 = st.columns([1, 2])
        with col_w1:
            st.markdown("#### Workout Types Distribution")
            type_counts = df_workouts['workout_type'].value_counts()
            st.bar_chart(type_counts)

            st.markdown("#### Training Statistics Summary")
            total_duration = df_workouts['duration_minutes'].sum()
            avg_duration = df_workouts['duration_minutes'].mean()
            total_burned = df_workouts['total_energy_kcal'].sum()

            st.metric("Total Workout Hours", f"{total_duration/60:.1f} hrs")
            st.metric("Total Energy Burned", f"{total_burned:,.1f} kcal")
            st.write(f"- 🚶 **Average duration:** {avg_duration:.1f} minutes")
            st.write(f"- 🏋️ **Workout Count:** {len(df_workouts)} sessions")

        with col_w2:
            st.markdown("#### Workouts Log List")
            st.dataframe(df_workouts, use_container_width=True)
    else:
        st.warning("No workouts records available.")


with tab_ai:
    st.markdown('### Apple Watch AI Assistant <span class="ai-badge">AI Health Coach</span>', unsafe_allow_html=True)
    st.markdown("Engage with your health LLM coach. It will analyze your health metrics and provide recommendations.")

    context_str = ""
    if not df_activity.empty:
        recent_act = df_activity.head(7)
        context_str += "### ACTIVITY SUMMARY (Last 7 Days):\n"
        for _, row in recent_act.iterrows():
            context_str += f"- Date: {row['date']} | Steps: {int(row['step_count']):,} | Active Energy: {row['active_energy_kcal']:.1f} kcal | Distance: {row['distance_km']:.1f} km\n"

    if not df_sleep.empty:
        recent_sleep = df_sleep.head(7)
        context_str += "\n### SLEEP STAGES (Last 7 Days):\n"
        for _, row in recent_sleep.iterrows():
            context_str += f"- Date: {row['date']} | Deep: {row['deep_sleep_minutes']:.1f}m | Light: {row['light_sleep_minutes']:.1f}m | REM: {row['rem_sleep_minutes']:.1f}m | Awake: {row['awake_minutes']:.1f}m\n"

    if not df_cardio.empty:
        recent_cardio = df_cardio.head(7)
        context_str += "\n### CARDIOVASCULAR METRICS (Last 7 Days):\n"
        for _, row in recent_cardio.iterrows():
            resting = f"{row['avg_resting_heart_rate']:.1f} bpm" if row['avg_resting_heart_rate'] else "N/A"
            hrv = f"{row['avg_hrv_sdnn']:.1f} ms" if row['avg_hrv_sdnn'] else "N/A"
            oxygen = f"{row['avg_blood_oxygen']:.1f}%" if row['avg_blood_oxygen'] else "N/A"
            context_str += f"- Date: {row['date']} | Resting Heart Rate: {resting} | HRV (SDNN): {hrv} | Blood Oxygen Avg: {oxygen}\n"

    if not df_workouts.empty:
        recent_wk = df_workouts.head(5)
        context_str += "\n### RECENT WORKOUTS:\n"
        for _, row in recent_wk.iterrows():
            context_str += f"- Type: {row['workout_type']} | Date: {row['start_date']} | Duration: {row['duration_minutes']:.1f}m | Energy: {row['total_energy_kcal']:.1f} kcal | Distance: {row['total_distance_km']:.1f} km\n"

    if "messages" not in st.session_state:
        st.session_state.messages = []

    if st.button("Generate Weekly Health Report"):
        report_prompt = f"""
        Analyze the following Apple Watch metrics for the last 7 days and generate a concise weekly health report.
        Point out:
        1. Sleep Quality: Is the deep and REM sleep sufficient?
        2. Activity: Did I hit my steps/active energy goals? 
        3. Cardiovascular & Recovery: How are resting HR and HRV indicating my recovery?
        4. Practical insights to improve my well-being next week.

        Context Data:
        {context_str}
        """

        st.session_state.messages.append({"role": "user", "content": "Generate my weekly health insights report."})

        with st.spinner("Analyzing metrics and generating report..."):
            try:
                ai_msg = call_llm(
                    ai_provider,
                    "You are a professional Health Data Coach. Offer metrics-driven insights based on data context provided.",
                    report_prompt,
                    ollama_url,
                    model_name,
                    groq_key,
                    groq_model
                )
                st.session_state.messages.append({"role": "assistant", "content": ai_msg})
            except Exception as e:
                st.error(f"Error connecting to AI provider ({ai_provider}): {e}")
                if ai_provider == "Local Ollama":
                    st.info("Ensure the Ollama container is active and the model has been downloaded.")
                else:
                    st.info("Check your API key and internet connection.")

    for msg in st.session_state.messages:
        with st.chat_message(msg["role"]):
            st.write(msg["content"])

    if prompt := st.chat_input("Ask a question about your health data..."):
        st.session_state.messages.append({"role": "user", "content": prompt})
        with st.chat_message("user"):
            st.write(prompt)

        full_prompt = f"""
        User Health Context (Last 7 Days):
        {context_str}

        User Question: {prompt}
        """

        with st.chat_message("assistant"):
            response_placeholder = st.empty()
            with st.spinner("Thinking..."):
                try:
                    ai_msg = call_llm(
                        ai_provider,
                        "You are Antigravity AI Health Coach, an expert health data assistant. Offer tailored wellness tips based on the user's data context. Keep it direct and helpful. Note: you are an AI, not a doctor.",
                        full_prompt,
                        ollama_url,
                        model_name,
                        groq_key,
                        groq_model
                    )
                    response_placeholder.write(ai_msg)
                    st.session_state.messages.append({"role": "assistant", "content": ai_msg})
                except Exception as e:
                    st.error(f"Error processing request: {e}")
                    if ai_provider == "Local Ollama":
                        st.info("Check if Ollama is running.")
                    else:
                        st.info("Check if you entered the Groq API key.")
