import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text

st.set_page_config(layout="wide")
st.title("SQL Console")

# ✅ PostgreSQL connection
engine = create_engine("postgresql+psycopg2://babak:babak33044@localhost:5432/trading")

# ✅ Load all table names for dropdown
with engine.connect() as conn:
    tables = conn.execute(text("""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'public'
    """)).fetchall()
    tableNames = [table[0] for table in tables]

# ✅ Sidebar table selector
st.sidebar.header("📊 Tables")
selectedTable = st.sidebar.selectbox("Select a table", tableNames)

# ✅ Preview selected table
if selectedTable:
    st.subheader(f"Preview: `{selectedTable}` (latest 10 rows)")
    try:
        df = pd.read_sql(f"SELECT * FROM {selectedTable} ORDER BY timestamp DESC LIMIT 10", engine)
        st.dataframe(df)
    except Exception as e:
        st.warning(f"Preview failed: {e}")

# ✅ Sidebar SQL console
st.sidebar.markdown("#### 💻 SQL Console")
sqlCode = st.sidebar.text_area("Enter your SQL query", height=150)

# ✅ Initialize session state flags
if "runQuery" not in st.session_state:
    st.session_state.runQuery = False

# ✅ Trigger query execution
if st.sidebar.button("Run SQL"):
    st.session_state.runQuery = True

# ✅ Execute and cache result
if st.session_state.runQuery:
    try:
        result = pd.read_sql(text(sqlCode), engine)
        st.session_state["lastQueryResult"] = result
        st.session_state["totalPages"] = max(1, (len(result) // 50) + 1)
        st.session_state["currentPage"] = 1
        st.success("Query executed successfully!")
    except Exception as e:
        st.error(f"Error: {e}")
        st.session_state["lastQueryResult"] = None
    st.session_state.runQuery = False

# ✅ Display results with pagination and download
if "lastQueryResult" in st.session_state and st.session_state["lastQueryResult"] is not None:
    result = st.session_state["lastQueryResult"]
    totalPages = st.session_state.get("totalPages", 1)
    currentPage = st.session_state.get("currentPage", 1)

    st.subheader(f"Page {currentPage} of {totalPages}")

    start = (currentPage - 1) * 50
    end = start + 50
    st.dataframe(result.iloc[start:end])

    # Navigation buttons
    col1, col2, col3 = st.columns(3)
    with col1:
        if st.button("⬅️ Previous") and currentPage > 1:
            st.session_state["currentPage"] -= 1
    with col3:
        if st.button("Next ➡️") and currentPage < totalPages:
            st.session_state["currentPage"] += 1

    # ✅ Full CSV download
    csv = result.to_csv(index=False).encode("utf-8")
    st.download_button(
        label="📥 Download full CSV",
        data=csv,
        file_name="query_result.csv",
        mime="text/csv"
    )

# ✅ Cleanup
engine.dispose()
