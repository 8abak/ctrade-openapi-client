import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text

st.set_page_config(layout="wide")
st.title("SQL Console")

#DB connection
engine=create_engine("postgresql+psycopg2://babak:babak33044@localhost:5432/trading")


#show all tables
with engine.connect() as conn:
    tables = conn.execute(text("""
                               SELECT table_name
                               FROM information_schema.tables
                               WHERE table_schema = 'public'
                               """)).fetchall()
    tableNames = [table[0] for table in tables]
    
st.sidebar.header("📊 Tables")
selectedTable = st.sidebar.selectbox("Select a table", tableNames)

if selectedTable:
    st.subheader(f"Preview:  `{selectedTable}`")
    df = pd.read_sql(f"SELECT * FROM {selectedTable} ORDER BY timestamp DESC LIMIT 10", engine)
    st.dataframe(df)


#SQL Console
st.sidebar.markdown("#### 💻 SQL Console")

sqlCode = st.sidebar.text_area("Enter your SQL query", height=150)

#Button: execute and store in session state
if st.sidebar.button("Store SQL"):
    try:
        result = pd.read_sql(text(sqlCode), engine)
        st.session_state["lastQueryResult"] = result
        st.success("Query stored successfully!")
    except Exception as e:
        st.error(f"Error: {e}")
        st.session_state["lastQueryResult"] = None


#If we have results in sesstion, display them
if "lastQueryResult" in st.session_state and st.session_state["lastQueryResult"] is not None:
    st.subheader("Last Query Result")
    st.dataframe(st.session_state["lastQueryResult"])


engine.dispose() 