import streamlit as st
imprt pandas as pd
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
    df = pd.read_sql(f"SELECT * FROM {selectedTable} LIMIT 10", engine)
    st.dataframe(df)


#SQL Console
st.markdown("#### SQL Console")

sqlCode = st.text_area("Enter your SQL query", height=150)
if st.button("Execute SQL"):
    try:
        result = pd.read_sql(text(sqlCode), engine)
        st.success("Query executed successfully!")
        st.dataframe(result)
    except Exception as e:
        st.error(f"Error: {e}")

engine.dispose()  # Close the database connection when done