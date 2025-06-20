# Run the query and store it in session
if "runQuery" not in st.session_state:
    st.session_state.runQuery = False

if st.sidebar.button("Run SQL"):
    st.session_state.runQuery = True

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

    st.session_state.runQuery = False  # reset

# Display paginated result
if "lastQueryResult" in st.session_state and st.session_state["lastQueryResult"] is not None:
    result = st.session_state["lastQueryResult"]
    totalPages = st.session_state.get("totalPages", 1)
    currentPage = st.session_state.get("currentPage", 1)

    st.subheader(f"Page {currentPage} of {totalPages}")
    
    start = (currentPage - 1) * 50
    end = start + 50
    st.dataframe(result.iloc[start:end])

    col1, col2, col3 = st.columns(3)
    with col1:
        if st.button("⬅️ Previous") and currentPage > 1:
            st.session_state["currentPage"] -= 1
    with col3:
        if st.button("Next ➡️") and currentPage < totalPages:
            st.session_state["currentPage"] += 1
