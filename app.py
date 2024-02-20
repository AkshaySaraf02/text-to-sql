import streamlit as st
import pandas as pd
from cosine_similarity import cosine_similarity_scores
import PyPDF2 as pdf
from openai import OpenAI
from sql_to_data import execution_context_creation, sql_execution, data_retrieval, destroy_execution_context
import time
from sql_doctor import curated_sql
from utils import s3_import, s3_export

def text_extraction(file):

    text = ""
    if file.name.split(".")[-1] == "pdf":
        # pdb.set_trace()
        raw_text = pdf.PdfReader(file)
        for page in range(len(raw_text.pages)):
            page = raw_text.pages[page]
            text += str(page.extract_text())
        return text
    else:
        for line in file:
            text += str(line).replace("\\r\\n", " ").replace("\\t", "   ").replace("b'", "").replace("'", '')
        return text
    

def generate(formatted_tables, needed_kpis, query, required_dbs):
    completion = client.chat.completions.create(
    model = gpt_engine, # have to make this GPT-4
    messages=[{"role": "system", "content": f"""
    
        You are Spark SQL Expert, based on the database schema info and KPI information from user generate a sql (Spark SQL) query.
        You are Spark SQL Expert, based on the database schema info and KPI information from user generate a Spark SQL query.
        Understand the context given for each schema and KPI before calculating to better understand the requirement.
        It is not necessary to use all the tables, only use the ones you think are required. Any column you dont recognize you must for sure understand from KPI info given below and calculate based on that only.
        Don't give any explanation unless asked, just output the query. Ask if theres any doubt, don't output wrong info. 
        
        Note: Below given information/context is in the form of list of dictionaries.

                Database schema: [{formatted_tables}],
                KPIs data: {needed_kpis}
        
        Important rules to follow while generating the query:
            1. Query should be strictly a Spark SQL Query.
            1. Make sure that all tables have aliases and all columns have prefixes of aliases of tables they are extracted from, to avoid ambiguous column issues.
            3. Do not consider dim_event_date_id column as date, it is just identifier for a particular date, Instead use date column from date table for date operations. 
        
        Output format:
        Spark SQL Query
        /summaryends (This separation is very mandatory. Never ever forget to put this separation.)
        One line statement of what data and how you extracted the data purely based on the SQL query you made in a very very short and concise manner. Make sure to match what you say and what you have done in the query, both should match. also point out if you feel there is a important table missing. 
        

            """},
    {"role": "user", "content": f"{query}"}])

    sql = (completion.choices[0].message.content).split("/summaryends")[0].replace("`", "").replace("sql", "")
    interpretation = completion.choices[0].message.content.split("/summaryends")[1]

    st.session_state.sql = sql
    st.session_state.query = query
    st.session_state.required_dbs = required_dbs

    return sql, interpretation


def modify_sql(interpretation, sql_query, feedback, formatted_tables, needed_kpis):
    completion = client.chat.completions.create(
    model = gpt_engine,
    messages=[{"role": "system", "content": f"""
    
        You are a Spark SQL expert, You have previously generated a Spark SQL query based on a prompt from the user which you will now have to 
        modify and tailor a bit based on the user's feedback. You will use your great expertise in modifying and tailoring that existing query 
        (which was generated by you previously) based on the users given feedback or changes. 
        You'll take in to account three things: 
            1. Your prior understanding of the pre existing query (Generated by you). 
            2. The pre existing query (Generated by you).
            3. User's feeback on the query i.e the new changes user wants.

        Other inputs like database tables and kpis are also given below that may prove helpful in calculating the query.        
        Note: Below given information/context is in the form of list of dictionaries.

                Database schema: [{formatted_tables}],
                KPIs data: {needed_kpis}
        
        
        Output format:
        Spark SQL Query
        /summaryends (This separation is very mandatory. Never ever forget to put this separation.)
        One line statement of what data and how you extracted the data purely based on the SQL query you made in a very very short and concise manner. Make sure to match what you say and what you have done in the query, both should match. also point out if you feel there is a important table missing. 
        

            """},
    {"role": "user", "content": f"""
                                    1. Interpretation: {interpretation}
                                    2. Previous query: {sql_query}
                                    3. Feedback / Changes Needed : {feedback}
                                """}])

st.title("Text to SQL 🤖")
st.sidebar.title("Inputs")

# Initializing OpenAI Client
OPEN_AI_API_KEY = st.sidebar.text_input('Enter your OpenAI API KEY', )

if OPEN_AI_API_KEY[-10:] == "5uPMacTtmb":
    gpt_engine = "gpt-4-turbo-preview"
else:
    gpt_engine = "gpt-3.5-turbo"

client = OpenAI(api_key=OPEN_AI_API_KEY)


# Session state code for nested buttons. 
if "sql" and "query" not in st.session_state:
    st.session_state.sql = ""
    st.session_state.query = ""

if "required_dbs" not in st.session_state:
    st.session_state.required_dbs = []

if "context_id" not in st.session_state:
    st.session_state.context_id = ""

if "messages" not in st.session_state.keys():
    st.session_state.messages = [{"role": "assistant", "content": "Hey! Can I help you with any data request?"}]

for message in st.session_state.messages:
    with st.chat_message(message["role"]):
        if "Required query" in message["content"] and message["role"]== "assistant":
            # st.subheader("System's Interpretation: ")
            st.write(message["content"].split("Required query")[0])
            with st.expander("Needed tables using Cosine Similarity"):
                required_dbs = eval(message["content"].split("Required query")[1].split("required_dbs")[1])
                for i, db in enumerate(required_dbs):
                        st.write(i+1,". ", db["table_name"])
            st.subheader("Query: ") 
            st.code(message["content"].split("Required query")[1].split("required_dbs")[0], "sql")
        else:
            st.write(message["content"])

# DB Schema & KPI Documents uploader
col1, col2 = st.sidebar.columns(2, gap="medium")
with col1:
    db_schema = st.sidebar.file_uploader(label="Database Schema", type=["pdf", "txt"])
with col2:
    kpis = st.sidebar.file_uploader(label="KPIs", type=["pdf", "txt"])

# Suggesting sample codes using Cosine Similarity.
sample_codes = eval(str(open("sample_codes.txt", "r").read()))
sample_codes = [code for code in sample_codes]
suggestions = []

# Enable User's input when all credentials are uploaded.
if OPEN_AI_API_KEY and db_schema and kpis:
    if query := st.chat_input("Enter your prompt"):
        print("FROM THE TOP")
        if st.session_state.context_id != "":
            destroy_execution_context(st.secrets.databricks.cluster_id, context_id=st.session_state.context_id)
        st.session_state.messages.append({"role": "user", "content": query})
        cosine_similarity_scores(query, sample_codes, threshold=10, output_list=suggestions, matching_key="context", name_key="name")
        with st.chat_message("user"):
            st.write(query)

    if len(suggestions) > 0:
        part1, part2 = st.columns(2, gap="small")
        with part1: 
            st.subheader("Do you mean the following?")
        with part2:
            for sugg in suggestions:
                st.button(sugg["name"])

    if st.session_state.messages[-1]["role"] != "assistant":
        with st.chat_message("assistant"):
            with st.spinner("Thinking..."):
                db_schema = text_extraction(db_schema)
                kpis = text_extraction(kpis)

                dbs = eval(db_schema)
                kpis = eval(kpis)
                
                required_dbs = []  # Cosine similarity to get relevant tables from DB Schema doc.  
                cosine_similarity_scores(prompt_text=query, context_text=dbs, threshold=3, label="--> DB Schema", name_key="table_name", output_list=required_dbs)

                needed_kpis = [] # Cosine similarity to get relevant KPIs from KPIs doc.
                cosine_similarity_scores(query, kpis, 5, label="--> Required KPIs", matching_key="kpi_name", name_key="kpi_name", output_list=needed_kpis)

                for kpi in needed_kpis: # Cosine similarity to get tables required to calculate selected KPIs .
                    cosine_similarity_scores(kpi, dbs, 14, required_dbs, name_key="table_name", label="--> Tables from KPIs")

                required_db_table_names = [d.get(list(d.keys())[0]) for d in required_dbs]

                relevant_tables = [db for db in dbs if db["table_name"] in required_db_table_names]
                formatted_tables = ",\n".join([str(table) for table in relevant_tables])

                try:
                    sql, interpretation = generate(formatted_tables, needed_kpis, query, required_dbs)
                except:
                    pass
            try:
                # st.subheader("System's Interpretation: ")
                st.write(interpretation)

                with st.expander("Needed tables using Cosine Similarity"):
                    for i, db in enumerate(required_dbs):
                        st.write(i+1,". ", db["table_name"])
                
                st.subheader("Query: ")
                st.code(sql, language="sql")

                sql_query = curated_sql(sql)
                print("Curated SQL: \n", sql_query)
            except:
                    st.write("Umm. Sorry I didn't catch that I only understand data requests.")

    try:
        message = {"role": "assistant", "content": interpretation + "\n\n\nRequired query" + sql + "required_dbs"+str(required_dbs)}
        st.session_state.messages.append(message)
    except:
        st.session_state.sql = ""
        st.session_state.query = ""
        st.session_state.required_dbs = ""
    



    # Feedback button.
    if len(st.session_state.sql) > 0:
        if st.button("Mark Query as Correct"):
            print("Correct Query") 
            st.subheader("Thank you for your feedback 👍🏻. Try some more prompts.")
            
            # Converting prompt and sql to dataframe.
            current_data = pd.DataFrame([[st.session_state.query, st.session_state.sql.replace("\n", " ")]], columns=["Prompt", "Query"])
            print(current_data)
            file_name = "analytics/data/akhil/llm_training_data/training_data.csv"

            # Appending new dataframe to previous and saving to future training data.
            training_data = s3_import(file_name)

            # training_data = pd.read_csv("training_data.csv")
            df = pd.concat([training_data, current_data], axis=0)[["Prompt", "Query"]]
            s3_export(df, file_name)
            st.session_state.sql = ""
            st.session_state.query = ""

        try:
            cluster_id= st.secrets.databricks.cluster_id
            context_id=execution_context_creation()
            st.session_state.context_id = context_id
            command_id=sql_execution(sql_query, context_id, cluster_id)
            print("Waiting for result retrieval...")
            countdown = st.empty()
            for i in range(20,0,-1):
                print("{:2d}".format(i), end="\r", flush=True)
                countdown.text(f"Please wait {i} seconds for the data.")
                time.sleep(1)
            countdown.empty()
            df=data_retrieval(context_id, command_id,cluster_id)
            if df.shape[0]>0:
                print("Result Retrieved Successfully  proceeding with destruction of execution context")
            else:
                print("Still waiting for command to execute, please try after sometime, proceeding with destruction of execution context")
            
            destroy_execution_context(cluster_id, context_id)
            st.dataframe(df)
                
        except:
            print("Some error has occured please check the flow")
            destroy_execution_context(cluster_id, context_id)
            st.session_state.context_id = ""














