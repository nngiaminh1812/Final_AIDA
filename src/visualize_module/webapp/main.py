import streamlit as st
import streamlit.components.v1 as components
from dotenv import load_dotenv
import os
import pandas as pd
from chatbox import send_prompt, establish_api
from sql_query import read_sql_query

# Load environment variables from .env file
load_dotenv()

# Get the API key from environment variables
API_KEY = os.getenv('API_KEY')
print(API_KEY)
st.write(establish_api(API_KEY))
print("API Key inserted successfully!")

# Define the AI prompt with the table schema
ai_prompt = ["""
        You are an expert in Microsoft SQL Server queries!
        You are using Microsoft SQL Server, and approriate query code suitable for it.
        The Microsoft SQL Server database contains the following table:

        Table 1: jobs
    [ID] [int] IDENTITY(1,1) NOT NULL,
	[PROJECT_ID] [int] NULL,
	[SOURCE] [varchar](255) NULL,
	[TYPE] [varchar](100) NULL,
	[TITLE] [varchar](1000) NULL,
	[SERVICES] [varchar](1000) NULL,
	[SKILLS] [varchar](1000) NULL,
	[DESCRIPTION] [text] NULL,
	[DATE_POSTED] [datetime] NULL,
	[REMAINING_DAYS] [int] NULL,
	[LOCATION] [varchar](255) NULL,
	[BUDGET_MIN] [float] NULL,
	[BUDGET_MAX] [float] NULL,
	[BUDGET_CURRENCY] [varchar](10) NULL,
	[WORKING_TYPE] [varchar](100) NULL,
	[PAYMENT_TYPE] [varchar](100) NULL,
	[BID_COUNT] [int] NULL,
	[LOWEST_BID] [float] NULL,
	[AVERAGE_BID] [float] NULL,
	[HIGHEST_BID] [float] NULL,
	[DURATION] [int] NULL,
	[URL] [varchar](2083) NULL
             
        Table 2: users
    [ID] [int] IDENTITY(1,1) NOT NULL,
	[USER_ID] [int] NULL,
	[SOURCE] [varchar](255) NULL,
	[REGION] [varchar](255) NULL,
	[OVERVIEW] [text] NULL,
	[SERVICES] [varchar](1000) NULL,
	[RATINGS] [float] NULL,
	[REVIEW_COUNT] [float] NULL,
	[COMPLETION_RATE] [float] NULL,
	[REHIRE_RATE] [float] NULL,
	[EXPERIENCE] [nvarchar](255) NULL,
	[URL] [varchar](2083) NULL
             
        Specifically, column Skills contains value which is a string for example :PHP,CodeIgniter,Shopee,Tiktok Shop. If you use Skills column, you must explode it first by ","
        Please provide an English question related to this table, and I'll help you generate the corresponding Microsoft SQL Server query.
        Return a Microsoft SQL Server query with no ''' at head and tail.
        Also, the Microsoft SQL Server code should not have ``` in the beginning or end and the Microsoft SQL Server word in the output.
        Last, just return Microsoft SQL Server query only. "SELECT............." and forbid use of "LIMIT" in the end of your query.
        """]

# Function to display the Tableau dashboard
def show_dashboard():
    st.title("Dashboard")
    components.html("""
        <div class='tableauPlaceholder' id='viz1732439622221' style='position: relative; width: 100%; height: 100vh;'>
            <noscript>
                <a href='#'>
                    <img alt='Dashboard 1' src='https://public.tableau.com/static/images/Ch/ChampionLeagueStats/Dashboard1/1_rss.png' style='border: none' />
                </a>
            </noscript>
            <object class='tableauViz' style='display:none;'>
                <param name='host_url' value='https%3A%2F%2Fpublic.tableau.com%2F' />
                <param name='embed_code_version' value='3' />
                <param name='site_root' value='' />
                <param name='name' value='ChampionLeagueStats/Dashboard1' />
                <param name='tabs' value='no' />
                <param name='toolbar' value='yes' />
                <param name='static_image' value='https://public.tableau.com/static/images/Ch/ChampionLeagueStats/Dashboard1/1.png' />
                <param name='animate_transition' value='yes' />
                <param name='display_static_image' value='yes' />
                <param name='display_spinner' value='yes' />
                <param name='display_overlay' value='yes' />
                <param name='display_count' value='yes' />
                <param name='language' value='en-US' />
            </object>
        </div>
        <script type='text/javascript'>
            var divElement = document.getElementById('viz1732439622221');
            var vizElement = divElement.getElementsByTagName('object')[0];
            vizElement.style.width = '100%';
            vizElement.style.height = '100vh';
            var scriptElement = document.createElement('script');
            scriptElement.src = 'https://public.tableau.com/javascripts/api/viz_v1.js';
            vizElement.parentNode.insertBefore(scriptElement, vizElement);
        </script>
    """, height=1000)

# Function to display the introduction page
def show_introduction():
    st.title("Introduction")
    st.header("About Our Team")
    st.write("""
        We are a team of dedicated professionals with expertise in data analysis, machine learning, and software development.
    """)
    st.header("About Our Project")
    st.write("""
        Our project aims to provide insightful data visualizations and analytics to help businesses make informed decisions.
    """)

# Function to interact with the Gemini API and access the SQLite3 database
def show_conversational_ai():
    # Establish sessions state variable to save conversation history
    if 'message_history' not in st.session_state:
        st.session_state['message_history'] = []

    message_history = st.session_state['message_history']
    st.title("Ask the AI")
    st.write("I am SQL expert, give me question about query data!")
    # Send, receive and preserve conversation history
    prompt = st.chat_input("Enter a prompt here")

    if prompt:
        message_history.append({"user": prompt})  
        response = send_prompt(ai_prompt, prompt)
        # print(f"Gemini query: {response}")

        if "Sorry" in response or "not a valid SQL query" in response:
            st.write(response)
        else:
            response = response.replace("`", "")
            print(response)
            query_res = read_sql_query(response)
            message_history.append({"assistant": query_res})

        # Update session state with the new message history
        st.session_state['message_history'] = message_history

    for i, message in enumerate(message_history):
        if 'user' in message:
            st.write(f"User: {message['user']}")
        if 'assistant' in message:
            st.write("### Query result:")
            if isinstance(message['assistant'], pd.DataFrame):
                if not message['assistant'].empty:
                    st.dataframe(message['assistant'])
                else:
                    st.write("No results found.")
            else:
                st.write(f"AI: {message['assistant']}")

# Main function to control the navigation
def main():
    st.sidebar.title("Navigation")
    page = st.sidebar.radio("Go to", ["Home", "Introduction", "Ask the AI"])

    if page == "Home":
        show_dashboard()
    elif page == "Introduction":
        show_introduction()
    elif page == "Ask the AI":
        show_conversational_ai()

if __name__ == "__main__":
    main()