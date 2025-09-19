import streamlit as st # Import python packages
from snowflake.snowpark.context import get_active_session
from snowflake.cortex import Complete
from snowflake.core import Root
import pandas as pd
import json
from datetime import datetime

pd.set_option("max_colwidth",None)

### Default Values
NUM_CHUNKS = 3 # Num-chunks provided as context
slide_window = 7 # Number of last conversations to remember

# service parameters
CORTEX_SEARCH_DATABASE = "POC_POLICY"
CORTEX_SEARCH_SCHEMA = "PROCUREMENT_POLICY"
CORTEX_SEARCH_SERVICE = "POLICY_SEARCH_SERVICE"

# columns to query in the service
COLUMNS = [
    "chunk",
    "chunk_index",
    "relative_path",
    "category"
]

session = get_active_session()
root = Root(session)                         
svc = root.databases[CORTEX_SEARCH_DATABASE].schemas[CORTEX_SEARCH_SCHEMA].cortex_search_services[CORTEX_SEARCH_SERVICE]

### Chat History Storage Functions

def initialize_chat_history_table():
    """Create or alter chat history table to include user column, feedback column, hallucination flag, and review column"""
    try:
        # Create table if it doesn't exist
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS POC_POLICY.PROCUREMENT_POLICY.CHAT_HISTORY (
            INTERACTION_ID STRING DEFAULT UUID_STRING(),
            TIMESTAMP TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
            USER_QUESTION STRING,
            AI_RESPONSE STRING,
            MODEL_USED STRING,
            CATEGORY_FILTER STRING,
            SOURCE_DOCUMENTS STRING,
            RESPONSE_TIME_MS INTEGER,
            USER_NAME STRING,
            RESPONSE_QUALITY STRING,
            IS_HALLUCINATION STRING,
            REVIEW_FEEDBACK STRING
        )
        """
        session.sql(create_table_sql).collect()
        
        # Check if required columns exist, if not add them
        columns_to_check = ['USER_NAME', 'RESPONSE_QUALITY', 'IS_HALLUCINATION', 'REVIEW_FEEDBACK']
        
        for column_name in columns_to_check:
            try:
                check_column_sql = f"""
                SELECT COLUMN_NAME 
                FROM INFORMATION_SCHEMA.COLUMNS 
                WHERE TABLE_SCHEMA = 'PROCUREMENT_POLICY' 
                AND TABLE_NAME = 'CHAT_HISTORY' 
                AND COLUMN_NAME = '{column_name}'
                """
                result = session.sql(check_column_sql).collect()
                
                if len(result) == 0:
                    # Add column if it doesn't exist
                    alter_table_sql = f"""
                    ALTER TABLE POC_POLICY.PROCUREMENT_POLICY.CHAT_HISTORY 
                    ADD COLUMN {column_name} STRING
                    """
                    session.sql(alter_table_sql).collect()
                    st.sidebar.success(f"Added {column_name} column to chat history table!")
                    
            except Exception as e:
                st.sidebar.warning(f"Column check/add issue for {column_name}: {str(e)}")
        
        return True
        
    except Exception as e:
        st.sidebar.error(f"Error with chat history table: {str(e)}")
        return False

def update_review_feedback(interaction_id, review_text):
    """Update review feedback for a specific interaction"""
    try:
        # Escape single quotes in the review text
        review_text_escaped = review_text.replace("'", "''") if review_text else ""
        
        update_sql = f"""
        UPDATE POC_POLICY.PROCUREMENT_POLICY.CHAT_HISTORY 
        SET REVIEW_FEEDBACK = '{review_text_escaped}'
        WHERE INTERACTION_ID = '{interaction_id}'
        """
        
        # Execute the update
        session.sql(update_sql).collect()
        
        # Verify the update worked by checking the row
        verify_sql = f"""
        SELECT REVIEW_FEEDBACK, INTERACTION_ID
        FROM POC_POLICY.PROCUREMENT_POLICY.CHAT_HISTORY 
        WHERE INTERACTION_ID = '{interaction_id}'
        """
        verify_result = session.sql(verify_sql).collect()
        
        if verify_result:
            updated_review = verify_result[0]['REVIEW_FEEDBACK']
            return updated_review == review_text_escaped
        else:
            return False
            
    except Exception as e:
        st.error(f"Error updating review feedback: {str(e)}")
        return False

def handle_feedback_buttons():
    """Handle feedback buttons including hallucination detection and review text box"""
    if not hasattr(st.session_state, 'latest_interaction_id') or not st.session_state.latest_interaction_id:
        return
    
    interaction_id = st.session_state.latest_interaction_id
    
    # Create unique keys for this interaction's feedback, hallucination, and review status in session state
    feedback_session_key = f"feedback_status_{interaction_id}"
    hallucination_session_key = f"hallucination_status_{interaction_id}"
    review_session_key = f"review_status_{interaction_id}"
    
    # Initialize feedback, hallucination, and review status if not in session state
    if (feedback_session_key not in st.session_state or 
        hallucination_session_key not in st.session_state or 
        review_session_key not in st.session_state):
        try:
            # Check which columns exist
            check_columns_sql = """
            SELECT COLUMN_NAME 
            FROM INFORMATION_SCHEMA.COLUMNS 
            WHERE TABLE_SCHEMA = 'PROCUREMENT_POLICY' 
            AND TABLE_NAME = 'CHAT_HISTORY' 
            AND COLUMN_NAME IN ('RESPONSE_QUALITY', 'IS_HALLUCINATION', 'REVIEW_FEEDBACK')
            """
            column_result = session.sql(check_columns_sql).collect()
            existing_columns = [row['COLUMN_NAME'] for row in column_result]
            
            # Build query based on existing columns
            select_fields = ['RESPONSE_QUALITY'] 
            if 'IS_HALLUCINATION' in existing_columns:
                select_fields.append('IS_HALLUCINATION')
            if 'REVIEW_FEEDBACK' in existing_columns:
                select_fields.append('REVIEW_FEEDBACK')
            
            check_sql = f"""
            SELECT {', '.join(select_fields)}
            FROM POC_POLICY.PROCUREMENT_POLICY.CHAT_HISTORY 
            WHERE INTERACTION_ID = '{interaction_id}'
            """
            result = session.sql(check_sql).collect()
            
            if result:
                st.session_state[feedback_session_key] = result[0]['RESPONSE_QUALITY']
                # Handle IS_HALLUCINATION column
                if 'IS_HALLUCINATION' in existing_columns:
                    st.session_state[hallucination_session_key] = result[0]['IS_HALLUCINATION']
                else:
                    st.session_state[hallucination_session_key] = None
                # Handle REVIEW_FEEDBACK column
                if 'REVIEW_FEEDBACK' in existing_columns:
                    st.session_state[review_session_key] = result[0]['REVIEW_FEEDBACK']
                else:
                    st.session_state[review_session_key] = None
            else:
                st.session_state[feedback_session_key] = None
                st.session_state[hallucination_session_key] = None
                st.session_state[review_session_key] = None
                
        except Exception as e:
            st.error(f"Error checking feedback: {str(e)}")
            st.session_state[feedback_session_key] = None
            st.session_state[hallucination_session_key] = None
            st.session_state[review_session_key] = None
    
    # Ensure all keys exist in session state
    for key in [feedback_session_key, hallucination_session_key, review_session_key]:
        if key not in st.session_state:
            st.session_state[key] = None
    
    current_feedback = st.session_state[feedback_session_key]
    current_hallucination = st.session_state[hallucination_session_key]
    current_review = st.session_state[review_session_key] or ""
    
    st.caption("**Was this response helpful?**")
    col1, col2, col3, col4 = st.columns([1, 1, 1, 3])
    
    with col1:
        # Thumbs up button
        if current_feedback == 'good':
            button_label = "👍 ✓"
            button_help = "You rated this as good"
        else:
            button_label = "👍 "
            button_help = "Rate as good"
        
        if st.button(button_label, 
                    key=f"thumbs_up_{interaction_id}",
                    help=button_help):
            
            if current_feedback != 'good':
                if update_feedback(interaction_id, 'good'):
                    st.session_state[feedback_session_key] = 'good'
                    st.rerun()
                else:
                    st.error("Failed to save feedback. Please try again.")
    
    with col2:
        # Thumbs down button
        if current_feedback == 'bad':
            button_label = "👎 ✓"
            button_help = "You rated this as bad"
        else:
            button_label = "👎 "
            button_help = "Rate as bad"
        
        if st.button(button_label, 
                    key=f"thumbs_down_{interaction_id}",
                    help=button_help):
            
            if current_feedback != 'bad':
                if update_feedback(interaction_id, 'bad'):
                    st.session_state[feedback_session_key] = 'bad'
                    st.rerun()
                else:
                    st.error("Failed to save feedback. Please try again.")
    
    with col3:
        # Hallucination - Yes button
        if current_hallucination == 'Yes':
            button_label = "🚫 ✓"
            button_help = "You marked this as hallucination"
        else:
            button_label = "🚫"
            button_help = "Mark as hallucination"
        
        if st.button(button_label, 
                    key=f"hallucination_yes_{interaction_id}",
                    help=button_help):
            
            if current_hallucination != 'Yes':
                if update_hallucination_flag(interaction_id, 'Yes'):
                    st.session_state[hallucination_session_key] = 'Yes'
                    st.rerun()
                else:
                    st.error("Failed to save hallucination flag. Please try again.")
    
    with col4:
        # Review text input with submit button
        review_text = st.text_input(
            label="**Feedback**",
            value=current_review,
            key=f"review_input_{interaction_id}",
            placeholder="Enter your detailed review here...",
            help="Provide additional comments or feedback about this response",
            label_visibility="collapsed"
        )
        
        # Only show submit button if there's text and it's different from current stored review
        if review_text.strip() and review_text.strip() != (current_review or "").strip():
            if st.button("Save", 
                        key=f"save_review_{interaction_id}",
                        help="Save your review"):
                if update_review_feedback(interaction_id, review_text.strip()):
                    st.session_state[review_session_key] = review_text.strip()
                    st.success("Review saved successfully!")
                    st.rerun()
                else:
                    st.error("Failed to save review. Please try again.")

def store_chat_interaction(question, response, model_name, category, source_docs, response_time_ms):
    """Store chat interaction - UPDATED VERSION with review feedback column (with fallback)"""
    try:
        # Get current user
        current_user = get_current_user()
        
        # Create source document links
        source_links = create_source_document_links(source_docs)
        
        # Truncate fields to avoid size issues
        question_safe = question[:500] if question else ""
        response_safe = response[:1000] if response else ""
        source_links_safe = source_links[:2000] if source_links else ""
        
        # Check if REVIEW_FEEDBACK column exists
        check_review_column_sql = """
        SELECT COLUMN_NAME 
        FROM INFORMATION_SCHEMA.COLUMNS 
        WHERE TABLE_SCHEMA = 'PROCUREMENT_POLICY' 
        AND TABLE_NAME = 'CHAT_HISTORY' 
        AND COLUMN_NAME = 'REVIEW_FEEDBACK'
        """
        review_column_exists = len(session.sql(check_review_column_sql).collect()) > 0
        
        if review_column_exists:
            # Use the full insert with REVIEW_FEEDBACK column
            insert_sql = """
            INSERT INTO POC_POLICY.PROCUREMENT_POLICY.CHAT_HISTORY 
            (USER_QUESTION, AI_RESPONSE, MODEL_USED, CATEGORY_FILTER, SOURCE_DOCUMENTS, RESPONSE_TIME_MS, USER_NAME, RESPONSE_QUALITY, IS_HALLUCINATION, REVIEW_FEEDBACK)
            VALUES (?, ?, ?, ?, ?, ?, ?, NULL, NULL, NULL)
            """
        else:
            # Fallback to insert without REVIEW_FEEDBACK column
            insert_sql = """
            INSERT INTO POC_POLICY.PROCUREMENT_POLICY.CHAT_HISTORY 
            (USER_QUESTION, AI_RESPONSE, MODEL_USED, CATEGORY_FILTER, SOURCE_DOCUMENTS, RESPONSE_TIME_MS, USER_NAME, RESPONSE_QUALITY, IS_HALLUCINATION)
            VALUES (?, ?, ?, ?, ?, ?, ?, NULL, NULL)
            """
        
        session.sql(insert_sql, params=[
            question_safe,
            response_safe, 
            model_name,
            category,
            source_links_safe,
            response_time_ms,
            current_user
        ]).collect()
        
        # Get the interaction ID of the just-inserted record
        get_latest_sql = """
        SELECT INTERACTION_ID 
        FROM POC_POLICY.PROCUREMENT_POLICY.CHAT_HISTORY 
        WHERE USER_NAME = ? 
        ORDER BY TIMESTAMP DESC 
        LIMIT 1
        """
        result = session.sql(get_latest_sql, params=[current_user]).collect()
        
        if result:
            latest_interaction_id = result[0]['INTERACTION_ID']
            st.session_state.latest_interaction_id = latest_interaction_id
            return True
        else:
            return False
        
    except Exception as e:
        st.sidebar.error(f"Storage failed: {str(e)}")
        return False

def clear_feedback_state():
    """Clear feedback state when starting new conversation"""
    # Clear all feedback-related session state including review feedback
    keys_to_remove = [key for key in st.session_state.keys() if 
                     key.startswith('feedback_') or 
                     key.startswith('hallucination_') or 
                     key.startswith('review_')]
    for key in keys_to_remove:
        del st.session_state[key]

def get_current_user():
    """Get the current user from Streamlit"""
    try:
        # Use Streamlit's user to get actual user
        if hasattr(st, 'user') and st.user:
            user_info = st.user
            # Try to get email or any identifier
            if hasattr(user_info, 'email') and user_info.email:
                return user_info.email
            elif hasattr(user_info, 'name') and user_info.name:
                return user_info.name
            elif hasattr(user_info, 'id') and user_info.id:
                return user_info.id
            else:
                return str(user_info)
        else:
            # Fallback to Snowflake user
            user_sql = "SELECT CURRENT_USER()"
            result = session.sql(user_sql).collect()
            user_name = result[0][0]
            return user_name if user_name and str(user_name) != 'None' else "Anonymous_User"
            
    except Exception as e:
        return "System_User"

def create_source_document_links(source_docs):
    """Convert source documents to links"""
    if not source_docs:
        return ""
    
    links = []
    for path in source_docs:
        try:
            cmd = f"select GET_PRESIGNED_URL(@policy_documents, '{path}', 360) as URL_LINK from directory(@policy_documents)"
            df_url_link = session.sql(cmd).to_pandas()
            url_link = df_url_link._get_value(0,'URL_LINK')
            doc_name = path.split('/')[-1]
            links.append(f"{doc_name}: {url_link}")
        except:
            links.append(f"{path.split('/')[-1]}: Error getting link")
    
    return " | ".join(links)

def update_feedback(interaction_id, feedback):
    """Update feedback for a specific interaction"""
    try:
        # Simple, direct update using proper SQL escaping
        update_sql = f"""
        UPDATE POC_POLICY.PROCUREMENT_POLICY.CHAT_HISTORY 
        SET RESPONSE_QUALITY = '{feedback}'
        WHERE INTERACTION_ID = '{interaction_id}'
        """
        
        # Execute the update
        session.sql(update_sql).collect()
        
        # Verify the update worked by checking the row
        verify_sql = f"""
        SELECT RESPONSE_QUALITY, INTERACTION_ID
        FROM POC_POLICY.PROCUREMENT_POLICY.CHAT_HISTORY 
        WHERE INTERACTION_ID = '{interaction_id}'
        """
        verify_result = session.sql(verify_sql).collect()
        
        if verify_result:
            updated_feedback = verify_result[0]['RESPONSE_QUALITY']
            return updated_feedback == feedback
        else:
            return False
            
    except Exception as e:
        st.error(f"Error updating feedback: {str(e)}")
        return False

def update_hallucination_flag(interaction_id, is_hallucination):
    """Update hallucination flag for a specific interaction"""
    try:
        # Simple, direct update using proper SQL escaping
        update_sql = f"""
        UPDATE POC_POLICY.PROCUREMENT_POLICY.CHAT_HISTORY 
        SET IS_HALLUCINATION = '{is_hallucination}'
        WHERE INTERACTION_ID = '{interaction_id}'
        """
        
        # Execute the update
        session.sql(update_sql).collect()
        
        # Verify the update worked by checking the row
        verify_sql = f"""
        SELECT IS_HALLUCINATION, INTERACTION_ID
        FROM POC_POLICY.PROCUREMENT_POLICY.CHAT_HISTORY 
        WHERE INTERACTION_ID = '{interaction_id}'
        """
        verify_result = session.sql(verify_sql).collect()
        
        if verify_result:
            updated_hallucination = verify_result[0]['IS_HALLUCINATION']
            return updated_hallucination == is_hallucination
        else:
            return False
            
    except Exception as e:
        st.error(f"Error updating hallucination flag: {str(e)}")
        return False

### Config/Init Functions
     
def config_options():
    st.sidebar.title("**Chat Configuration**")
    
    st.sidebar.selectbox('**Select LLM Model**',
                         ( 'llama3.1-70b',     
                          'llama3.1-8b', 
                          'snowflake-arctic',
                         'mistral-large2'), 
                    key="model_name")

    categories = session.table('policy_docs_chunks').select('category').distinct().collect()

    cat_list = ['ALL']
    for cat in categories:
        cat_list.append(cat.CATEGORY)
            
    st.sidebar.selectbox('**Select document category**', cat_list, key = "category_value")
   
    # Add horizontal line separator
    st.sidebar.markdown("---")

    st.sidebar.title("**Chat History**")

    st.sidebar.checkbox('Remember chat history', key="use_chat_history", value = True)
    #st.sidebar.checkbox('Store conversations in database', key="store_conversations", value = True)

    st.sidebar.checkbox('Summary of previous chat', key="debug", value = True)
    
    # FIXED: Show previous conversation summary using actual response
    if st.session_state.debug:
        if hasattr(st.session_state, 'messages') and st.session_state.messages and len(st.session_state.messages) >= 2:
            # Get the last question-answer pair
            previous_question = st.session_state.messages[-2]['content']
            previous_answer = st.session_state.messages[-1]['content']  # Get the actual answer
            
            # Check if we need to generate a new summary
            summary_key = f"summary_{len(st.session_state.messages)}"
            if summary_key not in st.session_state:
                summary_prompt = f"""
                    Summarize the following answer in 1-2 sentences. Be concise and capture the key points:
                    
                    Question: {previous_question}
                    Answer: {previous_answer}
                    
                    Provide only a brief summary of the answer:
                    """
                try:
                    summary = Complete(st.session_state.model_name, summary_prompt).replace("'", "")
                    st.session_state[summary_key] = f"Question: {previous_question}\n\nSummary: {summary}"
                except Exception as e:
                    # Fallback to truncated answer if summary generation fails
                    st.session_state[summary_key] = f"Question: {previous_question}\n\nAnswer: {previous_answer[:200]}..."
            
            st.sidebar.text("Previous Chat Summary:")
            st.sidebar.caption(st.session_state[summary_key])
    
    st.sidebar.button("Start Over", key="clear_conversation", on_click=init_messages, type="primary")
    st.sidebar.markdown("---")

def init_messages():
    if st.session_state.clear_conversation or "messages" not in st.session_state:
        st.session_state.messages = []
        st.session_state.last_relative_paths = None
        st.session_state.last_chunks_data = None
        st.session_state.previous_relative_paths = None
        st.session_state.previous_question_summary = None
        st.session_state.latest_interaction_id = None
        # Clear feedback state when starting over
        clear_feedback_state()
        # Clear all summary keys
        summary_keys_to_remove = [key for key in st.session_state.keys() if key.startswith('summary_')]
        for key in summary_keys_to_remove:
            del st.session_state[key]

def get_similar_chunks_search_service(query):
    if st.session_state.category_value == "ALL":
        response = svc.search(query, COLUMNS, limit=NUM_CHUNKS)
    else: 
        filter_obj = {"@eq": {"category": st.session_state.category_value} }
        response = svc.search(query, COLUMNS, filter=filter_obj, limit=NUM_CHUNKS)
    
    return response.json()  

def get_chat_history():
    chat_history = []
    start_index = max(0, len(st.session_state.messages) - slide_window)
    for i in range (start_index , len(st.session_state.messages) -1):
         chat_history.append(st.session_state.messages[i])
    return chat_history

def summarize_question_with_history(chat_history, question):
    prompt = f"""
        Based on the chat history below and the question, generate a query that extends the question
        with the chat history provided. The query should be in natural language. 
        Answer with only the query. Do not add any explanation.
        
        <chat_history>
        {chat_history}
        </chat_history>
        <question>
        {question}
        </question>
        """
    
    summary = Complete(st.session_state.model_name, prompt)   
    summary = summary.replace("'", "")
    return summary

def create_prompt(myquestion):
    if st.session_state.use_chat_history:
        chat_history = get_chat_history()

        if chat_history != []:
            question_summary = summarize_question_with_history(chat_history, myquestion)
            prompt_context =  get_similar_chunks_search_service(question_summary)
        else:
            prompt_context = get_similar_chunks_search_service(myquestion)
    else:
        prompt_context = get_similar_chunks_search_service(myquestion)
        chat_history = ""
  
    prompt = f"""
           You are an expert chat assistant that extracts information from the CONTEXT provided
           between <context> and </context> tags.
           You offer a chat experience considering the information included in the CHAT HISTORY
           provided between <chat_history> and </chat_history> tags..
           When answering the question contained between <question> and </question> tags
           be detailed, covering all the relevant information but do not hallucinate. 
           If you don´t have the information just say so.
           Do not answer any general questions apart from those that might be based on the CONTEXT documents.
           
           Do not mention the CONTEXT used in your answer.
           Do not mention the CHAT HISTORY used in your asnwer.

           Only answer the question if you can extract it from the CONTEXT provided.
           
           <chat_history>
           {chat_history}
           </chat_history>
           <context>          
           {prompt_context}
           </context>
           <question>  
           {myquestion}
           </question>
           Answer: 
           """
    
    json_data = json.loads(prompt_context)
    relative_paths = set(item['relative_path'] for item in json_data['results'])
    return prompt, relative_paths, json_data['results']

def answer_question(myquestion):
    # Start timing for performance tracking
    start_time = datetime.now()
    
    prompt, relative_paths, chunks_data = create_prompt(myquestion)
    # response = Complete(st.session_state.model_name, prompt)
    response = Complete(
                        model=st.session_state.model_name,
                        prompt=prompt,
                        options={'guardrails': True}
                       )
    
    # Calculate response time
    end_time = datetime.now()
    response_time_ms = int((end_time - start_time).total_seconds() * 1000)
    
    # Store in database if enabled
    if st.session_state.get('store_conversations', True):
        store_chat_interaction(
            question=myquestion,
            response=response,
            model_name=st.session_state.model_name,
            category=st.session_state.category_value,
            source_docs=relative_paths,
            response_time_ms=response_time_ms
        )
    
    return response, relative_paths, chunks_data

def show_chunks_content(chunks_data):
    """Display chunks content in sidebar"""
    with st.sidebar.expander("Source Content", expanded=True):
        for i, chunk in enumerate(chunks_data):
            st.markdown(f"**Chunk {i+1}**")
            st.markdown(f"**Document** - {chunk['relative_path']}")
            st.markdown(f"*Category: {chunk.get('category', 'N/A')}*")
            st.text_area(f"Content {i+1}:", chunk['chunk'], height=100, key=f"chunk_{i}")
            st.markdown("---")


def get_document_links(relative_paths):
    """Get document links from GPT_DOCUMENT_LINKS table for given relative paths"""
    try:
        if not relative_paths:
            return {}
        
        # Convert set to list for SQL IN clause
        paths_list = list(relative_paths)
        
        # Create placeholders for the IN clause
        placeholders = ','.join(['?' for _ in paths_list])
        
        # Query to get links for documents that have them
        links_sql = f"""
        SELECT DISTINCT 
            A.RELATIVE_PATH, 
            B.DOCUMENT_NAME, 
            B.LINK 
        FROM POLICY_DOCS_CHUNKS A 
        INNER JOIN GPT_DOCUMENT_LINKS B 
            ON A.RELATIVE_PATH = SUBSTRING(B.DOCUMENT_NAME, POSITION('/' IN B.DOCUMENT_NAME) + 1)
        WHERE A.RELATIVE_PATH IN ({placeholders})
        """
        
        result = session.sql(links_sql, params=paths_list).collect()
        
        # Create dictionary mapping relative_path to link
        links_dict = {}
        for row in result:
            links_dict[row['RELATIVE_PATH']] = {
                'document_name': row['DOCUMENT_NAME'],
                'link': row['LINK']
            }
        
        return links_dict
        
    except Exception as e:
        st.sidebar.error(f"Error fetching document links: {str(e)}")
        return {}

def show_context_documentation():
    """Show context documentation with links or download URLs"""
    if not (hasattr(st.session_state, 'last_relative_paths') and st.session_state.last_relative_paths):
        return
    
    st.sidebar.title("**Context Documentation**")
    st.sidebar.markdown("(Current Chat)")
    
    # Get available links from database
    available_links = get_document_links(st.session_state.last_relative_paths)
    
    for path in st.session_state.last_relative_paths:
        doc_name = path.split('/')[-1]
        
        # Check if this document has a link available
        if path in available_links:
            # Show the database link
            link_info = available_links[path]
            st.sidebar.markdown(
                f'🔗 <a href="{link_info["link"]}" target="_blank">{doc_name}</a>', 
                unsafe_allow_html=True
            )
        else:
            # Show download link as fallback
            try:
                cmd2 = f"select GET_PRESIGNED_URL(@policy_documents, '{path}', 360) as URL_LINK from directory(@policy_documents)"
                df_url_link = session.sql(cmd2).to_pandas()
                url_link = df_url_link._get_value(0,'URL_LINK')
                
                st.sidebar.markdown(
                    f'📄 <a href="{url_link}" target="_blank">{doc_name}</a>', 
                    unsafe_allow_html=True
                )
                
            except Exception as e:
                st.sidebar.caption(f"Error loading {doc_name}: {str(e)}")


def main():
    # Initialize session state
    if "messages" not in st.session_state:
        st.session_state.messages = []
        st.session_state.last_relative_paths = None
        st.session_state.last_chunks_data = None
        st.session_state.previous_relative_paths = None
        st.session_state.previous_question_summary = None
        st.session_state.latest_interaction_id = None

    # Initialize database table on first run
    if "table_initialized" not in st.session_state:
        initialize_chat_history_table()
        st.session_state.table_initialized = True

    st.title(f"Procurement GPT")
    st.subheader(f"Procurement Data Chat Assistant")

    
    st.write("List of documents provided in context")
    docs_available = session.sql("ls @policy_documents").collect()
    list_docs = []
    for doc in docs_available:
        list_docs.append(doc["name"])
    
    df_docs = pd.DataFrame(list_docs, columns=["Document Name"])
    st.dataframe(df_docs, use_container_width=True, hide_index=True)

    config_options()
    init_messages()
     
    # Display chat messages from history on app rerun
    for message in st.session_state.messages:
        with st.chat_message(message["role"]):
            st.markdown(message["content"])
    
    # Accept user input
    if question := st.chat_input("What do you want to know from the procurement documents?"):
        # Add user message to chat history
        st.session_state.messages.append({"role": "user", "content": question})
        # Display user message in chat message container
        with st.chat_message("user"):
            st.markdown(question)
        # Display assistant response in chat message container
        with st.chat_message("assistant"):
            message_placeholder = st.empty()
    
            question = question.replace("'","")
    
            with st.spinner(f"{st.session_state.model_name} thinking..."):
                response, relative_paths, chunks_data = answer_question(question)            
                response = response.replace("'", "")
                message_placeholder.markdown(response)

                # Store previous answer's data before updating with current
                if hasattr(st.session_state, 'last_relative_paths') and st.session_state.last_relative_paths:
                    st.session_state.previous_relative_paths = st.session_state.last_relative_paths
                
                # Clear previous question summary so it gets regenerated next time
                st.session_state.previous_question_summary = None
                
                # Store data in session state for persistent buttons
                st.session_state.last_relative_paths = relative_paths
                st.session_state.last_chunks_data = chunks_data

        st.session_state.messages.append({"role": "assistant", "content": response})
    
    # ALWAYS show feedback buttons if there's a latest interaction
    # This ensures they persist even after page refreshes or new questions
    if hasattr(st.session_state, 'latest_interaction_id') and st.session_state.latest_interaction_id:
        #st.markdown("---")  # Add separator before feedback section
        handle_feedback_buttons()
    
    # # # Show direct download links for current chat documents
    # if hasattr(st.session_state, 'last_relative_paths') and st.session_state.last_relative_paths:
    #     st.sidebar.title("**Context Documentation**")
    #     st.sidebar.markdown("(Current Chat)")
    #     for path in st.session_state.last_relative_paths:
    #         try:
    #             cmd2 = f"select GET_PRESIGNED_URL(@policy_documents, '{path}', 360) as URL_LINK from directory(@policy_documents)"
    #             df_url_link = session.sql(cmd2).to_pandas()
    #             url_link = df_url_link._get_value(0,'URL_LINK')
                
    #             doc_name = path.split('/')[-1]
    #             st.sidebar.markdown(f'📄 <a href="{url_link}" target="_blank">{doc_name}</a>', unsafe_allow_html=True)
        
    #         except Exception as e:
    #             st.sidebar.caption(f"Error loading {path}")

    # Show context documentation (links or downloads)
    if hasattr(st.session_state, 'last_relative_paths') and st.session_state.last_relative_paths:
        show_context_documentation()
    
    # Show source data button
    if hasattr(st.session_state, 'last_chunks_data') and st.session_state.last_chunks_data:
        if st.sidebar.button("Show Source Data", key="show_chunks"):
            show_chunks_content(st.session_state.last_chunks_data)

if __name__ == "__main__":
    main()