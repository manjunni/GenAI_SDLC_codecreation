import streamlit as st
import base64
import os
import time
import re
from datetime import datetime
import shutil
import stat
import time
#fix RuntimeError: Tried to instantiate class '__path__._path', but it does not exist! Ensure that it is registered via torch::class_ error for streamlit
import torch
torch.classes.__path__ = [] 
from document_loader import load_split_pdf,load_excel,fetch_file,load_docx
from RAG_azure import create_vectorstore,search_vectorstore,delete_container
from pipeline_code_creation_azure import create_pipeline_code
from azure.storage.blob import BlobServiceClient
from langchain_community.document_loaders import AzureBlobStorageContainerLoader


st.set_page_config(
    page_title="Code Creation",
    page_icon="🔄",
    layout="wide",
    initial_sidebar_state="collapsed",
    menu_items=None
)

# Function to load and encode the image to base64
def get_img_as_base64(file_path):
    if not os.path.exists(file_path):
        # If the file doesn't exist, create a placeholder for now
        return None
    with open(file_path, "rb") as img_file:
        return base64.b64encode(img_file.read()).decode()

# Define CSS styles
def apply_custom_styles():
    css = """
    <style>
        /* Reset all padding and margins */
        .block-container {
            padding: 0 !important;
            margin: 0 !important;
            max-width: 100% !important;
        }
        
        /* Main theme styling */
        .main {
            background-color: #191627;  /* Darker purple background */
            color: white;
            padding: 0 !important;
            margin: 0 !important;
            font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif;
        }
        
        /* Content wrapper styling - add padding to all main content */
        .main .block-container {
            padding: 0 !important;
            margin: 0 !important;
            max-width: 100% !important;
        }
        
        /* Keep all content except header properly padded */
        .main .element-container:not(:has(.header-container)):not(:has(.back-button)) {
            padding-left: 40px;
            padding-right: 40px;
        }
        
        /* Hide Streamlit UI elements */
        #MainMenu, footer, header {
            visibility: hidden !important;
        }
        
        /* Hide Streamlit branding */
        section[data-testid="stSidebar"] {
            display: none !important;
        }
        
        div[data-testid="stDecoration"],
        div[data-testid="stStatusWidget"],
        div[data-testid="stToolbar"],
        div[data-testid="stHeader"],
        div[data-testid="stSidebarCollapsedControl"] {
            display: none !important;
        }
        
        /* Remove default padding from app container */
        div.stApp {
            margin: 0 !important;
            padding: 0 !important;
            border: none !important;
        }
        
        /* Remove padding from sections */
        div[data-testid="stAppViewContainer"] > section > div {
            padding: 0 !important;
        }
        
        /* Header styling */
        .header-container {
            display: flex;
            align-items: center;
            padding: 15px 25px;
            background-color: #130f24;
            border-bottom: 1px solid #3d2b70;
            width: 100%;
            box-sizing: border-box;
            margin-top: -1rem;
            box-shadow: 0 4px 15px rgba(0,0,0,0.3);
            position: relative;
            z-index: 100;
        }
        
        .logo-container {
            display: flex;
            align-items: center;
        }
        
        .logo {
            height: 40px;
            margin-right: 18px;
            filter: drop-shadow(0 2px 3px rgba(0,0,0,0.2));
            transition: transform 0.3s ease;
        }
        
        .logo:hover {
            transform: scale(1.05);
        }
        
        .app-name {
            font-size: 28px;
            font-weight: 700;
            color: white;
            text-shadow: 0 2px 4px rgba(0,0,0,0.25);
            letter-spacing: 0.5px;
            background: linear-gradient(45deg, #ffffff, #c4b6ff);
            -webkit-background-clip: text;
            background-clip: text;
            -webkit-text-fill-color: transparent;
        }
        
        /* Back button styling */
        .back-button {
            background-color: #3d2b70;
            color: white;
            border: none;
            border-radius: 6px;
            padding: 8px 16px;
            font-size: 14px;
            font-weight: 500;
            cursor: pointer;
            margin: 15px 0 15px 40px;
            display: inline-flex;
            align-items: center;
            transition: all 0.3s ease;
            box-shadow: 0 2px 5px rgba(0,0,0,0.2);
            text-transform: uppercase;
            letter-spacing: 0.5px;
        }
        
        .back-button:hover {
            background-color: #5e44a8;
            transform: translateY(-2px);
            box-shadow: 0 4px 8px rgba(0,0,0,0.3);
        }
        
        .back-icon {
            margin-right: 8px;
            font-weight: bold;
        }
        
        /* Content container styling */
        .content-container {
            padding: 25px 80px;
            box-sizing: border-box;
            max-width: 1400px;
            margin: 0 auto;
        }
        
        /* Add subtle animations */
        @keyframes fadeIn {
            from { opacity: 0; transform: translateY(20px); }
            to { opacity: 1; transform: translateY(0); }
        }
        
        .fadeIn {
            animation: fadeIn 0.5s ease forwards;
        }
        
        /* Card hover effects */
        .hover-lift {
            transition: transform 0.3s ease, box-shadow 0.3s ease;
        }
        
        .hover-lift:hover {
            transform: translateY(-5px);
            box-shadow: 0 8px 15px rgba(0,0,0,0.3);
        }
        
        /* Code syntax highlighting enhancement */
        .language-python {
            font-family: 'Roboto Mono', monospace !important;
        }
        
        /* Tooltip styling */
        div[data-baseweb="tooltip"] {
            background-color: #23194c !important;
            border: 1px solid #3d2b70 !important;
            color: #e0e0e0 !important;
            border-radius: 4px !important;
            padding: 8px 12px !important;
            font-size: 13px !important;
        }
        
        /* Code input/output panel styling */
        .code-panel {
            background-color: #23194c;
            border-radius: 8px;
            padding: 22px;
            margin: 0 20px 20px 20px;
            box-shadow: 0 4px 10px rgba(0,0,0,0.3);
            border: 1px solid #3d2b70;
            transition: box-shadow 0.3s ease;
        }
        
        .code-panel:hover {
            box-shadow: 0 6px 14px rgba(0,0,0,0.4);
        }
        
        .code-panel-title {
            color: #c4b6ff;
            font-weight: bold;
            margin-bottom: 15px;
            font-size: 18px;
            border-bottom: 1px solid #3d2b70;
            padding-bottom: 8px;
            letter-spacing: 0.5px;
        }
        
        /* Status box styling */
        .status-box {
            background-color: #23194c;
            border-radius: 8px;
            padding: 22px;
            margin: 0 20px 20px 20px;
            box-shadow: 0 4px 10px rgba(0,0,0,0.3);
            border: 1px solid #3d2b70;
            position: relative;
            overflow: hidden;
        }
        
        .status-box::before {
            content: '';
            position: absolute;
            top: 0;
            left: 0;
            width: 100%;
            height: 4px;
            background: linear-gradient(90deg, #5e44a8, #9370DB, #5e44a8);
            background-size: 200% 100%;
            animation: gradientAnimation 2s linear infinite;
        }
        
        @keyframes gradientAnimation {
            0% {background-position: 0% 0%;}
            100% {background-position: 200% 0%;}
        }
        
        /* Custom button styling */
        .custom-button {
            background: linear-gradient(135deg, #2979ff, #1565c0);
            color: white;
            border-radius: 6px;
            padding: 12px 20px;
            border: none;
            cursor: pointer;
            transition: all 0.3s ease;
            font-weight: bold;
            box-shadow: 0 4px 6px rgba(0,0,0,0.15);
            text-transform: uppercase;
            letter-spacing: 0.8px;
            font-size: 14px;
        }
        
        .custom-button:hover {
            background: linear-gradient(135deg, #1976d2, #0d47a1);
            transform: translateY(-3px);
            box-shadow: 0 6px 10px rgba(0,0,0,0.25);
        }
        
        /* Primary button for important actions */
        .stButton button[kind="primary"] {
            background: linear-gradient(135deg, #2979ff, #1565c0) !important;
            color: white !important;
            padding: 12px 24px !important;
            font-weight: bold !important;
            letter-spacing: 1px !important;
            border-radius: 6px !important;
            border: none !important;
            box-shadow: 0 4px 8px rgba(0,0,0,0.2) !important;
            transition: all 0.3s ease !important;
            text-transform: uppercase !important;
        }
        
        .stButton button[kind="primary"]:hover {
            transform: translateY(-2px) !important;
            box-shadow: 0 6px 12px rgba(0,0,0,0.3) !important;
            background: linear-gradient(135deg, #1976d2, #0d47a1) !important;
        }
        
        /* Text area styling for the code inputs/outputs */
        .stTextArea textarea {
            background-color: #2a1b4a !important;
            color: #e0e0e0 !important;
            border: 1px solid #3d2b70 !important;
            border-radius: 8px !important;
            padding: 15px !important;
            font-family: 'Roboto Mono', monospace !important;
            font-size: 14px !important;
            line-height: 1.6 !important;
            transition: border-color 0.3s ease !important;
        }
        
        .stTextArea textarea:focus {
            border-color: #6e56cf !important;
            box-shadow: 0 0 0 2px rgba(110, 86, 207, 0.25) !important;
        }
        
        /* Input label styling */
        .stTextArea label, .stSelectbox label {
            font-weight: 600 !important;
            color: #c4b6ff !important;
            font-size: 15px !important;
            margin-bottom: 8px !important;
            letter-spacing: 0.5px !important;
        }
        
        /* Selectbox styling */
        .stSelectbox div[data-baseweb="select"] {
            background-color: #2a1b4a !important;
            border: 1px solid #3d2b70 !important;
            border-radius: 8px !important;
            transition: border-color 0.3s ease !important;
        }
        
        .stSelectbox div[data-baseweb="select"]:hover {
            border-color: #6e56cf !important;
        }
        
        /* Expander styling */
        .streamlit-expanderHeader {
            background-color: #2a1b4a !important;
            border-radius: 8px !important;
            color: #c4b6ff !important;
            font-weight: 600 !important;
            padding: 12px 18px !important;
            border: 1px solid #3d2b70 !important;
            transition: all 0.3s ease !important;
            margin-bottom: 2px !important;
        }
        
        .streamlit-expanderHeader:hover {
            background-color: #342159 !important;
            border-color: #6e56cf !important;
        }
        
        .streamlit-expanderContent {
            background-color: #23194c !important;
            border: 1px solid #3d2b70 !important;
            border-top: none !important;
            border-radius: 0 0 8px 8px !important;
            padding: 18px !important;
            animation: fadeIn 0.3s ease-in-out !important;
        }
        
        /* Progress bar styling */
        div.stProgress > div > div {
            background-color: #5e44a8 !important;
            background-image: linear-gradient(45deg, 
                #5e44a8 25%, 
                #6e56cf 25%, 
                #6e56cf 50%, 
                #5e44a8 50%, 
                #5e44a8 75%, 
                #6e56cf 75%, 
                #6e56cf 100%);
            background-size: 20px 20px !important;
            animation: progress-bar-stripes 1s linear infinite !important;
        }
        
        @keyframes progress-bar-stripes {
            from { background-position: 20px 0; }
            to { background-position: 0 0; }
        }

        div[data-testid="stElementContainer"]:nth-child(2){
            padding: 0 !important;
        }

        .stMarkdown:nth-child(1) {
            padding: 0 !important;
        }

        stElementContainer,
        .stMarkdown {
            padding: 0px 20px;
        }
        
        /* Streamlit component containers */
        div[data-testid="stForm"], 
        div[data-testid="stElementContainer"],
        div[data-testid="stExpander"],
        section[data-testid="stFileUploadDropzone"] {
            padding-left: 40px;
            padding-right: 40px;
        }
        
        /* Custom styling for streamlit components within our app */
        .streamlit-container {
            background-color: #23194c;
            padding: 16px;
            border-radius: 8px;
            margin: 0 20px 15px 20px;
            border: 1px solid #3d2b70;
        }
        
        /* Download button styling */
        .stDownloadButton button {
            background-color: #43a047 !important;
            color: white !important;
            border-radius: 6px !important;
            padding: 10px 16px !important;
            font-weight: 600 !important;
            transition: all 0.3s ease !important;
            border: none !important;
            box-shadow: 0 2px 5px rgba(0,0,0,0.15) !important;
            letter-spacing: 0.5px !important;
        }
        
        .stDownloadButton button:hover {
            background-color: #2e7d32 !important;
            box-shadow: 0 4px 8px rgba(0,0,0,0.2) !important;
            transform: translateY(-2px) !important;
        }
    </style>
    """
    st.markdown(css, unsafe_allow_html=True)

# Apply the custom styles
apply_custom_styles()

# Create header
def create_header():
    header_html = """
    <div class="header-container">
        <div class="logo-container">
            <img src="https://www.capgemini.com/wp-content/themes/capgemini-komposite/assets/images/logo.svg" class="logo" alt="Capgemini Logo">
            <div class="app-name">Code Creation <span style="font-size: 16px; opacity: 0.7; font-weight: normal; vertical-align: super;">BETA</span></div>
        </div>
    </div>
    """
    st.markdown(header_html, unsafe_allow_html=True)

# Back button to return to the home page
def create_back_button():
    back_button_html = """
    <a href="/" class="back-button" target="_self">
        <span class="back-icon">←</span> Back to Home
    </a>
    """
    st.markdown(back_button_html, unsafe_allow_html=True)

# Function to perform actual code creation
def print_message_doc_upload():
     st.chat_message("assistant").write("Domain document uploaded successfully")
def print_message():
            st.chat_message("assistant").write("Code downloaded")

def main():
    create_header()
    #create_back_button()
    
    # Title and description with improved styling
    st.markdown("""
    <div style="padding: 0 40px;">
        <h2 style='font-size: 32px; margin-bottom: 10px; font-weight: 700; color: #c4b6ff;'>
            <span style='margin-right: 12px;'>⚙️</span>Code Creation Tool
        </h2>
        <p style='font-size: 18px; line-height: 1.6; margin-bottom: 25px; color: #a79eca; max-width: 1600px;'>
            Enter source to target mapping document and optionally add any domain specific documentation.
            The system will use the mapping document to create data pipeline code in python/pyspark. It will refactor the code to follow data engineering best practices and will also generate code for any business logic that is covered in the domain document, if provided.
        </p>
    </div>
    """, unsafe_allow_html=True)
    
    # Input section with enhanced styling and hover effect
    st.markdown("""
    <div style="padding: 0 40px;">
        <div class="code-panel-title">
            <span style="font-size: 20px; margin-right: 8px;">📝</span>
            Input Requirements
        </div>
    </div>
    """, unsafe_allow_html=True)
    
    # Add file uploader option for code files
    mappingfile = st.file_uploader("Upload a mapping sheet(xlsx,docx,pdf)",type=["xlsx", "docx", "pdf"], label_visibility="collapsed",key="mappingfile")
    col1,col2 = st.columns([1,1])
    with col1:
         source_path = st.text_input("Source Path",help="Enter the source path for data on databricks",key="source_path")
    with col2:
        target_path = st.text_input("Target Path",help="Enter the target path for data on databricks",key="target_path")
    use_domain_doc = st.checkbox("Use domain document for code generation",value=False,key="use_domain_doc")
    domain_doc = st.file_uploader("Upload a domain document(xlsx,docx,pdf)",type=["xlsx", "docx", "pdf"],disabled=False if use_domain_doc else True,label_visibility="collapsed",key="domain_doc")
    upload_file = st.button("domain file upload",help="domain file upload",disabled=False if domain_doc else True,key="upload_file")
    
    
    # Advanced settings with more options
    with st.expander("Settings"):
        col1, col2 = st.columns([1,1])
        with col1:
             max_attempts = st.slider(
                    "Maximum Fix Attempts",
                    min_value=1,
                    max_value=5,
                    value=3,
                    help="Maximum number of attempts to fix any errors in the generated code"
                )
        with col2:
             target_language = st.selectbox(
            "Target Language",
            options=["Python", "PySpark"],
            index=0,
            help="Select the language for the generated code"
                )
    create_code = st.button("create code",help="create code from mapping sheet",disabled=False if mappingfile else True)

    if upload_file and domain_doc:
       with st.spinner("Uploading file..."):
          
            progress=st.progress(0)
            for i in range(0,70,10):
                time.sleep(0.1)
                progress.progress(i+10)
            file_path = fetch_file(domain_doc)
            print(file_path)
            if domain_doc.name.endswith(".pdf"):
                  docs,all_splits = load_split_pdf(file_path,True)
            if domain_doc.name.endswith(".xlsx"):
                  docs,all_splits = load_excel(file_path,True)
            if domain_doc.name.endswith(".docx"):
                  docs,all_splits = load_docx(file_path,True)
            print("document loaded")
            print(docs[0].metadata['source'])
            
            #remove the temp directory
            temp_dir = "temp"
            def remove_readonly(func, path, excinfo):
                  os.chmod(path, stat.S_IWRITE)
                  func(path)
            shutil.rmtree(temp_dir,onerror=remove_readonly)
            #st.write("sheet uploaded successfully")
            cosmos_container = "domain_doc"
            #deleting existing containers and creating new container
            delete_container()
            vector_store = create_vectorstore(all_splits,cosmos_container)
            print("added document to vector store")
                      
         #   _=vector_store.add_documents(all_splits)
         #   vector_store.save_local("faiss_index")
            print("document added to vector store")
            progress.progress(100)
            print_message_doc_upload()

                           

    if mappingfile and create_code:
        print(use_domain_doc)
        if use_domain_doc and not domain_doc:
            st.warning("Please upload a domain document to proceed with code generation.")
        else:
            with st.spinner("Code Generation in progress..."):
                output= st.empty()
                progress=st.progress(0)
                for i in range(0,70,10):
                    output.info(f"Generating and executing code... ({i+10}%)")
                    time.sleep(0.5)
                    progress.progress(i+10)
                file_path=fetch_file(mappingfile)
                if mappingfile.name.endswith(".pdf"):
                    docs = load_split_pdf(file_path,False)
                    print(docs)
                
                elif mappingfile.name.endswith(".xlsx"):
                
                    docs = load_excel(file_path,False)
                    print(docs)
                elif mappingfile.name.endswith(".docx"):
                    docs = load_docx(file_path,False)
                    print(docs)
                                
                print("folder created and sheet saved")
                temp_dir = "temp"
                def remove_readonly(func, path, excinfo):
                    os.chmod(path, stat.S_IWRITE)
                    func(path)
                shutil.rmtree(temp_dir,onerror=remove_readonly)
             
                #get data engg principles document from azure blob storage 
                # dataengg_docs = load_from_azureblob()
                # print("data engg principles",dataengg_docs)
                
                try:
                     
                    result = create_pipeline_code(docs,use_domain_doc,max_attempts,source_path,target_path,target_language)
                    print(result)          
                    
                    result_code = result['generation'].code
                    result_message = result['messages']
                    result_error = result['error']
                    print("result error",result_error)
                    
                    result_iterations = result['iteration']
                    result_common_functions = result['common_functions']
                    result_connection_details = result['connection_details']
                    progress.progress(100)
                    output.success("Code generation completed successfully!")
                    with output.container():
                        tab1,tab2 = st.tabs(["Generated Code", "Execution results"])
                        with tab1:
                            st.markdown("""
                            <div class="code-panel-title">
                                <span style="font-size: 20px; margin-right: 8px;">✅</span>
                                Generated Code
                            </div>
                            """, unsafe_allow_html=True)
                            col_code, col_message = st.columns([2, 1])
                            with col_code:         
                                st.code(result_code, language='python')
                                col1, col2 = st.columns([1, 1])
                                
                                with col1:
                                        st.download_button(
                                        label="Download code",
                                        data=result_code,
                                        file_name="script.py",
                                        mime="text/plain",
                                        help="download the code",
                                        key="download-code",
                                        on_click=print_message
                                )
                                        
                                with col2:
                                        if st.button("New Code Generation", use_container_width=True, key="new_code_gen"):
                                            # Reset session state for all input widgets
                                            for key in ["mappingfile", "source_path", "target_path", "use_domain_doc", "domain_doc", "upload_file"]:
                                                if key in st.session_state:
                                                    del st.session_state[key]
                                            st.rerun()
                            with col_message:
                                st.write("Common functions : ")
                                st.code(result_common_functions, language='python')
                                st.write("Connections needed : ")   
                                st.code(result_connection_details, language='python')  
                        with tab2:
                            
                            if result_error=='yes':
                                cleaned_message = re.sub(r'\x1b\[[0-9;]*m', '', result_message[-1][1])
                                st.write("❌ Code generation encountered some issues")
                                
                                st.code(cleaned_message,language='text')
                                st.write("No. of iterations:", result_iterations)
                                

                            else:
                                st.write("✅ Code generated and executed without errors in ", result_iterations,"/",max_attempts, "iterations")
                except Exception as e:
                    st.error(f"An error occurred during code generation: {e}")



if __name__ == "__main__":
    main()
