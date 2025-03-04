import streamlit as st
import requests
import pandas as pd
import time
from datetime import datetime
import io
import pyperclip
import tempfile
import os
from typing import Dict, Any
from io import BytesIO


# Set page config must be the first Streamlit command
st.set_page_config(
    page_title="Email Validator",
    page_icon="✉️",
    layout="wide"
)



BACKEND_URL = os.getenv("BACKEND_URL", "https://email-verify-backend-oayu.onrender.com")

# Add custom CSS
st.markdown("""
    <style>
    .stProgress .st-bo {
        background-color: #e0f7e0;  /* Light green background */
    }
    .stProgress .st-bo > div {
        background-color: #28a745;  /* Darker green for progress */
    }
    .stMetric {
        background-color: #28a745;  /* Green background */
        color: black !important;   /* Black text */
        padding: 15px;
        border-radius: 5px;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
    }
    .stMetric:hover {
        box-shadow: 0 4px 8px rgba(0,0,0,0.2);
    }
    .stButton>button {
        width: 100%;
        margin-top: 10px;
    }
    .metric-container {
        background-color: #f0f2f6;
        padding: 10px;
        border-radius: 5px;
        margin: 5px 0;
    }
    .metric-label {
        font-weight: bold;
        color: #1f77b4;
    }
    .metric-value {
        color: #2c3e50;
    }
    </style>
""", unsafe_allow_html=True)

# Sidebar
with st.sidebar:
    st.title("📧 Email Validator Pro")
    st.markdown("---")
    st.markdown("""
    ### Features
    - Bulk email validation
    - Single email validation
    - Disposable email detection
    - Typo detection
    - Domain verification
    - Real-time progress tracking
    """)
    st.markdown("---")
    st.markdown("""
    ### Instructions
    1. Upload a CSV file with email addresses
    2. Wait for validation to complete
    3. Download results
    """)

# Main content
st.title("📧 Email Validation Dashboard")

# Create tabs for different validation methods
tab1, tab2 = st.tabs(["Bulk Validation", "Single Email"])

def process_file(file):
    """Process uploaded file and return list of emails."""
    try:
        if file.name.endswith('.csv'):
            df = pd.read_csv(file)
            if 'email' not in df.columns:
                st.error("CSV file must contain an 'email' column")
                return None
            return df['email'].tolist()
        elif file.name.endswith('.txt'):
            # Read text file and split by newlines
            content = file.getvalue().decode('utf-8')
            return [email.strip() for email in content.split('\n') if email.strip()]
        else:
            st.error("Unsupported file format. Please upload a CSV or TXT file.")
            return None
    except Exception as e:
        st.error(f"Error processing file: {str(e)}")
        return None

def display_results(result_data: Dict[str, Any], container):
    """Display validation results in a formatted table."""
    if not result_data or 'results' not in result_data:
        container.error("No results to display")
        return
    
    # Create DataFrame from results
    results = result_data['results']
    df = pd.DataFrame(results)
    
    # Add status icons
    df['Status'] = df['is_valid'].map({True: '✅ Valid', False: '❌ Invalid'})
    
    # Create a new DataFrame with the required columns
    display_df = pd.DataFrame({
        'Email': df['email'],
        'Status': df['Status'],
        'Message': df['message'],  # Add message column
        'Syntax': df['syntax_check'].map({True: '✅', False: '❌'}),
        'Format': df['format_validation'].map({True: '✅', False: '❌'}),
        'DNS': df['dns_verification'].map({True: '✅', False: '❌'}),
        'MX': df['mx_record_check'].map({True: '✅', False: '❌'}),
        'Disposable': df['disposable_email'].map({True: '❌', False: '✅'}),
        'Role-based': df['role_based_email'].map({True: '❌', False: '✅'}),
        'Typo': df['typo_detection'].map({True: '❌', False: '✅'}),
        'Bounce Risk': df['bounce_risk']
    })
    
    # Display the table
    container.dataframe(display_df, use_container_width=True)

with tab1:
    st.header("Bulk Email Validation")
    st.write("Upload a CSV file with an 'email' column or a TXT file with one email per line.")
    
    uploaded_file = st.file_uploader("Choose a file", type=['csv', 'txt'])
    
    if uploaded_file is not None:
        # Check file size
        file_size = len(uploaded_file.getvalue()) / (1024 * 1024)  # Convert to MB
        if file_size > 10:
            st.warning("⚠️ File size exceeds 10MB. Processing may take longer.")
        
        if st.button("🔍 Validate Emails"):
            try:
                with st.spinner("Processing..."):
                    # Upload the file directly
                    files = {'file': (uploaded_file.name, uploaded_file.getvalue(), uploaded_file.type)}
                    response = requests.post(f"{BACKEND_URL}/upload/", files=files)
                    
                    if response.status_code == 200:
                        task_id = response.json()['task_id']
                        
                        # Create containers for progress and results
                        progress_container = st.empty()
                        status_container = st.empty()
                        results_container = st.empty()
                        
                        # Poll for results
                        max_retries = 60  # 30 seconds with 0.5s sleep
                        retry_count = 0
                        
                        while retry_count < max_retries:
                            result_response = requests.get(f"{BACKEND_URL}/results/{task_id}")
                            
                            if result_response.status_code == 200:
                                result_data = result_response.json()
                                
                                if result_data.get('status') == 'completed':
                                    # Display metrics cards
                                    metrics_col1, metrics_col2, metrics_col3, metrics_col4 = st.columns(4)
                                    
                                    with metrics_col1:
                                        st.metric(
                                            "Valid Emails",
                                            result_data.get('valid_count', 0),
                                            help="Number of valid email addresses"
                                        )
                                    
                                    with metrics_col2:
                                        st.metric(
                                            "Invalid Emails",
                                            result_data.get('invalid_count', 0),
                                            help="Number of invalid email addresses"
                                        )
                                    
                                    with metrics_col3:
                                        st.metric(
                                            "Total Emails",
                                            result_data.get('total_count', 0),
                                            help="Total number of emails processed"
                                        )
                                    
                                    with metrics_col4:
                                        st.metric(
                                            "Processing Time",
                                            f"{result_data.get('processing_time', 0):.2f}s",
                                            help="Time taken to process all emails"
                                        )
                                    
                                    # Add spacing
                                    st.markdown("---")
                                    
                                    # Display results table
                                    if 'results' in result_data and result_data['results']:
                                        display_results(result_data, results_container)
                                    else:
                                        st.error("No results data available")
                                    
                                    # Add download and refresh buttons
                                    col1, col2, col3 = st.columns(3)
                                    
                                    with col1:
                                        # Convert results to DataFrame
                                        df = pd.DataFrame(result_data.get('results', []))
                                        if not df.empty:
                                            # Create display DataFrame
                                            display_df = pd.DataFrame({
                                                'Email': df['email'],
                                                'Status': df['is_valid'].map({True: 'Valid', False: 'Invalid'}),
                                                'Message': df['message'],  # Add message column
                                                'Syntax': df['syntax_check'].map({True: 'Yes', False: 'No'}),
                                                'Format': df['format_validation'].map({True: 'Yes', False: 'No'}),
                                                'DNS': df['dns_verification'].map({True: 'Yes', False: 'No'}),
                                                'MX': df['mx_record_check'].map({True: 'Yes', False: 'No'}),
                                                'Disposable': df['disposable_email'].map({True: 'Yes', False: 'No'}),
                                                'Role-based': df['role_based_email'].map({True: 'Yes', False: 'No'}),
                                                'Typo': df['typo_detection'].map({True: 'Yes', False: 'No'}),
                                                'Bounce Risk': df['bounce_risk']
                                            })
                                            
                                            # Create CSV
                                            csv = display_df.to_csv(index=False)
                                            st.download_button(
                                                label="📥 Download CSV",
                                                data=csv,
                                                file_name="email_validation_results.csv",
                                                mime="text/csv"
                                            )
                                        else:
                                            st.warning("No results to download")
                                    
                                    with col2:
                                        if not df.empty:
                                            # Create Excel file
                                            excel_buffer = BytesIO()
                                            with pd.ExcelWriter(excel_buffer, engine='xlsxwriter') as writer:
                                                display_df.to_excel(writer, index=False, sheet_name='Validation Results')
                                                
                                                # Get the workbook and the worksheet
                                                workbook = writer.book
                                                worksheet = writer.sheets['Validation Results']
                                                
                                                # Add formats
                                                header_format = workbook.add_format({
                                                    'bold': True,
                                                    'bg_color': '#4CAF50',
                                                    'font_color': 'white',
                                                    'border': 1
                                                })
                                                
                                                # Format the header
                                                for col_num, value in enumerate(display_df.columns.values):
                                                    worksheet.write(0, col_num, value, header_format)
                                                
                                                # Auto-adjust column widths
                                                for idx, col in enumerate(display_df):
                                                    max_length = max(
                                                        display_df[col].astype(str).apply(len).max(),
                                                        len(str(col))
                                                    )
                                                    worksheet.set_column(idx, idx, max_length + 2)
                                            
                                            excel_buffer.seek(0)
                                            st.download_button(
                                                label="📊 Download Excel",
                                                data=excel_buffer,
                                                file_name="email_validation_results.xlsx",
                                                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                                            )
                                        else:
                                            st.warning("No results to download")
                                    
                                    with col3:
                                        if st.button("🔄 Refresh Results"):
                                            st.rerun()
                                    
                                    break
                                elif result_data.get('status') == 'processing':
                                    # Update progress
                                    current = result_data.get('current', 0)
                                    total = result_data.get('total', 0)
                                    progress = current / total if total > 0 else 0
                                    
                                    # progress_container.progress(progress)
                                    status_container.text(f"Processing emails...")
                                    
                                    time.sleep(0.5)
                                    retry_count += 1
                                elif result_data.get('status') == 'failed':
                                    st.error(f"Validation failed: {result_data.get('error', 'Unknown error')}")
                                    break
                            else:
                                st.error(f"Failed to get validation results: {result_response.text}")
                                break
                        
                        if retry_count >= max_retries:
                            st.error("Validation timed out. Please try again.")
                    else:
                        st.error("Failed to start validation")
            except Exception as e:
                st.error(f"An error occurred: {str(e)}")

with tab2:
    st.subheader("Single Email Validation")
    
    # Create a form for the email input
    with st.form("email_validation_form"):
        email = st.text_input("Enter email address:", key="single_email")
        
        # Create columns for buttons
        col1, col2, col3 = st.columns([1, 1, 1])
        
        with col1:
            # Submit button
            submit_button = st.form_submit_button("🔍 Validate")
        
        with col2:
            # Clear button
            if st.form_submit_button("🗑️ Clear"):
                # Clear the form and results
                st.session_state.pop('single_email', None)
                st.session_state.pop('single_result', None)
                st.rerun()
    
    # Handle validation when form is submitted
    if submit_button and email:
        with st.spinner("Validating email..."):
            try:
                response = requests.post(
                    f"{BACKEND_URL}/validate/",
                    json={"email": email}
                )
                if response.status_code == 200:
                    result = response.json()
                    st.session_state.single_result = result
                    st.success("Validation complete!")
                else:
                    st.error(f"Error: {response.text}")
            except Exception as e:
                st.error(f"Error: {str(e)}")
    elif submit_button and not email:
        st.warning("Please enter an email address")
    
    # Display results if available
    if 'single_result' in st.session_state and st.session_state.single_result:
        result = st.session_state.single_result
        st.markdown("### Results")
        
        # Display validation status
        status_color = "green" if result['is_valid'] else "red"
        st.markdown(f"**Status:** <span style='color: {status_color}'>{'Valid' if result['is_valid'] else 'Invalid'}</span>", unsafe_allow_html=True)
        
        # Display validation details
        st.markdown("#### Validation Details")
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("**Basic Checks:**")
            st.markdown(f"- Syntax Check: {'✅' if result['syntax_check'] else '❌'}")
            st.markdown(f"- Format Validation: {'✅' if result['format_validation'] else '❌'}")
            st.markdown(f"- DNS Verification: {'✅' if result['dns_verification'] else '❌'}")
            st.markdown(f"- MX Record Check: {'✅' if result['mx_record_check'] else '❌'}")
        
        with col2:
            st.markdown("**Additional Checks:**")
            st.markdown(f"- Disposable Email: {'❌' if result['disposable_email'] else '✅'}")
            st.markdown(f"- Role-based Email: {'❌' if result['role_based_email'] else '✅'}")
            st.markdown(f"- Typo Detection: {'❌' if result['typo_detection'] else '✅'}")
            st.markdown(f"- Bounce Risk: {'❌' if result['bounce_risk'] else '✅'}")
        
        # Display message if any
        if result.get('message'):
            st.markdown(f"**Message:** {result['message']}")
        
        # Copy to clipboard button
        if st.button("📋 Copy Results to Clipboard"):
            result_text = f"""
Email: {email}
Status: {'Valid' if result['is_valid'] else 'Invalid'}
Message: {result.get('message', 'N/A')}

Validation Details:
- Syntax Check: {'✅' if result['syntax_check'] else '❌'}
- Format Validation: {'✅' if result['format_validation'] else '❌'}
- DNS Verification: {'✅' if result['dns_verification'] else '❌'}
- MX Record Check: {'✅' if result['mx_record_check'] else '❌'}
- Disposable Email: {'❌' if result['disposable_email'] else '✅'}
- Role-based Email: {'❌' if result['role_based_email'] else '✅'}
- Typo Detection: {'❌' if result['typo_detection'] else '✅'}
- Bounce Risk: {'❌' if result['bounce_risk'] else '✅'}
"""
            pyperclip.copy(result_text)
            st.success("Results copied to clipboard!")
