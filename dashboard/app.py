import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
import matplotlib.pyplot as plt
from datetime import datetime
import os
import glob
import json

# Set page configuration
st.set_page_config(
    page_title="ETL Pipeline Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Function to make objects JSON serializable
def json_serialize_df_column(obj):
    if pd.api.types.is_dtype_equal(obj, 'object'):
        return str(obj)
    return str(obj)

# Function to load data files
@st.cache_data(ttl=300)
def get_data_files():
    raw_files = glob.glob('./data/raw/*.csv')
    processed_files = glob.glob('./data/processed/processed_data_*.csv')
    report_files = glob.glob('./data/processed/analysis_report_*.txt')
    
    # Sort files by creation time, newest first
    raw_files = sorted(raw_files, key=os.path.getctime, reverse=True) if raw_files else []
    processed_files = sorted(processed_files, key=os.path.getctime, reverse=True) if processed_files else []
    report_files = sorted(report_files, key=os.path.getctime, reverse=True) if report_files else []
    
    return raw_files, processed_files, report_files

# Caching functions for performance
@st.cache_data(ttl=300)
def calculate_age_metrics(df, birth_date_col):
    today = pd.to_datetime('today')
    df = df.copy()
    df['age'] = today.year - pd.to_datetime(df[birth_date_col]).dt.year
    birth_dates = pd.to_datetime(df[birth_date_col])
    df.loc[((today.month < birth_dates.dt.month) | 
           ((today.month == birth_dates.dt.month) & (today.day < birth_dates.dt.day))), 'age'] -= 1
    return df['age']

@st.cache_data(ttl=300)
def get_email_domain_stats(df):
    df['email_domain'] = df['email'].str.extract(r'@(.+)$')
    return df['email_domain'].value_counts().head(10)

@st.cache_data(ttl=300)
def get_area_code_stats(df):
    # Remove extensions and standardize separators
    df['cleaned_phone'] = df['phone'].str.extract(r'^([^x]*)')  # Remove everything after 'x'
    df['cleaned_phone'] = df['cleaned_phone'].str.replace(r'[^\d]', '')  # Remove non-digits
    # Extract first 3 digits as area code
    df['area_code'] = df['cleaned_phone'].str[:3]
    return df['area_code'].value_counts().head(10)

# Function to create a metric card
def metric_card(title, value, delta=None, suffix=""):
    if delta:
        st.metric(label=title, value=f"{value}{suffix}", delta=delta)
    else:
        st.metric(label=title, value=f"{value}{suffix}")

# Application title and description
st.title("📊 ETL Pipeline Dashboard")
st.markdown("""
This dashboard provides insights into the data processed by the Airflow ETL pipeline.
""")

# Load data files
raw_files, processed_files, report_files = get_data_files()

# Check if any files were found
if not (raw_files or processed_files):
    st.warning("No data files found. Please run the ETL pipeline first.")
    st.stop()

# File selection in sidebar
st.sidebar.header("Data Selection")
st.sidebar.markdown("Select a file to analyze:")

# File type selector
file_type = st.sidebar.radio("Select Data Type", ["Processed Data", "Raw Data"])

# File selection logic
if file_type == "Processed Data":
    if processed_files:
        selected_file = st.sidebar.selectbox(
            "Select Processed File",
            options=[os.path.basename(f) for f in processed_files],
            index=0
        )
        file_path = next((f for f in processed_files if os.path.basename(f) == selected_file), None)
    else:
        st.error("No processed data files found.")
        st.stop()
else:  # Raw Data
    if raw_files:
        selected_file = st.sidebar.selectbox(
            "Select Raw File",
            options=[os.path.basename(f) for f in raw_files],
            index=0
        )
        file_path = next((f for f in raw_files if os.path.basename(f) == selected_file), None)
    else:
        st.error("No raw data files found.")
        st.stop()

# Main try-except block for data loading and processing
try:
    # Load the selected file with optimized datatypes
    try:
        df = pd.read_csv(file_path, low_memory=False)
        
        # Optimize numeric columns
        for col in df.select_dtypes(include=['int64', 'float64']).columns:
            if df[col].notnull().all():  # Only optimize if no null values
                if df[col].dtype == 'int64' and df[col].between(df[col].min(), df[col].max()).all():
                    df[col] = df[col].astype('int32')
                elif df[col].dtype == 'float64':
                    df[col] = df[col].astype('float32')
        
        # Convert problematic column types to string
        for col in df.columns:
            if not pd.api.types.is_numeric_dtype(df[col]) and not pd.api.types.is_string_dtype(df[col]):
                df[col] = df[col].astype(str)
    except Exception as e:
        st.error(f"Error loading data: {str(e)}")
        st.stop()
    
    # Get file statistics
    file_stats = os.stat(file_path)
    file_date = datetime.fromtimestamp(file_stats.st_mtime)
    
    # Display basic file info
    st.markdown(f"### File: `{selected_file}`")
    st.markdown(f"**Last modified:** {file_date.strftime('%Y-%m-%d %H:%M:%S')}  |  **Size:** {file_stats.st_size / 1024:.2f} KB  |  **Records:** {len(df):,}  |  **Columns:** {len(df.columns)}")
    # Create tabs for different views
    main_tabs = st.tabs([
        "📊 Overview", 
        "📈 Standard Charts",
        "👥 Demographics",
        "🔍 Patterns",
        "📋 Column Analysis"
    ])

    # Identify numeric and categorical columns
    numeric_columns = df.select_dtypes(include=[np.number]).columns.tolist()
    categorical_columns = df.select_dtypes(include=['object']).columns.tolist()

    # =============== OVERVIEW TAB ===============
    with main_tabs[0]:
        st.subheader("Data Overview")

        # Display summary statistics
        col1, col2, col3 = st.columns(3)
        with col1:
            metric_card("Total Records", len(df))
        with col2:
            metric_card("Numeric Columns", len(numeric_columns))
        with col3:
            metric_card("Categorical Columns", len(categorical_columns))

        # Display data sample
        st.subheader("Data Sample")
        st.dataframe(df.head(), use_container_width=True)

        # Display summary statistics
        st.subheader("Summary Statistics")
        if numeric_columns:
            stats_df = df[numeric_columns].describe()
            st.dataframe(stats_df, use_container_width=True)
        else:
            st.info("No numeric columns found for summary statistics.")

    # =============== STANDARD CHARTS TAB (Previously Data Visualization) ===============
    with main_tabs[1]:
        st.subheader("Standard Charts")

        # Visualization type selector
        viz_type = st.selectbox(
            "Select Visualization Type",
            ["Distribution Plot", "Scatter Plot", "Box Plot", "Bar Plot"]
        )

        if viz_type == "Distribution Plot":
            col = st.selectbox("Select Column", numeric_columns)
            if col:
                fig = px.histogram(
                    df, x=col,
                    title=f"Distribution of {col}",
                    marginal="box"
                )
                st.plotly_chart(fig, use_container_width=True)

        elif viz_type == "Scatter Plot":
            col1 = st.selectbox("Select X-axis", numeric_columns, key="scatter_x")
            col2 = st.selectbox("Select Y-axis", numeric_columns, key="scatter_y")
            color_col = st.selectbox(
                "Color by (optional)", 
                ["None"] + categorical_columns,
                key="scatter_color"
            )

            if col1 and col2:
                fig = px.scatter(
                    df,
                    x=col1,
                    y=col2,
                    color=None if color_col == "None" else color_col,
                    title=f"{col2} vs {col1}"
                )
                st.plotly_chart(fig, use_container_width=True)

        elif viz_type == "Box Plot":
            col = st.selectbox("Select Numeric Column", numeric_columns)
            group_col = st.selectbox(
                "Group by (optional)", 
                ["None"] + categorical_columns
            )

            if col:
                if group_col != "None":
                    fig = px.box(df, x=group_col, y=col)
                else:
                    fig = px.box(df, y=col)
                st.plotly_chart(fig, use_container_width=True)

        elif viz_type == "Bar Plot":
            col = st.selectbox("Select Category Column", categorical_columns)
            if col:
                value_counts = df[col].value_counts()
                fig = px.bar(
                    x=value_counts.index,
                    y=value_counts.values,
                    title=f"Count of {col}"
                )
                st.plotly_chart(fig, use_container_width=True)

    # =============== DEMOGRAPHICS TAB ===============
    with main_tabs[2]:
        st.subheader("Demographic Analysis")
        
        demo_type = st.selectbox(
            "Select Demographics View",
            ["Age Analysis", "Gender Distribution", "Job Analysis", "Geographic Distribution", "Email Analysis"]
        )
        
        if demo_type == "Age Analysis":
            # Calculate age from birth date if available
            if 'birth_date' in df.columns or 'date_of_birth' in df.columns or 'Date of birth' in df.columns:
                birth_date_col = 'birth_date' if 'birth_date' in df.columns else ('date_of_birth' if 'date_of_birth' in df.columns else 'Date of birth')
                today = pd.to_datetime('today')
                df['age'] = today.year - pd.to_datetime(df[birth_date_col]).dt.year
                # Adjust for birthdays that haven't occurred this year
                birth_dates = pd.to_datetime(df[birth_date_col])
                df.loc[((today.month < birth_dates.dt.month) | 
                       ((today.month == birth_dates.dt.month) & (today.day < birth_dates.dt.day))), 'age'] -= 1
                
                col1, col2 = st.columns(2)
                with col1:
                    # Age distribution
                    fig = px.histogram(
                        df,
                        x='age',
                        nbins=20,
                        title="Age Distribution"
                    )
                    st.plotly_chart(fig, use_container_width=True)
                
                with col2:
                    # Age groups
                    df['age_group'] = pd.cut(df['age'], 
                        bins=[0, 18, 25, 35, 50, 65, 100],
                        labels=['Under 18', '18-25', '26-35', '36-50', '51-65', '65+'])
                    age_groups = df['age_group'].value_counts()
                    fig = px.pie(values=age_groups.values, 
                               names=age_groups.index, 
                               title="Age Groups Distribution")
                    st.plotly_chart(fig, use_container_width=True)
            else:
                st.warning("No birth date column found in the dataset")
                
        elif demo_type == "Gender Distribution":
            if 'Sex' in df.columns:
                col1, col2 = st.columns(2)
                with col1:
                    # Gender distribution
                    gender_dist = df['Sex'].value_counts()
                    fig = px.pie(
                        values=gender_dist.values,
                        names=gender_dist.index,
                        title="Gender Distribution"
                    )
                    st.plotly_chart(fig, use_container_width=True)
                
                with col2:
                    # Gender statistics
                    st.subheader("Gender Statistics")
                    st.dataframe(df['Sex'].value_counts(normalize=True)
                               .mul(100)
                               .round(2)
                               .reset_index()
                               .rename(columns={'index': 'Gender', 
                                              'Sex': 'Percentage (%)'}))
            else:
                st.warning("No gender/sex column found in the dataset")
                
        elif demo_type == "Job Analysis":
            if 'Job Title' in df.columns:
                col1, col2 = st.columns(2)
                with col1:
                    # Top jobs
                    top_jobs = df['Job Title'].value_counts().head(10)
                    fig = px.bar(
                        x=top_jobs.index,
                        y=top_jobs.values,
                        title="Top 10 Job Titles"
                    )
                    st.plotly_chart(fig, use_container_width=True)
                
                with col2:
                    # Job titles by gender if available
                    if 'Sex' in df.columns:
                        job_gender = pd.crosstab(df['Job Title'], df['Sex']).head(10)
                        fig = px.bar(
                            job_gender,
                            title="Top 10 Jobs by Gender",
                            barmode='group'
                        )
                        st.plotly_chart(fig, use_container_width=True)
                    else:
                        st.info("Sex data not available for job distribution")
            else:
                st.warning("No job title column found in the dataset")
                
        elif demo_type == "Geographic Distribution":
            if 'phone' in df.columns:
                col1, col2 = st.columns(2)
                
                with col1:
                    # Use cached function for area code analysis
                    area_counts = get_area_code_stats(df)
                    
                    fig = px.bar(
                        x=area_counts.index,
                        y=area_counts.values,
                        title="Top 10 Area Codes Distribution",
                        labels={'x': 'Area Code', 'y': 'Number of Users'}
                    )
                    st.plotly_chart(fig, use_container_width=True)
                
                with col2:
                    # Display area code statistics
                    st.subheader("Area Code Statistics")
                    stats_df = pd.DataFrame({
                        'Area Code': area_counts.index,
                        'Count': area_counts.values,
                        'Percentage (%)': (area_counts.values / len(df) * 100).round(2)
                    })
                    st.dataframe(stats_df, use_container_width=True)
            else:
                st.warning("No phone number column found in the dataset")
                
        elif demo_type == "Email Analysis":
            if 'email' in df.columns:
                # Use cached function for email domain analysis
                domain_counts = get_email_domain_stats(df)
                
                fig = px.pie(
                    values=domain_counts.values,
                    names=domain_counts.index,
                    title="Top 10 Email Providers"
                )
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.warning("No email column found in the dataset")

    # =============== PATTERNS TAB ===============
    with main_tabs[3]:
        st.subheader("Data Patterns Analysis")
        
        pattern_type = st.selectbox(
            "Select Pattern Analysis",
            ["Name Analysis", "User ID Patterns", "Birth Date Patterns"]
        )
        
        if pattern_type == "Name Analysis":
            col1, col2 = st.columns(2)
            
            # First name analysis
            if 'first_name' in df.columns:
                with col1:
                    df['first_name_length'] = df['first_name'].str.len()
                    fig = px.histogram(
                        df,
                        x='first_name_length',
                        title="First Name Length Distribution"
                    )
                    st.plotly_chart(fig, use_container_width=True)
            
            # Last name analysis
            if 'last_name' in df.columns:
                with col2:
                    df['last_name_length'] = df['last_name'].str.len()
                    fig = px.histogram(
                        df,
                        x='last_name_length',
                        title="Last Name Length Distribution"
                    )
                    st.plotly_chart(fig, use_container_width=True)
                    
            if 'first_name' not in df.columns and 'last_name' not in df.columns:
                st.warning("No name columns found in the dataset")
                
        elif pattern_type == "User ID Patterns":
            if 'user_id' in df.columns:
                # Analyze user ID patterns
                df['id_length'] = df['user_id'].astype(str).str.len()
                df['id_prefix'] = df['user_id'].astype(str).str[:2]
                
                col1, col2 = st.columns(2)
                with col1:
                    # ID length distribution
                    fig = px.histogram(
                        df,
                        x='id_length',
                        title="User ID Length Distribution"
                    )
                    st.plotly_chart(fig, use_container_width=True)
                
                with col2:
                    # Common ID prefixes
                    prefix_counts = df['id_prefix'].value_counts().head(10)
                    fig = px.bar(
                        x=prefix_counts.index,
                        y=prefix_counts.values,
                        title="Common User ID Prefixes"
                    )
                    st.plotly_chart(fig, use_container_width=True)
            else:
                st.warning("No user ID column found in the dataset")
                
        elif pattern_type == "Birth Date Patterns":
            if 'birth_date' in df.columns or 'date_of_birth' in df.columns or 'Date of birth' in df.columns:
                birth_date_col = 'birth_date' if 'birth_date' in df.columns else ('date_of_birth' if 'date_of_birth' in df.columns else 'Date of birth')
                df['birth_date_parsed'] = pd.to_datetime(df[birth_date_col])
                
                col1, col2 = st.columns(2)
                with col1:
                    # Month distribution
                    month_dist = df['birth_date_parsed'].dt.month.value_counts().sort_index()
                    fig = px.bar(
                        x=['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 
                           'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'],
                        y=month_dist.values,
                        title="Birth Month Distribution"
                    )
                    st.plotly_chart(fig, use_container_width=True)
                
                with col2:
                    # Day of week distribution
                    day_dist = df['birth_date_parsed'].dt.day_name().value_counts()
                    fig = px.bar(
                        x=['Monday', 'Tuesday', 'Wednesday', 'Thursday', 
                           'Friday', 'Saturday', 'Sunday'],
                        y=[day_dist.get(day, 0) for day in 
                           ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 
                            'Friday', 'Saturday', 'Sunday']],
                        title="Birth Day of Week Distribution"
                    )
                    st.plotly_chart(fig, use_container_width=True)
            else:
                st.warning("No birth date column found in the dataset")

    # =============== COLUMN ANALYSIS TAB ===============
    with main_tabs[4]:
        st.subheader("Column Analysis")

        # Column selector
        selected_column = st.selectbox(
            "Select Column for Analysis",
            df.columns.tolist()
        )

        if selected_column:
            col1, col2 = st.columns(2)

            with col1:
                st.subheader("Column Statistics")
                stats = {
                    "Type": str(df[selected_column].dtype),
                    "Unique Values": df[selected_column].nunique(),
                    "Missing Values": df[selected_column].isna().sum(),
                    "Missing %": f"{(df[selected_column].isna().sum() / len(df)) * 100:.2f}%"
                }

                for stat, value in stats.items():
                    st.metric(stat, value)

            with col2:
                st.subheader("Value Distribution")
                if df[selected_column].dtype in ['int64', 'float64']:
                    fig = px.histogram(
                        df,
                        x=selected_column,
                        title=f"Distribution of {selected_column}"
                    )
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    value_counts = df[selected_column].value_counts().head(10)
                    fig = px.bar(
                        x=value_counts.index,
                        y=value_counts.values,
                        title=f"Top 10 Values in {selected_column}"
                    )
                    st.plotly_chart(fig, use_container_width=True)
    # =============== DATA EXPLORER TAB ===============
    with main_tabs[3]:
        st.subheader("Data Explorer")
        
        col1, col2 = st.columns([1, 2])
        
        with col1:
            # Filter controls
            st.subheader("Search & Filter")
            
            with st.expander("Filter Options", expanded=True):
                # Text search
                search_col = st.selectbox(
                    "Search in column",
                    options=['All Columns'] + df.columns.tolist()
                )
                
                search_text = st.text_input("Search text (case-insensitive)")
                
                if search_text and search_col != 'All Columns':
                    # Filter for specific column
                    mask = df[search_col].astype(str).str.contains(search_text, case=False, na=False)
                    filtered_df = df[mask]
                elif search_text:
                    # Search across all columns
                    mask = pd.Series(False, index=df.index)
                    for col in df.columns:
                        mask |= df[col].astype(str).str.contains(search_text, case=False, na=False)
                    filtered_df = df[mask]
                else:
                    filtered_df = df
                
                # Numeric range filters
                numeric_filters = st.multiselect(
                    "Add numeric filters",
                    options=numeric_columns
                )
                
                for col in numeric_filters:
                    min_val, max_val = float(filtered_df[col].min()), float(filtered_df[col].max())
                    if min_val < max_val:
                        range_vals = st.slider(
                            f"Filter {col}",
                            min_value=min_val,
                            max_value=max_val,
                            value=(min_val, max_val)
                        )
                        filtered_df = filtered_df[
                            (filtered_df[col] >= range_vals[0]) & 
                            (filtered_df[col] <= range_vals[1])
                        ]
                
                st.info(f"Showing {len(filtered_df):,} of {len(df):,} records")
        
        with col2:
            # Display filtered data
            st.subheader("Filtered Data")
            st.dataframe(filtered_df, use_container_width=True)
            
            if not filtered_df.empty:
                csv = filtered_df.to_csv(index=False).encode('utf-8')
                st.download_button(
                    "Download Filtered Data",
                    data=csv,
                    file_name=f"filtered_{selected_file}",
                    mime="text/csv"
                )

    # =============== DATA QUALITY TAB ===============
    with main_tabs[4]:
        st.subheader("Data Quality Assessment")
        
        # Overall quality metrics
        total_cells = df.size
        missing_cells = df.isna().sum().sum()
        missing_pct = (missing_cells / total_cells) * 100
        duplicate_rows = df.duplicated().sum()
        
        col1, col2, col3 = st.columns(3)
        with col1:
            metric_card("Completeness", f"{100 - missing_pct:.1f}", suffix="%")
        with col2:
            metric_card("Missing Values", missing_cells, f"{missing_pct:.1f}%")
        with col3:
            metric_card("Duplicate Rows", duplicate_rows, f"{(duplicate_rows/len(df))*100:.1f}%")
        
        # Column quality analysis
        st.subheader("Column Quality Analysis")
        
        quality_data = []
        for col in df.columns:
            missing = df[col].isna().sum()
            unique_count = df[col].nunique()
            
            quality_data.append({
                'Column': col,
                'Type': str(df[col].dtype),
                'Missing Values': missing,
                'Missing %': f"{(missing/len(df))*100:.1f}%",
                'Unique Values': unique_count,
                'Unique %': f"{(unique_count/len(df))*100:.1f}%",
                'Sample Values': ', '.join(map(str, df[col].dropna().head(3)))
            })
        
        quality_df = pd.DataFrame(quality_data)
        st.dataframe(quality_df, use_container_width=True)
        
        # Completeness visualization
        st.subheader("Data Completeness by Column")
        completeness_data = pd.DataFrame({
            'Column': df.columns,
            'Completeness': [100 - (df[col].isna().sum() / len(df) * 100) for col in df.columns]
        }).sort_values('Completeness')
        
        fig = px.bar(
            completeness_data,
            x='Completeness',
            y='Column',
            orientation='h',
            title="Column Completeness (%)",
            color='Completeness',
            color_continuous_scale='RdYlGn'
        )
        st.plotly_chart(fig, use_container_width=True)

except Exception as e:
    st.error(f"Error loading or processing file: {str(e)}")
    st.stop()

# Footer
st.sidebar.markdown("---")
st.sidebar.info(
    f"Dashboard updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
)