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
    # Use absolute paths that match the container paths due to volume mapping
    raw_files = glob.glob('./data/raw/*.csv')
    processed_files = glob.glob('./data/processed/processed_data_*.csv')
    report_files = glob.glob('./data/processed/analysis_report_*.txt')
    
    # Sort files by creation time, newest first
    raw_files = sorted(raw_files, key=os.path.getctime, reverse=True) if raw_files else []
    processed_files = sorted(processed_files, key=os.path.getctime, reverse=True) if processed_files else []
    report_files = sorted(report_files, key=os.path.getctime, reverse=True) if report_files else []
    
    return raw_files, processed_files, report_files

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

# Load data
raw_files, processed_files, report_files = get_data_files()

# Check if any files were found
if not (raw_files or processed_files):
    st.warning("No data files found. Please run the ETL pipeline first.")
    st.stop()

# File selection in sidebar
st.sidebar.header("Data Selection")
st.sidebar.markdown("Select a file to analyze:")

# File type tabs
file_type = st.sidebar.radio("Select Data Type", ["Processed Data", "Raw Data"])

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

# Load selected file
try:
    df = pd.read_csv(file_path)
    
    # Convert problematic column types to string to avoid serialization issues
    for col in df.columns:
        if not pd.api.types.is_numeric_dtype(df[col]) and not pd.api.types.is_string_dtype(df[col]):
            df[col] = df[col].astype(str)
    
    file_stats = os.stat(file_path)
    file_date = datetime.fromtimestamp(file_stats.st_mtime)
    
    # Display basic file info above tabs
    st.markdown(f"### File: `{selected_file}`")
    st.markdown(f"**Last modified:** {file_date.strftime('%Y-%m-%d %H:%M:%S')}  |  **Size:** {file_stats.st_size / 1024:.2f} KB  |  **Records:** {len(df):,}  |  **Columns:** {len(df.columns)}")
    
    # =============== MAIN DASHBOARD TABS ===============
    main_tabs = st.tabs([
        "📋 Overview", 
        "📊 Column Analysis", 
        "📈 Visualizations", 
        "🔍 Data Explorer",
        "📝 Data Quality"
    ])

    # =============== OVERVIEW TAB ===============
    with main_tabs[0]:
        col1, col2 = st.columns([3, 2])
        
        with col1:
            st.subheader("Data Preview")
            st.dataframe(df.head(10), use_container_width=True)
            
            # Summary statistics - handle non-numeric columns
            st.subheader("Summary Statistics")
            try:
                stats_df = df.select_dtypes(include=['number']).describe()
                st.dataframe(stats_df, use_container_width=True)
            except Exception as e:
                st.warning(f"Could not generate numeric statistics: {str(e)}")
                st.dataframe(df.describe(include='all'), use_container_width=True)
            
        with col2:
            st.subheader("File Information")
            
            # File metrics
            metric_card("File Age", f"{(datetime.now() - file_date).days}", suffix=" days")
            metric_card("Number of Rows", f"{len(df):,}")
            metric_card("Number of Columns", f"{len(df.columns):,}")
            
            # Data types distribution
            st.subheader("Column Data Types")
            # Safely convert dtypes to strings for grouping
            dtype_mapping = {col: str(dtype) for col, dtype in zip(df.columns, df.dtypes)}
            type_counts = pd.Series(dtype_mapping).value_counts().reset_index()
            type_counts.columns = ['Data Type', 'Count']
            
            # Create pie chart with type counts
            try:
                fig = px.pie(
                    type_counts, 
                    values='Count', 
                    names='Data Type',
                    color_discrete_sequence=px.colors.qualitative.Pastel,
                    hole=0.4
                )
                fig.update_layout(
                    legend=dict(orientation="h", yanchor="bottom", y=-0.2, xanchor="center", x=0.5),
                    margin=dict(t=25, b=0, l=25, r=25),
                    height=300
                )
                st.plotly_chart(fig, use_container_width=True)
            except Exception as e:
                st.warning(f"Could not generate type distribution chart: {str(e)}")
                st.dataframe(type_counts, use_container_width=True)

    # =============== COLUMN ANALYSIS TAB ===============
    with main_tabs[1]:
        st.subheader("Column Details")
        
        # Column selection
        selected_column = st.selectbox(
            "Select column to analyze",
            options=df.columns.tolist()
        )
        
        col1, col2 = st.columns([1, 1])
        
        with col1:
            # Column statistics
            st.subheader(f"Statistics for '{selected_column}'")
            
            if pd.api.types.is_numeric_dtype(df[selected_column]):
                # Handle numeric columns
                try:
                    stats = pd.DataFrame({
                        'Statistic': ['Count', 'Mean', 'Std Dev', 'Min', '25%', 'Median', '75%', 'Max', 'Null Count', 'Unique Values'],
                        'Value': [
                            df[selected_column].count(),
                            df[selected_column].mean(),
                            df[selected_column].std(),
                            df[selected_column].min(),
                            df[selected_column].quantile(0.25),
                            df[selected_column].median(),
                            df[selected_column].quantile(0.75),
                            df[selected_column].max(),
                            df[selected_column].isna().sum(),
                            df[selected_column].nunique()
                        ]
                    })
                    
                    # Format numeric values
                    stats['Value'] = stats['Value'].apply(lambda x: f"{x:,.4f}" if isinstance(x, (int, float)) else str(x))
                    
                except Exception as e:
                    st.warning(f"Error calculating statistics: {str(e)}")
                    stats = pd.DataFrame({
                        'Statistic': ['Count', 'Null Count', 'Unique Values'],
                        'Value': [
                            df[selected_column].count(),
                            df[selected_column].isna().sum(),
                            df[selected_column].nunique()
                        ]
                    })
            else:
                # Handle non-numeric columns
                try:
                    most_common = None
                    freq_most_common = 0
                    
                    if df[selected_column].value_counts().shape[0] > 0:
                        most_common = str(df[selected_column].value_counts().index[0])
                        freq_most_common = int(df[selected_column].value_counts().iloc[0])
                    
                    stats = pd.DataFrame({
                        'Statistic': ['Count', 'Null Count', 'Unique Values', 'Most Common', 'Frequency of Most Common'],
                        'Value': [
                            df[selected_column].count(),
                            df[selected_column].isna().sum(),
                            df[selected_column].nunique(),
                            most_common if most_common else "N/A",
                            freq_most_common
                        ]
                    })
                except Exception as e:
                    st.warning(f"Error calculating statistics: {str(e)}")
                    stats = pd.DataFrame({
                        'Statistic': ['Count', 'Null Count', 'Unique Values'],
                        'Value': [
                            df[selected_column].count(),
                            df[selected_column].isna().sum(),
                            df[selected_column].nunique()
                        ]
                    })
            
            st.dataframe(stats, use_container_width=True, hide_index=True)
        
        with col2:
            # Column visualization
            st.subheader(f"Distribution of '{selected_column}'")
            
            if pd.api.types.is_numeric_dtype(df[selected_column]):
                try:
                    # Histogram for numeric data
                    fig = px.histogram(
                        df,
                        x=selected_column,
                        nbins=30,
                        color_discrete_sequence=['#3366CC']
                    )
                    fig.update_layout(
                        xaxis_title=selected_column,
                        yaxis_title="Frequency",
                        showlegend=False,
                        margin=dict(l=40, r=40, t=40, b=40),
                        height=300
                    )
                    st.plotly_chart(fig, use_container_width=True)
                    
                    # Box plot
                    fig = px.box(
                        df,
                        y=selected_column,
                        color_discrete_sequence=['#3366CC']
                    )
                    fig.update_layout(
                        yaxis_title=selected_column,
                        showlegend=False,
                        margin=dict(l=40, r=40, t=40, b=40),
                        height=300
                    )
                    st.plotly_chart(fig, use_container_width=True)
                except Exception as e:
                    st.warning(f"Could not create visualizations: {str(e)}")
                
            else:
                try:
                    # Bar chart for categorical data
                    # Convert to string to ensure compatibility
                    temp_col = df[selected_column].astype(str)
                    value_counts = temp_col.value_counts().reset_index()
                    value_counts.columns = [selected_column, 'Count']
                    value_counts = value_counts.sort_values('Count', ascending=False).head(20)
                    
                    fig = px.bar(
                        value_counts,
                        x=selected_column,
                        y='Count',
                        color_discrete_sequence=['#3366CC']
                    )
                    fig.update_layout(
                        xaxis_title=selected_column,
                        yaxis_title="Count",
                        showlegend=False,
                        margin=dict(l=40, r=40, t=40, b=40),
                        height=400
                    )
                    
                    # Rotate x-axis labels if there are many categories
                    if len(value_counts) > 5:
                        fig.update_layout(xaxis_tickangle=-45)
                    
                    st.plotly_chart(fig, use_container_width=True)
                except Exception as e:
                    st.warning(f"Could not create visualization: {str(e)}")
                    st.write("Top values:")
                    top_values = df[selected_column].value_counts().head(10)
                    st.dataframe(top_values)

    # =============== VISUALIZATIONS TAB ===============
    with main_tabs[2]:
        st.subheader("Data Visualizations")
        
        # Get numeric columns for plotting
        try:
            numeric_columns = df.select_dtypes(include=['number']).columns.tolist()
        except Exception as e:
            # Fallback: manually check each column
            numeric_columns = []
            for col in df.columns:
                try:
                    if pd.to_numeric(df[col], errors='coerce').notna().all():
                        numeric_columns.append(col)
                except:
                    pass
        
        if len(numeric_columns) < 2:
            st.warning("Not enough numeric columns for creating visualizations. The dashboard found these numeric columns: " + 
                      (", ".join(numeric_columns) if numeric_columns else "None"))
        else:
            viz_type = st.radio(
                "Select visualization type",
                options=["Scatter Plot", "Line Chart", "Correlation Heatmap", "Pair Plot"],
                horizontal=True
            )
            
            if viz_type == "Scatter Plot":
                col1, col2 = st.columns([1, 1])
                
                with col1:
                    x_axis = st.selectbox("Select X-axis", options=numeric_columns, index=0)
                
                with col2:
                    y_axis = st.selectbox("Select Y-axis", options=numeric_columns, index=min(1, len(numeric_columns)-1))
                
                # Add color selection if there are categorical columns
                try:
                    categorical_cols = df.select_dtypes(include=['object', 'category']).columns.tolist()
                except:
                    # Fallback: use columns with low number of unique values
                    categorical_cols = [col for col in df.columns if col not in numeric_columns and df[col].nunique() < 20]
                
                color_col = None
                if categorical_cols:
                    color_col = st.selectbox(
                        "Color points by (optional)",
                        options=['None'] + categorical_cols,
                        index=0
                    )
                    if color_col == 'None':
                        color_col = None
                
                try:
                    # Create scatter plot
                    fig = px.scatter(
                        df.sample(min(5000, len(df))),  # Sample to avoid overcrowding
                        x=x_axis,
                        y=y_axis,
                        color=color_col,
                        trendline="ols",
                        labels={x_axis: x_axis, y_axis: y_axis},
                        title=f"{y_axis} vs {x_axis}"
                    )
                    fig.update_layout(height=600)
                    st.plotly_chart(fig, use_container_width=True)
                    
                    # Calculate correlation
                    corr = df[x_axis].corr(df[y_axis])
                    st.metric("Correlation Coefficient", f"{corr:.4f}")
                except Exception as e:
                    st.warning(f"Could not create scatter plot: {str(e)}")
                    st.write("Please try different columns or visualization types.")
            
            elif viz_type == "Line Chart":
                try:
                    x_axis = st.selectbox("Select X-axis", options=numeric_columns, index=0)
                    y_columns = st.multiselect(
                        "Select Y-axis variable(s)",
                        options=[col for col in numeric_columns if col != x_axis],
                        default=[numeric_columns[min(1, len(numeric_columns)-1)]]
                    )
                    
                    if y_columns:
                        # Sort data by x-axis for connected lines
                        chart_data = df.sort_values(by=x_axis)
                        
                        # Create line chart
                        fig = go.Figure()
                        
                        for y_col in y_columns:
                            fig.add_trace(go.Scatter(
                                x=chart_data[x_axis],
                                y=chart_data[y_col],
                                mode='lines',
                                name=y_col
                            ))
                        
                        fig.update_layout(
                            xaxis_title=x_axis,
                            yaxis_title="Value",
                            legend_title="Variables",
                            height=500
                        )
                        st.plotly_chart(fig, use_container_width=True)
                    else:
                        st.warning("Please select at least one Y-axis variable")
                except Exception as e:
                    st.warning(f"Could not create line chart: {str(e)}")
                    st.write("Please try different columns or visualization types.")
            
            elif viz_type == "Correlation Heatmap":
                try:
                    # Limit to top 15 numeric columns if there are many
                    if len(numeric_columns) > 15:
                        st.info("Showing correlation for the first 15 numeric columns only.")
                        corr_columns = numeric_columns[:15]
                    else:
                        corr_columns = numeric_columns
                    
                    # Calculate correlation matrix
                    corr_matrix = df[corr_columns].corr()
                    
                    # Create heatmap
                    fig = px.imshow(
                        corr_matrix,
                        text_auto='.2f',
                        color_continuous_scale='RdBu_r',
                        aspect="auto",
                        title="Correlation Matrix"
                    )
                    fig.update_layout(height=600)
                    st.plotly_chart(fig, use_container_width=True)
                    
                    # Highlight strongest correlations
                    corr_pairs = corr_matrix.unstack().sort_values(ascending=False)
                    # Remove self-correlations
                    corr_pairs = corr_pairs[corr_pairs < 0.999]
                    
                    if not corr_pairs.empty:
                        st.subheader("Strongest Correlations")
                        top_corrs = pd.DataFrame({
                            'Variables': [f"{i[0]} & {i[1]}" for i in corr_pairs.index[:5]],
                            'Correlation': corr_pairs.values[:5]
                        })
                        st.dataframe(top_corrs, use_container_width=True, hide_index=True)
                except Exception as e:
                    st.warning(f"Could not create correlation heatmap: {str(e)}")
                    st.write("Please try different columns or visualization types.")
            
            elif viz_type == "Pair Plot":
                try:
                    # Let user select a subset of columns (max 5 for readability)
                    selected_cols = st.multiselect(
                        "Select columns to include (max 5 recommended)",
                        options=numeric_columns,
                        default=numeric_columns[:min(3, len(numeric_columns))]
                    )
                    
                    if len(selected_cols) < 2:
                        st.warning("Please select at least 2 columns")
                    elif len(selected_cols) > 5:
                        st.warning("Too many columns selected. Limiting to first 5 for readability.")
                        selected_cols = selected_cols[:5]
                    
                    if len(selected_cols) >= 2:
                        # Create scatter plot matrix
                        fig = px.scatter_matrix(
                            df,
                            dimensions=selected_cols,
                            color_discrete_sequence=px.colors.qualitative.Set2
                        )
                        fig.update_layout(height=700)
                        st.plotly_chart(fig, use_container_width=True)
                except Exception as e:
                    st.warning(f"Could not create pair plot: {str(e)}")
                    st.write("Please try different columns or visualization types.")

    # =============== DATA EXPLORER TAB ===============
    with main_tabs[3]:
        st.subheader("Data Explorer")
        
        col1, col2 = st.columns([1, 2])
        
        with col1:
            # Filter controls
            st.subheader("Filter Data")
            
            with st.expander("Search & Filter", expanded=True):
                # Text search
                search_col = st.selectbox(
                    "Search in column",
                    options=['All Columns'] + df.columns.tolist()
                )
                
                search_text = st.text_input("Search text (case-insensitive)")
                
                try:
                    if search_text and search_col != 'All Columns':
                        # Filter for the specific column
                        if pd.api.types.is_string_dtype(df[search_col]):
                            filtered_df = df[df[search_col].str.contains(search_text, case=False, na=False)]
                        else:
                            # Convert column to string for searching
                            filtered_df = df[df[search_col].astype(str).str.contains(search_text, case=False, na=False)]
                    elif search_text:
                        # Search across all columns
                        mask = pd.Series(False, index=df.index)
                        for col in df.columns:
                            try:
                                mask = mask | df[col].astype(str).str.contains(search_text, case=False, na=False)
                            except:
                                # Skip columns that can't be converted to string
                                pass
                        filtered_df = df[mask]
                    else:
                        filtered_df = df
                except Exception as e:
                    st.warning(f"Search error: {str(e)}")
                    filtered_df = df
                
                # Add numeric range filters for selected columns
                numeric_cols_for_filter = st.multiselect(
                    "Add numeric filters",
                    options=numeric_columns
                )
                
                for col in numeric_cols_for_filter:
                    try:
                        min_val = float(filtered_df[col].min())
                        max_val = float(filtered_df[col].max())
                        
                        # Only show filter if we have a range of values
                        if min_val < max_val:
                            filter_range = st.slider(
                                f"Filter by {col}",
                                min_value=min_val,
                                max_value=max_val,
                                value=(min_val, max_val),
                                step=(max_val - min_val) / 100
                            )
                            
                            filtered_df = filtered_df[(filtered_df[col] >= filter_range[0]) & 
                                                    (filtered_df[col] <= filter_range[1])]
                    except Exception as e:
                        st.warning(f"Could not create filter for {col}: {str(e)}")
                
                # Show number of filtered results
                st.info(f"Showing {len(filtered_df):,} of {len(df):,} records")
        
        with col2:
            # Display filtered data
            st.subheader("Filtered Data")
            st.dataframe(filtered_df.head(100), use_container_width=True)
            
            # Show download button
            if not filtered_df.empty:
                csv = filtered_df.to_csv(index=False).encode('utf-8')
                
                st.download_button(
                    label="Download filtered data as CSV",
                    data=csv,
                    file_name=f"filtered_{selected_file}",
                    mime="text/csv",
                )

    # =============== DATA QUALITY TAB ===============
    with main_tabs[4]:
        st.subheader("Data Quality Assessment")
        
        # Data quality metrics
        try:
            total_cells = df.size
            missing_cells = df.isna().sum().sum()
            missing_pct = (missing_cells / total_cells) * 100 if total_cells > 0 else 0
            duplicate_rows = df.duplicated().sum()
            duplicate_pct = (duplicate_rows / len(df)) * 100 if len(df) > 0 else 0
            
            col1, col2, col3 = st.columns(3)
            
            with col1:
                metric_card("Data Completeness", f"{100 - missing_pct:.2f}", suffix="%")
            with col2:
                metric_card("Missing Values", f"{missing_cells:,}", f"{missing_pct:.2f}%")
            with col3:
                metric_card("Duplicate Rows", f"{duplicate_rows:,}", f"{duplicate_pct:.2f}%")
            
            # Column quality analysis
            st.subheader("Column Quality Analysis")
            
            # Handle potential data type issues safely
            quality_data = []
            for col in df.columns:
                try:
                    missing = df[col].isna().sum()
                    missing_pct = (missing / len(df)) * 100 if len(df) > 0 else 0
                    unique_values = df[col].nunique()
                    unique_pct = (unique_values / len(df)) * 100 if len(df) > 0 else 0
                    
                    # Safely get sample values
                    try:
                        non_na_values = df[col].dropna()
                        if len(non_na_values) > 0:
                            samples = non_na_values.sample(min(3, len(non_na_values))).tolist()
                            sample_str = ", ".join(str(x) for x in samples)
                        else:
                            sample_str = "No non-null values"
                    except:
                        sample_str = "Cannot display samples"
                    
                    quality_data.append({
                        'Column': col,
                        'Type': str(df[col].dtype),
                        'Missing Values': missing,
                        'Missing %': missing_pct,
                        'Unique Values': unique_values,
                        'Unique %': unique_pct,
                        'Sample Values': sample_str
                    })
                except Exception as e:
                    # Fallback for problematic columns
                    quality_data.append({
                        'Column': col,
                        'Type': str(df[col].dtype),
                        'Missing Values': "Error",
                        'Missing %': "Error",
                        'Unique Values': "Error",
                        'Unique %': "Error",
                        'Sample Values': f"Error: {str(e)}"
                    })
            
            quality_df = pd.DataFrame(quality_data)
            st.dataframe(quality_df, use_container_width=True, hide_index=True)
            
            # Completeness visualization
            st.subheader("Data Completeness by Column")
            
            try:
                # Filter to only include rows with valid numeric data
                valid_quality_rows = [row for row in quality_data 
                                    if isinstance(row['Missing %'], (int, float))]
                
                if valid_quality_rows:
                    completeness_data = pd.DataFrame(valid_quality_rows)
                    completeness_data['Completeness %'] = 100 - completeness_data['Missing %']
                    completeness_data = completeness_data.sort_values('Completeness %')
                    
                    fig = px.bar(
                        completeness_data,
                        y='Column',
                        x='Completeness %',
                        color='Completeness %',
                        color_continuous_scale='RdYlGn',
                        range_color=[0, 100],
                        labels={'Column': '', 'Completeness %': 'Completeness (%)'},
                        title="Data Completeness by Column"
                    )
                    fig.update_layout(height=max(300, 30 * len(completeness_data)))
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.warning("Could not generate completeness visualization due to data issues.")
            except Exception as e:
                st.warning(f"Could not create completeness chart: {str(e)}")
        except Exception as e:
            st.error(f"Error in data quality analysis: {str(e)}")

except Exception as e:
    st.error(f"Error loading or processing the file: {str(e)}")
    st.error(f"Error details: {type(e).__name__}")
    
    # Display available file paths
    st.subheader("Available Files")
    st.write("Raw files:")
    st.write("\n".join(raw_files) if raw_files else "No raw files found.")
    st.write("Processed files:")
    st.write("\n".join(processed_files) if processed_files else "No processed files found.")

# Footer with timestamp
st.sidebar.markdown("---")
st.sidebar.info(
    f"Dashboard generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n"
    f"User: SefanosOk"
)