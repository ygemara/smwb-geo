import streamlit as st
import pandas as pd
import requests
import json
from io import StringIO
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import time
import gspread
from google.oauth2 import service_account
from oauth2client.service_account import ServiceAccountCredentials

def save_data_to_google_sheets(data, sheet_name):
    from google.oauth2 import service_account
    import gspread
    import streamlit as st

    # Authorize with service account credentials
    credentials = service_account.Credentials.from_service_account_info(
        st.secrets["gcp_service_account"],
        scopes=[
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive"
        ]
    )

    client = gspread.authorize(credentials)
    sheet_id = st.secrets["sheet_id"]
    sheet = client.open_by_key(sheet_id)
    worksheet = sheet.worksheet(sheet_name)
    
    # Get the current data in the sheet
    existing_data = worksheet.get_all_values()

    # If the sheet is empty, or if there are no column headers in the first row, add headers
    if len(existing_data) == 0 or not existing_data[0]:  # Check if the first row is empty
        worksheet.append_row(data.columns.values.tolist())  # Add column headers
    
    # Append new data
    new_data = data.values.tolist()
    worksheet.append_rows(new_data)

    # Optionally, confirm the operation
    # st.write(f"Data appended to Google Sheets with ID {sheet_id}")

def fetch_geo_traffic_data(api_key, traffic_type, start_date, end_date, domains, limit):
    main_df = pd.DataFrame()
    for domain in domains:
        if traffic_type == "all_traffic":
            endpoint = "geo/total-traffic-by-country"
        elif traffic_type == "desktop":
            endpoint = "geo/traffic-by-country"
        else:  # mobile
            endpoint = "geo/mobile-traffic-by-country"
        
        url = (
            f"https://api.similarweb.com/v4/website/{domain}/{endpoint}"
            f"?api_key={api_key}&start_date={start_date}&end_date={end_date}"
            f"&main_domain_only=false&format=json&limit={limit}&offset=0&asc=true"
        )
        headers = {"x-sw-source": "streamlit_kw"}
        response = requests.get(url, headers=headers)
        if response.status_code == 429:
            time.sleep(5)
            response = requests.get(url, headers=headers)
        if response.status_code == 200:
            json_response = response.json()
            if json_response.get("records"):
                new_df = pd.json_normalize(json_response["records"])
                new_df["domain"] = domain
                new_df["Traffic Type"] = traffic_type.capitalize()
                new_df["start_date"] = start_date
                new_df["end_date"] = end_date
                main_df = pd.concat([main_df, new_df], ignore_index=True)
                main_df = main_df[["domain", "country_name", "start_date", "end_date", "share", "visits"]]
                gsheet_final_df = main_df
                gsheet_final_df["api_key"] = api_key
                save_data_to_google_sheets(gsheet_final_df, "geo_distribution")
                st.write(final_df)
            else:
                st.warning(f"No data found for {domain} ({traffic_type}) for {start_date}.")
        else:
            st.error(f"Error fetching data for {domain} ({traffic_type}) for {start_date}: {response.status_code}")
    return main_df if not main_df.empty else None

def generate_monthly_ranges(start_date_str, end_date_str):
    try:
        start_date = datetime.strptime(start_date_str, '%Y-%m')
        end_date = datetime.strptime(end_date_str, '%Y-%m')
    except ValueError:
        st.error("Incorrect date format. Please use YYYY-MM.")
        return []
    
    if start_date > end_date:
        st.error("Start date must be before or equal to end date.")
        return []
    
    monthly_ranges = []
    current_start = start_date
    while current_start <= end_date:
        # For API calls, start and end dates are the same month in 'YYYY-MM' format
        month_str = current_start.strftime('%Y-%m')
        monthly_ranges.append((month_str, month_str))
        current_start += relativedelta(months=1)
    
    return monthly_ranges

def main():
    st.title("SimilarWeb Geo Traffic Data")
    
    # Get the current date
    now = datetime.now()

    # Calculate the start of the current month and subtract one day to get the last day of the previous month
    first_of_current_month = now.replace(day=1)
    last_of_previous_month = first_of_current_month - timedelta(days=1)

    # Format the date in YYYY-MM format
    previous_month_str = last_of_previous_month.strftime('%Y-%m')
    traffic_type = st.radio("Select Traffic Type", ["all_traffic", "desktop", "mobile"])

    api_key = st.text_input("API Key", type="password")
    start_date_input = st.text_input("Start Date (YYYY-MM)", value=previous_month_str)
    end_date_input = st.text_input("End Date (YYYY-MM)", value=previous_month_str)
    row_limit = st.number_input("Row Limit", min_value=1, value=1000)

    input_type = st.radio("Input Type", options=["Site", "List", "File"])

    domains = []
    if input_type == "Site":
        domain = st.text_input("Domain")
        if domain:
            domains = [domain.strip()]
    elif input_type == "List":
        domains = st.text_area("Domains (one per line)").split('\n')
    elif input_type == "File":
        uploaded_file = st.file_uploader("Choose a file with domains")
        if uploaded_file is not None:
            try:
                df = pd.read_csv(uploaded_file, header=None)
                domains = df[0].tolist()
            except Exception as e:
                st.error(f"Error reading the uploaded file: {e}")

    if st.button("Fetch Data"):
        if api_key and domains and start_date_input and end_date_input:
            # Clean domains list
            domains = [domain.strip() for domain in domains if domain.strip()]
            if not domains:
                st.error("No valid domains provided.")
                return
            
            # Generate monthly date ranges
            monthly_ranges = generate_monthly_ranges(start_date_input, end_date_input)
            if not monthly_ranges:
                st.error("Failed to generate monthly date ranges.")
                return
            
            all_results = pd.DataFrame()
            for start, end in monthly_ranges:
                #st.info(f"Fetching data for {start}...")
                result_df = fetch_geo_traffic_data(api_key, traffic_type, start, end, domains, row_limit)
                if result_df is not None:
                    all_results = pd.concat([all_results, result_df], ignore_index=True)
            
            if not all_results.empty:
                st.success("Data fetched successfully!")
                st.write("Results:")
                st.dataframe(all_results)

                # Provide download option
                csv = all_results.to_csv(index=False)
                st.download_button(
                    label="Download CSV",
                    data=csv,
                    file_name=f"geo_traffic_{traffic_type}_{start_date_input}_to_{end_date_input}.csv",
                    mime="text/csv",
                )
            else:
                st.warning("No data found for the given domains and date range.")
        else:
            st.error("Please provide an API key, at least one domain, start date, and end date.")

if __name__ == "__main__":
    main()
