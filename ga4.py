# Import required libraries
import pandas as pd
import sys, os, datetime, configparser
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import DateRange, Dimension, Metric, RunReportRequest
from google.oauth2 import service_account

# Initialize the parser with interpolation disabled
config = configparser.ConfigParser(interpolation=None)
config.read(r'C:\Users\research\Documents\MarketingMetrics\ga4config.ini')  # Ensure the path to your config file is correct

# Define the report period as last month of data
today = datetime.date.today()
first_day_of_current_month = datetime.date(today.year, today.month, 1)
last_day_of_previous_month = first_day_of_current_month - datetime.timedelta(days=1)
first_day_of_previous_month = datetime.date(last_day_of_previous_month.year, last_day_of_previous_month.month, 1)

# Format dates for use in the API call
start_date = first_day_of_previous_month.strftime('%Y-%m-%d')
end_date = last_day_of_previous_month.strftime('%Y-%m-%d')
formatted_end_date = pd.Timestamp('today').strftime('%Y-%m-%d') if end_date == 'today' else end_date
folder_path = r'C:\Users\research\OneDrive - Atlanta Convention & Visitors Bureau\Marketing Dashboard Data'

# Check if the directory exists, and create it if it does not
print(f"Checking if the directory {folder_path} exists...")
if not os.path.exists(folder_path):
    os.makedirs(folder_path)
    print(f"Created directory at {folder_path}")
else:
    print(f"Directory already exists at {folder_path}")

def log_print(*args, **kwargs):
    log_dir = f'{folder_path}/log'
    log_file_path = f'{log_dir}/Ga4log_{start_date}_{formatted_end_date}.txt'

    # Ensure the log directory exists
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
        print(f"Created log directory at {log_dir}")

    with open(log_file_path, 'a') as f:
        original_stdout = sys.stdout
        sys.stdout = f
        print(*args, **kwargs)
        sys.stdout = original_stdout  # Restore original stdout
    print(*args, **kwargs)  # Print to console

def get_total_row_count(client, property_id):
    request = RunReportRequest(
        property=f"properties/{property_id}",
        metrics=[Metric(name="sessions")],
        date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
        limit=1  # Request the smallest possible dataset
    )
    response = client.run_report(request)
    return response.row_count

def get_ga4_data(input_table):
    log_print("Starting the data retrieval process...")
    # Retrieve configurations
    try:
        # Service Account Credentials
        service_account_info = {
            "type": config.get('service_account', 'type'),
            "project_id": config.get('service_account', 'project_id'),
            "private_key_id": config.get('service_account', 'private_key_id'),
            "private_key": config.get('service_account', 'private_key'),
            "client_email": config.get('service_account', 'client_email'),
            "client_id": config.get('service_account', 'client_id'),
            "auth_uri": config.get('service_account', 'auth_uri'),
            "token_uri": config.get('service_account', 'token_uri'),
            "auth_provider_x509_cert_url": config.get('service_account', 'auth_provider_x509_cert_url'),
            "client_x509_cert_url": config.get('service_account', 'client_x509_cert_url')
        }
        # Google Analytics Property ID
        property_id = config.get('google_analytics', 'property_id')
    except configparser.NoOptionError as e:
        print(f"Missing option in config file: {e}")
        sys.exit(1)

    credentials = service_account.Credentials.from_service_account_info(service_account_info)
    client = BetaAnalyticsDataClient(credentials=credentials)

    # Replace with your actual GA4 property ID
    property_id = "261912022"
    log_print(f"Using GA4 property ID: {property_id}")

    try: #Get total number of pages
        total_rows = get_total_row_count(client, property_id)
        log_print(f"Total rows to retrieve: {total_rows}")
    except Exception as e:
        log_print(f"Failed to retrieve total row count: {e}")
        return

    limit = 1000  # Set the limit per page
    offset = 0
    more_pages = True
    data = []
    page_count = 0

    # First, retrieve total row count to calculate total pages (this part needs to be adjusted based on the actual method to get total rows)
    total_pages = (total_rows // limit) + (total_rows % limit > 0)
    log_print(f"Total pages estimated: {total_pages}")

    while more_pages:
        try:
            # Configure the request with pagination
            request = RunReportRequest(
                property=f"properties/{property_id}",
                dimensions=[
                    Dimension(name="date"),
                    Dimension(name="deviceCategory"),
                    Dimension(name="sessionSourceMedium"),
                    Dimension(name="sessionDefaultChannelGroup")
                ],
                metrics=[
                    Metric(name="screenPageViews"),
                    Metric(name="sessions"),
                    Metric(name="activeUsers"),
                    Metric(name="averageSessionDuration"),
                    Metric(name="engagedSessions"),
                    Metric(name="userEngagementDuration"),
                    Metric(name="newUsers"),
                    Metric(name="eventCount")
                ],
                date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
                limit=limit,
                offset=offset
            )
            response = client.run_report(request)
            page_count += 1
            log_print(f"Page {page_count} of data retrieved.")

            # Process the response into a pandas DataFrame
            if response.rows:
                for row in response.rows:
                    data.append({
                        "Date": row.dimension_values[0].value,
                        "Device Category": row.dimension_values[1].value,
                        "Source Medium": row.dimension_values[2].value,
                        "Default Channel Group": row.dimension_values[3].value,
                        "Views": int(row.metric_values[0].value),
                        "Sessions": int(row.metric_values[1].value),
                        "Active Users": int(row.metric_values[2].value),
                        "Average Session Duration": str(row.metric_values[3].value),
                        "Engaged Sessions": int(row.metric_values[4].value),
                        "User Engagement": int(row.metric_values[5].value),
                        "New Users": int(row.metric_values[6].value),
                        "Event Count": int(row.metric_values[7].value)
                    })
            
            # Check if more pages are needed
                if len(response.rows) < limit:
                    more_pages = False
                else:
                    offset += limit
            else:
                more_pages = False

        except Exception as e:
            log_print(f"Error retrieving report: {e}")
            return None

    log_print("All data processing complete.")
    df = pd.DataFrame(data)
    
    # Save DataFrame to Excel
    output_file_name = f"GA4_data_{start_date}_{formatted_end_date}.xlsx"
    output_file_path = f'{folder_path}/Ga4data/{output_file_name}'
    try:
        df.to_excel(output_file_path, index=False)
        log_print(f"DataFrame saved successfully to {output_file_path}.")
    except Exception as e:
        log_print(f"Failed to save DataFrame: {e}")

    return df

# Function call (assuming you have an 'input_table' argument)
result_df = get_ga4_data(None)
