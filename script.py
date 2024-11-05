import pandas as pd
import requests
import time
import os
import logging

# Configuration
OKTA_DOMAIN = 'your_okta_domain'  # Replace with your Okta domain, e.g., 'yourcompany.okta.com'
API_TOKEN = os.environ.get('OKTA_API_TOKEN')  # Ensure this environment variable is set securely

# Rate limiting configuration
OKTA_RATE_LIMIT_PER_MINUTE = 6000  # Okta API rate limit per minute
USAGE_PERCENTAGE = 0.8  # Use 80% of the rate limit to allow leeway
REQUESTS_PER_MINUTE = int(OKTA_RATE_LIMIT_PER_MINUTE * USAGE_PERCENTAGE)
REQUESTS_PER_SECOND = REQUESTS_PER_MINUTE / 60

# Headers for the API request
HEADERS = {
    'Authorization': f'SSWS {API_TOKEN}',
    'Accept': 'application/json',
    'Content-Type': 'application/json'
}

# Logging configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("processing.log"),
        logging.StreamHandler()
    ]
)

def process_file(file_path):
    """
    Process a CSV file containing profile IDs by modifying it in place.
    """
    # Read the CSV file
    logging.info(f"Reading {file_path}...")
    df = pd.read_csv(file_path, dtype={'ProfileID': str})
    total_profiles = len(df)
    logging.info(f"Total profiles in {file_path}: {total_profiles}")

    # Ensure necessary columns exist
    if 'Exists' not in df.columns:
        df['Exists'] = None
    if 'UserID' not in df.columns:
        df['UserID'] = None
    if 'Status' not in df.columns:
        df['Status'] = None

    # Identify profiles that need to be processed
    profiles_to_process = df[df['Exists'].isnull()]
    remaining_profiles = len(profiles_to_process)
    logging.info(f"Profiles remaining to process in {file_path}: {remaining_profiles}")

    if remaining_profiles == 0:
        logging.info(f"All profiles in {file_path} have been processed.")
        return

    # Rate limiting variables
    request_interval = 1 / REQUESTS_PER_SECOND  # Time between requests in seconds
    last_request_time = 0

    # Process each profile ID
    for index, row in profiles_to_process.iterrows():
        profile_id = row['ProfileID']

        # Rate limiting
        elapsed_time = time.time() - last_request_time
        if elapsed_time < request_interval:
            sleep_time = request_interval - elapsed_time
            time.sleep(sleep_time)

        last_request_time = time.time()

        # Make API request
        url = f'https://{OKTA_DOMAIN}/api/v1/users'
        params = {'filter': f'profile.id eq "{profile_id}"'}
        retries = 3
        for attempt in range(retries):
            try:
                response = requests.get(url, headers=HEADERS, params=params, timeout=10)
                if response.status_code == 200:
                    users = response.json()
                    if users:
                        user_info = users[0]
                        df.at[index, 'Exists'] = 'Yes'
                        df.at[index, 'UserID'] = user_info['id']
                        df.at[index, 'Status'] = user_info['status']
                    else:
                        df.at[index, 'Exists'] = 'No'
                        df.at[index, 'UserID'] = None
                        df.at[index, 'Status'] = None
                    break  # Exit retry loop on success
                elif response.status_code == 429:
                    retry_after = int(response.headers.get('Retry-After', 1))
                    logging.warning(f"Rate limit exceeded. Retrying after {retry_after} seconds.")
                    time.sleep(retry_after)
                else:
                    logging.error(f"Error fetching profile ID {profile_id}: HTTP {response.status_code}")
                    df.at[index, 'Exists'] = 'Error'
                    df.at[index, 'UserID'] = None
                    df.at[index, 'Status'] = None
                    break  # Exit retry loop on client error
            except requests.exceptions.RequestException as e:
                logging.error(f"Exception for profile ID {profile_id}: {e}")
                time.sleep(2 ** attempt)
        else:
            # Failed after retries
            df.at[index, 'Exists'] = 'Error'
            df.at[index, 'UserID'] = None
            df.at[index, 'Status'] = None

        # Save progress periodically (every 100 records)
        if (index + 1) % 100 == 0 or (index + 1) == total_profiles:
            df.to_csv(file_path, index=False)
            logging.info(f"Processed {index + 1}/{total_profiles} profiles in {file_path}.")

    # Final save
    df.to_csv(file_path, index=False)
    logging.info(f"Finished processing {file_path}.")

def main():
    # Specify the input file path
    file_path = 'profiles.csv'  # Replace with the path to your CSV file

    try:
        process_file(file_path)
    except Exception as e:
        logging.error(f"Error processing {file_path}: {e}")

if __name__ == '__main__':
    main()
