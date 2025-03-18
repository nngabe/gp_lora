import requests
import json

def query_newsdata_api(API_KEY = "YOUR_API_KEY", params = None, verbose=False):
    # Define the API endpoint (e.g., for news articles)
    BASE_URL = "https://newsdata.com/api/1/news"

    # Define parameters for the API request (e.g., search query, language, etc.)
    if params == None:
        params = {
            "apikey": API_KEY,
            "q": "technology",  # Example search query
            "language": "en"   # Example language
        }

    try:
        # Make the API request
        response = requests.get(BASE_URL, params=params)
        response.raise_for_status()  # Raise an exception for bad status codes

        # Parse the JSON response
        data = response.json()

        # Print the response (or process it as needed)
        if verbose: print(json.dumps(data, indent=4))
        
        return data

    except requests.exceptions.RequestException as e:
        if verbose: print(f"An error occurred: {e}")

        return e
    
if __name__ == '__main__':

    # Replace with your actual API key
    API_KEY = "XYZ"
    data = query_newsdata_api(API_KEY)

