import requests
import json
import os
import datetime

def query_newsdata_api(API_KEY = "YOUR_API_KEY", params = None, verbose = False):
    # Define the API endpoint (e.g., for news articles)
    BASE_URL = "https://newsdata.io/api/1/archive"

    # Define parameters for the API request (e.g., search query, language, etc.)
    if params == None:
        params = {
            "apikey": API_KEY,
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

    API_KEY = os.environ['NEWSDATA_TOKEN']
    to_date = str(datetime.date.today()-datetime.timedelta(days=1))
    from_date = str(datetime.date.today()-datetime.timedelta(days=8))
    queries = ["semiconductor","AI"]
   
    data = {}
    for q in queries:
        params = {
                "apikey": API_KEY,
                "q": q,
                "language": "en",
                "from_date": from_date,
                "to_date": to_date,
                 }

        data[q] = query_newsdata_api(API_KEY,params=params)
        json_file = 'data/'+'news_text__'+'q'+q +to_date[4:].replace('-','') + from_date[4:].replace('-','')+ '__.json'
        txt_file = 'data/'+'news_text__'+'q'+q +to_date[4:].replace('-','') + from_date[4:].replace('-','')+ '__.txt'
        with open(json_file, 'w') as file:
            json.dump(data[q], file, indent=4)
        
        res = data[q]['results']
        tostr = lambda s: s if type(s)==str else ""
        content_list = [ tostr(r['content'])  for r in res]
        content = '</s> <s>'.join(content_list)
        with open(txt_file, "w") as text_file:
            text_file.write(content)
