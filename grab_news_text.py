import requests
import json
import os
import datetime

def query_newsdata_api(API_KEY = "YOUR_API_KEY", params = None, verbose = False, pagination_depth=5):
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

def deduplicate(titles):
    mapp = {}
    value_holder = set()
    for i in titles:
        if titles[i] not in value_holder:
            mapp[i] = titles[i]
            value_holder.add(titles[i])
    return mapp

homogenize = lambda string: string.lower().lower().replace("’","").replace("'","").replace("‘","")
tostr = lambda s: s if type(s)==str else ""
    
if __name__ == '__main__':

    API_KEY = os.environ['NEWSDATA_TOKEN']
    to_date = str(datetime.date.today()-datetime.timedelta(days=1))
    from_date = str(datetime.date.today()-datetime.timedelta(days=30))
    queries = ["semiconductor"]
    pagination_depth = 400
    content = ''
   
    data = {}
    num_articles = 0
    for q in queries:
        params = {
                "apikey": API_KEY,
                "q": q,
                "language": "en",
                "from_date": from_date,
                "to_date": to_date,
                 }

        for i in range(pagination_depth):
            data[q] = query_newsdata_api(API_KEY,params=params)
            if i%10 == 0:
                print(f'parsing {data[q]['totalResults']} articles for query "{q}" on page {i+1}/{pagination_depth}')
            params['page'] = data[q]['nextPage']
            json_file = 'data/'+'news_text__'+'q'+q[:4]+'_' +to_date[4:].replace('-','') + from_date[4:].replace('-','')+ '__' + str(i)+'.json'
            txt_file = 'data/'+'news_text__'+'q'+q[:4]+'_' +to_date[4:].replace('-','') + from_date[4:].replace('-','')+ '__.txt'
            with open(json_file, 'w') as file:
                json.dump(data[q], file, indent=4)
           
            # deduplicate articles by title before appending text to content
            titles = {i:homogenize(r['title']) for i,r in enumerate(data[q]['results'])}
            titles = deduplicate(titles) 
            res = [data[q]['results'][i] for i in titles.keys()]
            content_list = [ tostr(r['content'])  for r in res]
            content_list = [c for c in content_list if len(c) > 1000]
            content += '\n'.join(content_list)
            num_articles += len(content_list)
        with open(txt_file, "w") as text_file:
            text_file.write(content)
    
        print(f'number of articles matching query "{params['q']}": {data[q]['totalResults']} ')
        print(f'number of articles reached through pagination with depth {pagination_depth}: {50*pagination_depth}')
        print(f'number of articles after deduplication: {num_articles}')
