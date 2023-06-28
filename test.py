import requests
import json

def get_paper_info(api_key, search_query):
    base_url = 'https://api.elsevier.com/content/search/scopus'
    headers = {
        'Accept':'application/json',
        'X-ELS-APIKey': api_key
    }
    params = {
        'query': search_query
    }
    response = requests.get(base_url, headers=headers, params=params)

    if response.status_code == 200:
        return json.loads(response.text)
    else:
        return None


api_key = '66cd78ce90f1a93a9fd8584a4d004a6d'
search_query = '"artificial intelligence" AND "climate change"'
papers = get_paper_info(api_key, search_query)

if papers:
    print(papers)
else:
    print('Failed to fetch data')