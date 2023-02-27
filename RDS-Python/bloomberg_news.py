import requests


def get_stories(topic):

    url = "https://bb-finance.p.rapidapi.com/market/auto-complete"

    querystring = {"query":topic}

    headers = {
        "X-RapidAPI-Key": "c3d4a335aamshb6bfa0a6441f1b9p170490jsn2d3c801bb851",
        "X-RapidAPI-Host": "bb-finance.p.rapidapi.com"
    }

    response = requests.request("GET", url, headers=headers, params=querystring)
    data_js = response.json()
    for items in data_js.news:
        print(items)
    print(response.json)

def get_story_details(story_id):

    url = "https://bb-finance.p.rapidapi.com/stories/detail"

    querystring = {"internalID":story_id}

    headers = {
        "X-RapidAPI-Key": "c3d4a335aamshb6bfa0a6441f1b9p170490jsn2d3c801bb851",
        "X-RapidAPI-Host": "bb-finance.p.rapidapi.com"
    }

    response = requests.request("GET", url, headers=headers, params=querystring)

    print(response.text)

get_story_details(story_id="RLRO10DWX2PU01")
#get_stories("corn")