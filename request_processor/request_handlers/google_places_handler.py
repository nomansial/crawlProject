import requests
import pandas as pd


def get_google_place_id(api_key, company_name):

    endpoint = "https://maps.googleapis.com/maps/api/place/textsearch/json"

#endpoint = "https://maps.googleapis.com/maps/api/place/findplacefromtext/json"
    params = {
        'query': company_name,
        'inputtype': 'textquery',
        'fields': 'place_id',
        'key': api_key,
    }

    try:

        response = requests.get(endpoint, params=params)
        print(response)
        response.raise_for_status()  # Raise an exception for bad responses (4xx or 5xx)

        # Parse the JSON response
        result = response.json()
        

        # Check if the API request was successful
        if result['status'] == 'OK':
            lat = result['results'][0]['geometry']['location']['lat']
            lng = result['results'][0]['geometry']['location']['lng']
            complete_address = result['results'][0]['formatted_address']
            place_id = result['results'][0]['place_id']
            return [place_id, lat, lng,complete_address]
        else:
            return None

    except requests.exceptions.RequestException as e:
        return f"Error making API request: {e}"
    