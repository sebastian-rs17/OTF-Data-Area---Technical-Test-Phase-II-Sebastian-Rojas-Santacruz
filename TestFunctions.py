import pandas as pd
import numpy as np
import requests

# Function to get information

# In this function only is needed the token because into the filter exist a property which set allowed to collect in True,
# for that reason, next times is not going to be necessary drop the allowed to collect in false. Every response going to collect 100 rows per response.

def getInfo_ATC_True(key):
    api_key = key
    url = "https://api.hubapi.com/crm/v3/objects/contacts/search"
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json"
    }

    query = {
        "filterGroups": [
            {
                "filters": [
                    {
                        "propertyName": "allowed_to_collect",
                        "operator": "EQ",
                        "value": "true"
                    }
                ]
            }
        ],
        "properties": [
            "hs_object_id",
            "firstname",
            "lastname",
            "raw_email",
            "phone",
            "country",
            "technical_test___create_date",
            "industry",
            "address",
            "allowed_to_collect"
        ],
        "limit": 100,
        "after": 0
    }

    contacts = []
    continuar = True

    while continuar:
        response = requests.post(url, headers=headers, json=query)

        if response.status_code == 200:
            results = response.json()
            for contact in results["results"]:
                properties = contact["properties"]
                contacts.append({
                    "ID": properties.get("hs_object_id"),
                    "First Name": properties.get("firstname"),
                    "Last Name": properties.get("lastname"),
                    "Email": properties.get("raw_email"),
                    "Phone": properties.get("phone"),
                    "Country": properties.get("country"),
                    "Technical_test_created_date": properties.get("technical_test___create_date"),
                    "Industry": properties.get("industry"),
                    "Address": properties.get("address"),
                    "Allowed to Collect": properties.get("allowed_to_collect"),
                })
            
            
            continuar = results.get("hasMore", True)
            if continuar:

                if "paging" in results and "next" in results["paging"]:
                    query["after"] = results["paging"]["next"]["after"]
                else:
                    continuar = False
        else:
            print("Error:", response.status_code)
            break

    df = pd.DataFrame(contacts)
    return df

# Function to country detection

# In this functions we dont need to create a manual dictionary because we are going to search the cities based on a list of cities or places into that countries
# for that reason, the function is going to search into list.
# 
# Anyways, we can create the dictionary to do it simplier.                

def irl_detection(country):

    headers = {'Authorization': 'Bearer API_TOKEN'}
    irl_info = requests.get(f'https://brightdata.com/api/cities?country={country}', headers=headers)

    irl_locations = irl_info.json()

    irl_places = []
    n = 0

    for n in range(0, len(irl_locations)):
        irl_place = irl_locations[n]['name']
        irl_places.append(irl_place)

    def irl_cities_recognition(irl_places):
        cities_list = []
        cities = set(irl_places)
        for city in cities:
            cities_list.append(city)

        return cities_list

    irl_places_values = irl_cities_recognition(irl_places)
    return irl_places_values

def eng_detection(country):

    headers = {'Authorization': 'Bearer API_TOKEN'}
    eng_info = requests.get(f'https://brightdata.com/api/cities?country={country}', headers=headers)

    eng_locations = eng_info.json()

    eng_places = []
    n = 0

    for eng_filter in range(0, len(eng_info.json())):
        if eng_info.json()[eng_filter]['region'] == 'England':
            eng_place = eng_locations[eng_filter]['name']
            eng_places.append(eng_place)

    for n in range(0, len(eng_locations)):
        eng_place = eng_locations[n]['name']
        eng_places.append(eng_place)

    def eng_cities_recognition(eng_places):
        cities_list = []
        cities = set(eng_places)
        for city in cities:
            cities_list.append(city)

        return cities_list

    eng_places_values = eng_cities_recognition(eng_places)
    return eng_places_values


# def detect_country(country, eng_list, irl_list):
#     clean_country = np.where(country == 'England', 'England', 
#                     np.where(country == 'Ireland', 'Ireland',
#                     np.where(country.isin(eng_list), ('England', country),
#                     np.where(country.isin(irl_list), ('Ireland', country), 'No Information - Empty'))))
#     return clean_country


import numpy as np

def detect_country(country, eng_list, irl_list):

    results = []

    for c in country:
        if c == 'England':
            results.append(('England', '-'))
        elif c == 'Ireland':
            results.append(('Ireland', '-'))
        elif c in eng_list:
            results.append(('England', c))
        elif c in irl_list:
            results.append(('Ireland', c))
        else:
            results.append('No Information - Empty')

    return results

# Function to found emails

def raw_emails(emails):
    import re
    clean_emails = []
    for email in emails:
        match = re.search(r'<(.*?)>', str(email))
        if match:
            clean_emails.append(match.group(1))
        else:
            clean_emails.append("No email found")

    return clean_emails

def clean_numbers(numbers, condition):

    result = np.where(condition == 'England', '(+44) ' + numbers.str.replace('-', '', regex = True), 
             np.where(condition == 'Ireland', '(+353) ' + numbers.str.replace('-', '', regex = True), 'No Phone Number Found'))
    
    return result

def duplicates_managment(df, condition, condition2):

    df = df.sort_values(by=condition, ascending = False).groupby(condition2).agg({
        'First Name': 'first',
        'Last Name': 'first',
        'ID': 'first',
        'Technical_test_created_date': 'first',
        'Address': 'first',
        'Country':'first',
        'City':'first',
        'Country City Detection': 'first',
        'Country Detection': 'first',
        'Raw Email': 'first',
        'Assigned Number': 'first',
        'Industry': ';'.join}).reset_index()

    df.rename(columns={'Raw Email':'Email',
                       'Assigned Number': 'Phone',
                       'Technical_test_created_date': 'Original Created Date',
                       'Industry': 'Original Industry',
                       'ID': 'Temporary ID'}, inplace=True)

    return df