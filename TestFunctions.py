import pandas as pd
import numpy as np
import requests
import json
import re

# Function to get information

# In this function only is needed the token because into the filter exist a property which set allowed to collect in True,
# for that reason, next times is not going to be necessary drop the allowed to collect in false. Every response going to collect 100 rows per response.

# Requiere: Key -> Token
# Returns: Dataframe with information

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
# 
# Required: Country two letters code -> Example: CO for Colombia, AR for Argentina, IE in this case for Ireland
# Returns: List of places of specified code country      

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

# Required: Country two letters code -> Example: GB for United Kingdom, but this case is special, because we need to add a filter region by England
# Returns: List of places of specified code country    

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

# This function set a condition to create the tuples between Countries and Cities, if there is a country and no city, is going to 
# set a tuple Country, - but if exist a City is going to set a tuple Country, City, otherwise, No Information - Empty
#
# Requierd: Country Column and two list to check into list in order to set the conditions.
# Returns: List of tuples with Country and City information

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

# This function set a condition to create the emails based on content into the symbols <>. 
# If there is no Email to process, set No email found
#
# Requierd: Emails column
# Returns: List of cleaned emails
 
def raw_emails(emails):

    clean_emails = []
    for email in emails:
        match = re.search(r'<(.*?)>', str(email))
        if match:
            clean_emails.append(match.group(1))
        else:
            clean_emails.append("No email found")

    return clean_emails

# This function set a condition to clean the numbers in order to country and left zeros.
# If there is no Phone Number to process, set No Phone Number Found
#
# Requierd: Phone Column and condition column (in this case the Country Detection column)
# Returns: List of cleaned numbers
 

def clean_numbers(numbers, condition):

    only_numbers = numbers.str.replace('-', '', regex=True)
    cleaned_numbers = only_numbers.str.lstrip('0')
    
    result = np.where(condition == 'England', '(+44) ' + cleaned_numbers, 
             np.where(condition == 'Ireland', '(+353) ' + cleaned_numbers, 'No Phone Number Found'))
    
    return result

# This function set a condition to manage duplicated.
# In this case, instead of droping duplicates, I'm using a sort values by Technical_test_created_date, grouping by Full Name
# and put the columns to do the final upload to HubSpot
# 
# Its really important setting the first in the values because that is going to keep the most recently creation values 
# and the last is do the ";".join to the Industries to take all customer industries instead of droping it.
#
# Requierd: Dataframe, first condition (Technical_test_created_date) and secord condition (Fullname)
# Returns: Dataframe without duplicates management.

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

# This function upload the dataframe.
#
# Requierd: Dataframe and token
# Returns: Dataframe charged

def upload_contacts_to_hubspot(df, key):

    url = "https://api.hubapi.com/crm/v3/objects/contacts"

    def create_info(contact_data, key):
        headers = {
            "Authorization": f"Bearer {key}",
            "Content-Type": "application/json"
        }
        response = requests.post(url, headers=headers, data=json.dumps(contact_data))
        return response

    for index, row in df.iterrows():
        contact_data = {
            "properties": {
                "temporary_id": row['Temporary ID'],
                "email": row['Email'],
                "phone": row['Phone'],
                "country": row['Country'],
                "city": row['City'],
                "firstname": row['First Name'],
                "lastname": row['Last Name'],
                "address": row['Address'],
                "original_created_date": row['Original Created Date'],
                "industry": row['Original Industry'],
            }
        }
        
        create_info(contact_data, key)