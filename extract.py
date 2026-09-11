import requests
from dotenv import load_dotenv
import os
import json

load_dotenv()

API_KEY = os.getenv("API_KEY")
BASE_URL='https://api.restcountries.com/countries/v5'

try: 
    response = requests.get(
    f'{BASE_URL}?q=canada',
    headers={'Authorization': f'Bearer {API_KEY}'}
    )

    if response.status_code == 200:
        data = response.json()
        with open("sample_country.json", "w") as fn:
            json.dump(data, fn, indent=2)
            print("Successfully added JSON data into new json file")
    else:
        print(f"API Error Response: {response.text}")

except Exception as e:
    print(e)