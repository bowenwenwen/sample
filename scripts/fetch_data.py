import requests
import json

def fetch_data(api_url):
    response = requests.get(api_url)
    response.raise_for_status()
    return response.json()

def save_data(data, file_path):
    with open(file_path, 'w') as file:
        json.dump(data, file)

if __name__ == "__main__":
    api_url = "https://api.example.com/data"
    data = fetch_data(api_url)
    save_data(data, "/path/to/save/data.json")
