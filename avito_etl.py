
from bs4 import BeautifulSoup
import requests
import pandas as pd
import numpy as np
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
import pandas as pd
import io
from dotenv import load_dotenv
import os
# Function to extract Product Title
def get_title(soup):

    try:
        # Outer Tag Object
        title = soup.find("h1", attrs={"class":'sc-1g3sn3w-12 mnjON'})
        
        # Inner NavigatableString Object
        title_value = title.text

        # Title as a string value
        title_string = title_value.strip()

    except AttributeError:
        title_string = ""

    return title_string

# Function to extract Product Price
def get_price(soup):

    try:
        price = soup.find("p", attrs={'class':'sc-1x0vz2r-0 kzRRVw sc-1g3sn3w-13 kliyMh'}).text.strip()

    except AttributeError:

        try:
            # If there is some deal price
            price = soup.find("p", attrs={'class':'sc-1x0vz2r-0 kzRRVw sc-1g3sn3w-13 kliyMh'}).string.strip()

        except:
            price = ""

    return price

# Function to extract Product Rating
def get_location(soup):

    try:
        location = soup.find("span", attrs={'class':'sc-1x0vz2r-0 gCIGeB'}).text.strip()
    
    except AttributeError:
        try:
            location = soup.find("span", attrs={'class':'a-icon-alt'}).string.strip()
        except:
            location = ""	

    return location

# Function to extract Number of User Reviews
def get_time(soup):
    try:
        time = soup.find("time").string.strip()

    except AttributeError:
        time = ""	

    return time

# Function to extract Availability Status
def get_description(soup):
    try:
        description = soup.find("p", attrs={'class':'sc-ij98yj-0 iMUDvH'})
        description = description.text.strip()

    except AttributeError:
        description = "Not Available"	

    return description

# ETL function
def run_etl(search):
    load_dotenv()

    # add your user agent 
    usrAgnt = os.getenv("USER_AGENT")
    HEADERS = ({'User-Agent':usrAgnt, 'Accept-Language': 'en-US, en;q=0.5'})

    # The webpage URL
    search = search.replace(" ", "_")
    page = 1
    
    URL = f"https://www.avito.ma/fr/maroc/{search}--%C3%A0_vendre?o={page}"

    # HTTP Request
    webpage = requests.get(URL, headers=HEADERS)

    # Soup Object containing all data
    soup = BeautifulSoup(webpage.content, "html.parser")

    # Fetch links as List of Tag Objects
    links = soup.find_all("a", attrs={'class':'sc-jejop8-1 cYNgZe'})

    # Store the links
    links_list = []

    # Loop for extracting links from Tag Objects
    for link in links:
            links_list.append(link.get('href'))

    d = {"title":[], "price":[], "location":[], "time":[],"description":[]}
    
    # Loop for extracting product details from each link 
    for link in links_list:
        if link:
            new_webpage = requests.get(link, headers=HEADERS)

            new_soup = BeautifulSoup(new_webpage.content, "html.parser")

            # Function calls to display all necessary product information
            d['title'].append(get_title(new_soup))
            d['price'].append(get_price(new_soup))
            d['location'].append(get_location(new_soup))
            d['time'].append(get_time(new_soup))
            d['description'].append(get_description(new_soup))

    
    # Set up connection to Azure Blob Storage
    connection_string = os.getenv("CONN_STRING")
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)

    # Define blob details
    account_name = os.getenv("USERNAME")
    container_name = os.getenv("CONTAINER_NAME")
    blob_name = f"avito_{search}_data.csv"

    # Create a DataFrame (replace this with your DataFrame creation)
    amazon_df = pd.DataFrame.from_dict(d)
    amazon_df['title'].replace('', np.nan, inplace=True)
    amazon_df = amazon_df.dropna(subset=['title'])

    # Convert the DataFrame to CSV format
    csv_data = amazon_df.to_csv(index=False, sep=';')

    # Convert the CSV data to bytes
    csv_bytes = csv_data.encode("utf-8")

    # Get a blob client to upload the data
    container_client = blob_service_client.get_container_client(container_name)
    blob_client = container_client.get_blob_client(blob_name)

    # Upload the data to the blob
    blob_client.upload_blob(csv_bytes, overwrite=True)

    print("Blob uploaded successfully.")

    '''
    # To store csv file localy

    amazon_df = pd.DataFrame.from_dict(d)
    amazon_df['title'].replace('', np.nan, inplace=True)
    amazon_df = amazon_df.dropna(subset=['title'])
    amazon_df.to_csv("avito_data.csv", header=True, index=False, sep=';')
    '''

