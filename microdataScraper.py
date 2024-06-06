import pandas as pd
import requests
from w3lib.html import get_base_url
import time
from fake_useragent import UserAgent
from urllib.parse import urlparse
from bs4 import BeautifulSoup
import extruct
from socket import error as SocketError
import errno
import xml.etree.ElementTree as ET
import datetime
from w3lib.html import get_base_url
from mysql import connector
import os

# Global declarations

ALL_URLS=[]
PRODUCTS=[]
HOST = os.environ.key('dbHost')
USER = os.environ.key('dbUser')
PASSWORD = os.environ.key('dbPass')
DATABASE = os.environ.keys('dbDatabase')
MYSQLCONNECTION = connector.connect(
        host=HOST,
        user=USER,
        password=PASSWORD,
        database=DATABASE
)
namespaces = {'ns': 'http://www.sitemaps.org/schemas/sitemap/0.9'}

# Handle Headers and user agents
def getUserAgent():
    ua = UserAgent()
    headers = {'User-Agent': ua.random}
    return headers

# Get sitemap

def getSiteMap(url):
    try:
        response = requests.get(url,headers=getUserAgent())
        if response.status_code == 200:
            return response.content
        else:
            print(f"Failed to download sitemap. Status code: {response.status_code}")
            print(response.content)
            return None
    except SocketError as e:
        if e.errno != errno.ECONNRESET:
            raise
        pass    



# Parse sitemap

def parseSitemap(sitemapContent):
    if sitemapContent:
        root = ET.fromstring(sitemapContent)
        urls = [child[0].text for child in root if len(child) > 0]
        return urls
    else:
        return [] 


# Get content from sitemap urls

def getSecondLayerOfSitemap(parsedSitemap):
    print("Getting second layer of sitemap")
    product_urls = [url for url in parsedSitemap if 'product-sitemap' in url]
    for url in product_urls:
        sitemapContent = getSiteMap(url)
        layerOneContent = parseSitemap(sitemapContent)
        ALL_URLS.append(layerOneContent)
    i = 0
    while i < len(ALL_URLS):
        if isinstance(ALL_URLS[i], list):
            # Extend the main list with the nested list's elements
            ALL_URLS[i:i+1] = ALL_URLS[i]
        else:
            i += 1
    return ALL_URLS
# Extract content from url (html/microdata)

def extractMetadata(url):
    response = requests.get(url, headers=getUserAgent())
    base_url = get_base_url(response.text, response.url)
    htmlContent = response.content
    metadata = extruct.extract(htmlContent,base_url=base_url)
    return metadata

# Parse content


def get_nested_dictionary_by_key_value(dictionary, target_key, target_value):
    
    for key in dictionary:
        if isinstance(dictionary[key], list):
            for item in dictionary[key]:
                if isinstance(item, dict):
                    if target_key in item and item[target_key] == target_value:
                        return item
                    else:
                        nested_result = get_nested_dictionary_by_key_value(item, target_key, target_value)
                        if nested_result:
                            return nested_result
    pass
# Upload to database

def uploadProductsToDb(products):
    x = datetime.datetime.now()
    print("Uploading to DB")
    cursor = MYSQLCONNECTION.cursor()
    for product in products:
        cursor.execute("SELECT id,category_name FROM categories WHERE category_name = %s", (product['name'],))
        category = cursor.fetchone()
        if category:
            categoryId=category[0]
        else: 
            cursor.execute("INSERT INTO categories (category_name) VALUES (%s)",(product['name'],))
            categoryId=cursor.lastrowid
        cursor.execute("SELECT * FROM products WHERE NAME=%s AND category_id=%s",(product['name'],categoryId,))
        parsedProduct=cursor.fetchone()
        if parsedProduct:
            productId=parsedProduct[0]
        else:
            cursor.execute("INSERT INTO products (name,description,category_id) VALUES (%s,%s,%s)",(product['name'],product['description'],categoryId,))
            productId=cursor.lastrowid
        cursor.execute("SELECT * FROM shop WHERE name=%s",(product['shopName'],))
        shop = cursor.fetchone()
        if shop:
            shopId=shop[0]
        else:
            cursor.execute("INSERT INTO shop (name) VALUES (%s)",(product['shopName'],))
            shopId=cursor.lastrowid
        cursor.execute("SELECT * FROM shop_items WHERE product_id = %s AND shop_id = %s",(productId,shopId,))
        shopItem=cursor.fetchone()
        if shopItem is None:
            cursor.execute("INSERT INTO shop_items (shop_id,product_id,url,availability,last_entry_date) VALUES (%s,%s,%s,%s,%s)",(shopId,productId,product['url'],product['availability'],product['date'],))
            shopItemId=cursor.lastrowid
        elif shopItem[3] or shopItem[4] or shopItem[5]:
            cursor.execute("UPDATE shop_items SET url=%s,availability=%s,last_entry_date=%s WHERE product_id=%s AND shop_id = %s",(product['url'],product['availability'],product['date'],productId,shopId,))
            shopItemId=cursor.lastrowid
        else:
            return
        cursor.execute("INSERT INTO prices (price,currency,date,product_id,shop_id) VALUES (%s,%s,%s,%s,%s)",(product['price'],product['currency'],product['date'],productId,shopId,))
    MYSQLCONNECTION.commit()


# Main

def scrapeSite(sitemapUrl):
    print("Getting products from sitemap")
    x = datetime.datetime.now()
    sitemap = getSiteMap(sitemapUrl)
    parsedSitemap = parseSitemap(sitemap)
    getSecondLayerOfSitemap(parsedSitemap)
    print("Fully parsed sitemap")
    PRODUCTS=[]
    batch_size = 50
    delay_seconds = 30
    for i in range(0,len(ALL_URLS), batch_size):
        batch = ALL_URLS[i:i + batch_size]
        for url in batch:
            print("Scraping url" + url)
            metadata = extractMetadata(url)
            result = get_nested_dictionary_by_key_value(metadata, '@type', 'Product')
            if result == None:
                continue
            for offer in result.get('offers'):
                product = {
                'name': result.get('name'),
                'shopName': offer.get("seller").get("name"),
                'sku': result.get('sku'),
                'url': result.get('url'),
                'description': result.get('description'),
                'availability': offer.get('availability'),
                'price': offer.get('price'),
                'currency': offer.get('priceCurrency'),
                'date': str(x.year) +"/"+str(x.month)+"/"+str(x.day),
                }
                PRODUCTS.append(product)
        print(f"Batch {i//batch_size + 1} processed, uploading to DB, waiting for {delay_seconds} seconds...")
        uploadProductsToDb(PRODUCTS)
        PRODUCTS = []
        time.sleep(delay_seconds)

if __name__ == '__main__':

    SITEMAPURL = 'https://www.univerzalno.com/sitemap_index.xml'
    scrapeSite(SITEMAPURL)
    MYSQLCONNECTION.close()
