import extruct
import requests
import xml.etree.ElementTree as ET
import datetime
from mysql import connector
from socket import error as SocketError
import errno
import random
#from user_agent import generate_user_agent

# conn = connector.connect(
#         host="192.168.1.24",
#         user="homeAssistant",
#         password="HomeAssistantPassword#1",
#         database='diplomski'
# )

products=[]
existing_map=[]
user_agents = [
    "Mozilla/5.0",
    "Mozilla/5.0",
]
sitemap_url = 'https://www.univerzalno.com/sitemap_index.xml'
categoryId=0
productId=0
shopId=0


def download_sitemap(url):
    try:
        user_agent = random.choice(user_agents)
        headers = {'User-Agent': user_agent}
        response = requests.get(url,headers=headers)
        if response.status_code == 200:
            return response.content
        else:
            print(f"Failed to download sitemap. Status code: {response.status_code}")
            return None
    except SocketError as e:
        if e.errno != errno.ECONNRESET:
            raise # Not error we are looking for
        pass

def parse_sitemap(xml_content):
    if xml_content:
        root = ET.fromstring(xml_content)
        urls = [child[0].text for child in root if len(child) > 0]
        return urls
    else:
        return []

def extract_metadata(url):
    user_agent = random.choice(user_agents)
    headers = {'User-Agent': user_agent}
    response = requests.get(url,headers=headers)
    print(response)
    html_content = response.text
    metadata = extruct.extract(html_content, base_url=url, syntaxes=['json-ld', 'microdata', 'opengraph'], uniform=True)
    return metadata



# Add check if its nested dict

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



sitemap_content = download_sitemap(sitemap_url)


def parseAllProducts(sitemap_content):
    print("Getting products from sitemap")
    x = datetime.datetime.now()
    if sitemap_content:
        parsed_urls = parse_sitemap(sitemap_content)
        for url in parsed_urls:
            sitemap_content = download_sitemap(url)
            newParsed = parse_sitemap(sitemap_content)
            for url in newParsed:
                metadata = extract_metadata(url)
                result = get_nested_dictionary_by_key_value(metadata, '@type', 'Product')
                if result == None:
                    continue
                for offer in result.get('offers'):
                    print (offer)
                    # product = {
                    # 'name': result.get('name', ''),
                    # 'shopName':"Univerzalno",
                    # 'sku': result.get('sku', ''),
                    # 'url': result.get('url', ''),
                    # 'description': result.get('description', ''),
                    # 'availability': offer.get('availability', ''),
                    # 'price': offer.get('price', ''),
                    # 'priceCurrency': offer.get('priceCurrency', ''),
                    # 'date': str(x.day) +"/"+str(x.month)+"/"+str(x.year),
                    # }

                   # products.append(product)
                    # if len(products)>500:
                    #      return
                    # print('Update %d' % len(products), end='\r')
        print("Done Parsing Sitemap")
        print("....................")
    else:
        print("Sitemap download failed.")

products = []
sites=[["https://www.univerzalno.com/shop/apple-iphone-15-pro-max-256gb/","https://procomp.ba/iphone-mobiteli/25855-apple-iphone-15-pro-max-256gb-blue-titanium.html","https://imtec.ba/mobiteli/69387-apple-iphone-15-pro-max-256gb-blue-titanium.html"],["Univerzalno","Procomp","Imtec"]]
i=0
product_name=""
product_description=""
shop_name=""
product_url=""
product_availability=""
product_price=""
product_currency=""

for site in sites[0]:
    x = datetime.datetime.now()
    print(site)
    metadata = extract_metadata(site)
    result = get_nested_dictionary_by_key_value(metadata, '@type', 'Product')
    product_name=result.get("name")
    product_description= result.get("description")
    shop_name=(sites[1][i])
    product_url = result.get("url") 
    if isinstance(result.get("offers"),list):
        for offer in result.get("offers"):
            product_availability=offer.get("availability")
            product_price=offer.get("price")
            product_currency=offer.get("priceCurrency")
    else:
        offer=result.get("offers")
        product_availability=offer.get("availability")
        product_price=offer.get("price")
        product_currency=offer.get("priceCurrency")


    product = {
        'name':product_name,
        'product_description': product_description,
        'shop_name': shop_name,
        'url':product_url,
        'availability':product_availability,
        'price':product_price,
        'currency':product_currency,
        'date':str(x.year)+"-"+str(x.month)+"-"+str(x.day),
    }
    products.append(product)
    i+=1
    print(product)



def uploadProductsToDb(products,conn):
    x = datetime.datetime.now()
    print("Uploading to DB")
    cursor = conn.cursor()
        # Insert data into 'products' and 'product_details' tables
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
            cursor.execute("INSERT INTO products (name,description,category_id) VALUES (%s,%s,%s)",(product['name'],product['product_description'],categoryId,))
            productId=cursor.lastrowid
        cursor.execute("SELECT * FROM shop WHERE name=%s",(product['shop_name'],))
        shop = cursor.fetchone()
        if shop:
            shopId=shop[0]
        else:
            cursor.execute("INSERT INTO shop (name) VALUES (%s)",(product['shop_name'],))
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
    conn.commit()
    conn.close()
        

        
uploadProductsToDb(products,conn)