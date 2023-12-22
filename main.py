from woocommerce import API
import pandas as pd
import json
from transliterate import slugify
import wget
import zipfile
from zipfile import ZipFile
from slugify import slugify
import os

shop_url = "https://****.ru"
consumer_key = "*****************************"
consumer_secret = "****************************"
wcapi = API(
    url=shop_url,
    consumer_key=consumer_key,
    consumer_secret=consumer_secret,
    version="wc/v3"
)
# количество товаров за раз ввод
batch_size = 100
# количество товаров за раз загрузка из базы
products_per_page = 100

pricezipfile='/home/ivan/price.zip'
pricecsv='/home/ivan/price.csv'
dira='/home/ivan/'
oldbase='/home/ivan/site.csv'

# количество товаров за раз ввод
batch_size = 100
# количество товаров за раз загрузка из базы
products_per_page = 100
#наценка
nacenka=1.12

pricezipfile='/home/ivan/price.zip'
pricecsv='/home/ivan/price.csv'
dira='/home/ivan/'
oldbase='/home/ivan/site.csv'

# получаем товары

# получаем общее количество товаров в магазине
total_products = wcapi.get("products", params={'per_page': products_per_page } ).headers["X-WP-Total"]
print(total_products)
total_pages = int(total_products) // products_per_page + 1
print (total_pages)

products = []

for page in range(1, total_pages + 1):
    # Получаем товары для текущей страницы
    response = wcapi.get("products", params={'per_page': products_per_page , 'page' : page } )
    print(page)
    # Добавляем товары к общему списку
    products.extend(response.json())

#print(products)
df1 = pd.DataFrame(products)
df1.to_csv(oldbase,index=False)

print('Beginning file download with wget module')
url = 'https://price.autoeuro.ru/PriceAE_(4873159461).zip?1703024450'
try:
    wget.download(url, '/home/ivan/price.zip')
    print('Succes download price :) ')
except:
        print ('Fail to download price :( ')

price_zip = ZipFile(pricezipfile)
price_zip.printdir()
print(price_zip.namelist()[0])
price_zip.extract(price_zip.namelist()[0],dira)
os.rename (dira+price_zip.namelist()[0],pricecsv)
print('Файл выгружен в csv')
os.remove(pricezipfile)
print ('удалили старый zip')

#мержим два df что бы узнать че как новое и удаленное
df2=pd.read_csv(pricecsv, encoding='UTF-8')
print (df2)
df_merged = df1.merge(df2, left_on='sku', right_on='КаталожныйНомер', how='left')
df_merged.to_csv('/home/ivan/merge.csv',index=False)

# создаем отдельный df для создания товаров
df_to_create = df2[~df2['КаталожныйНомер'].isin(df1['sku'])]
df_to_create.to_csv('/home/ivan/create.csv',index=False)

# создаем отдельный df для отключение
df_to_disable = df1[~df1['sku'].isin(df_merged['КаталожныйНомер'])]
df_to_disable.loc[:, 'status'] = 'draft'
df_to_disable.to_csv('/home/ivan/disable.csv',index=False)

# выбираем минимальный набор столбцов для создания новых товаров
df_to_create = df_to_create[['Производитель','КаталожныйНомер','НомерПроизводителя','ОригинальныйНомер', 'Применение', 'Цена', 'МинУпаковка','Наличие']]

print('считаем цены новых товаров')
df_to_create['price']=round(df_to_create['Цена']*nacenka , 2)

# переименовываем для json
df_to_create = df_to_create.rename(
    columns={
#        "НомерПроизводителя": "name",
#        "Применение": "short_description",
#        "Цена": "price",
        "КаталожныйНомер": "sku",
        "Наличие": "stock_quantity"
    })
# добавим генерацию чпу
#df_to_create['slug'] = df_to_create['name'].apply(slugify)
df_to_create.to_csv('/home/ivan/create2.csv',index=False)

products_to_create = json.loads(df_to_create.to_json(orient="records"))

