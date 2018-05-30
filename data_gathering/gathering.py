"""
ЗАДАНИЕ

Выбрать источник данных и собрать данные по некоторой предметной области.

Цель задания - отработать навык написания программ на Python.
В процессе выполнения задания затронем области:
- организация кода в виде проекта, импортирование модулей внутри проекта
- unit тестирование
- работа с файлами
- работа с протоколом http
- работа с pandas
- логирование

Требования к выполнению задания:

- собрать не менее 1000 объектов

- в каждом объекте должно быть не менее 5 атрибутов
(иначе просто будет не с чем работать.
исключение - вы абсолютно уверены что 4 атрибута в ваших данных
невероятно интересны)

- сохранить объекты в виде csv файла

- считать статистику по собранным объектам


Этапы:

1. Выбрать источник данных.

Это может быть любой сайт или любое API

Примеры:
- Пользователи vk.com (API)
- Посты любой популярной группы vk.com (API)
- Фильмы с Кинопоиска
(см. ссылку на статью ниже)
- Отзывы с Кинопоиска
- Статьи Википедии
(довольно сложная задача,
можно скачать дамп википедии и распарсить его,
можно найти упрощенные дампы)
- Статьи на habrahabr.ru
- Объекты на внутриигровом рынке на каком-нибудь сервере WOW (API)
(желательно англоязычном, иначе будет сложно разобраться)
- Матчи в DOTA (API)
- Сайт с кулинарными рецептами
- Ebay (API)
- Amazon (API)
...

Не ограничивайте свою фантазию. Это могут быть любые данные,
связанные с вашим хобби, работой, данные любой тематики.
Задание специально ставится в открытой форме.
У такого подхода две цели -
развить способность смотреть на задачу широко,
пополнить ваше портфолио (вы вполне можете в какой-то момент
развить этот проект в стартап, почему бы и нет,
а так же написать статью на хабр(!) или в личный блог.
Чем больше у вас таких активностей, тем ценнее ваша кандидатура на рынке)

2. Собрать данные из источника и сохранить себе в любом виде,
который потом сможете преобразовать

Можно сохранять страницы сайта в виде отдельных файлов.
Можно сразу доставать нужную информацию.
Главное - постараться не обращаться по http за одними и теми же данными много раз.
Суть в том, чтобы скачать данные себе, чтобы потом их можно было как угодно обработать.
В случае, если обработать захочется иначе - данные не надо собирать заново.
Нужно соблюдать "этикет", не пытаться заддосить сайт собирая данные в несколько потоков,
иногда может понадобиться дополнительная авторизация.

В случае с ограничениями api можно использовать time.sleep(seconds),
чтобы сделать задержку между запросами

3. Преобразовать данные из собранного вида в табличный вид.

Нужно достать из сырых данных ту самую информацию, которую считаете ценной
и сохранить в табличном формате - csv отлично для этого подходит

4. Посчитать статистики в данных
Требование - использовать pandas (мы ведь еще отрабатываем навык использования инструментария)
То, что считаете важным и хотели бы о данных узнать.

Критерий сдачи задания - собраны данные по не менее чем 1000 объектам (больше - лучше),
при запуске кода командой "python3 -m gathering stats" из собранных данных
считается и печатается в консоль некоторая статистика

Код можно менять любым удобным образом
Можно использовать и Python 2.7, и 3

Зачем нужны __init__.py файлы
https://stackoverflow.com/questions/448271/what-is-init-py-for

Про документирование в Python проекте
https://www.python.org/dev/peps/pep-0257/

Про оформление Python кода
https://www.python.org/dev/peps/pep-0008/


Примеры сбора данных:
https://habrahabr.ru/post/280238/

Для запуска тестов в корне проекта:
python3 -m unittest discover

Для запуска проекта из корня проекта:
python3 -m gathering gather
или
python3 -m gathering transform
или
python3 -m gathering stats


Для проверки стиля кода всех файлов проекта из корня проекта
pep8 .

"""

import logging

import sys

import vk

#from datetime import datetime

from datetime import date

from datetime import datetime

import dateutil

import re

import csv

import time

import pandas as pd

import numpy as np

#import parse

from scrappers.scrapper import Scrapper
from storages.file_storage import FileStorage

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


SCRAPPED_FILE = 'scrapped_data.txt'
TABLE_FORMAT_FILE = 'data.csv'

#расчет возраста
def calculate_age(birth_date):
    
    match_res0 = re.fullmatch(r'[0-3][0-9].[0-1][0-2].[0-9][0-9][0-9][0-9]',birth_date)
    match_res1 = re.fullmatch(r'[0-9].[0-1][0-2].[0-9][0-9][0-9][0-9]',birth_date)
    match_res2 = re.fullmatch(r'[0-3][0-9].[0-9].[0-9][0-9][0-9][0-9]',birth_date)
    match_res3 = re.fullmatch(r'[0-9].[0-9].[0-9][0-9][0-9][0-9]',birth_date)
    
    if match_res0 == None and match_res1 == None and match_res2 == None and match_res3 == None :
        return ''
    
    today = date.today()
    
    b_date = datetime.strptime(birth_date, '%d.%m.%Y').date()
    
    age = dateutil.relativedelta.relativedelta(today, b_date)
    
    return age.years

def gather_process():
    logger.info("gather")
    storage = FileStorage(SCRAPPED_FILE)
    
    print("Hello")
    
    session = vk.Session()
    vk_api = vk.API(session)

    members = vk_api.groups.getMembers(group_id = 'bolshe_buketa',v=5)

    i = 0
    with open('list_to_csv.csv', 'w', newline='') as csv_file:
        csv_writer = csv.writer(csv_file)
        for vk_user_id in members['users']:
            
            time.sleep(1)
            
            user = vk_api.users.get(user_id=vk_user_id,v=5, fields = 'name, online,bdate,city,sex,about,connections,contacts')[0]
            
            if 'home_phone' in user:
                user['home_phone'] = user['home_phone'].replace('\u2665','').replace('\u2605','').replace('\u260e','').replace(':','').replace(',','')
                
            if 'about' in user:
                user['about'] = user['about'].replace('\u2665','').replace('\u2605','').replace('\u260e','').replace(':','').replace(',','')
            
            if 'city' in user:
                
                city = vk_api.database.getCitiesById(city_ids = user['city'],v=5)
                 
                if user['city'] != 0:
                    user['city_name'] = city[0]['title'].replace(':','')
                else:
                    user['city_name'] = ''
                
                del user['city']
            i = i+1
            print(i)
            print(user)
            try:
                csv_writer.writerow([user])
            except:
                user['about'] = 'Не удалось декодировать и записать'
                try:
                    csv_writer.writerow([user])
                except:
                    user['home_phone'] = 'Не удалось декодировать и записать'
                    csv_writer.writerow([user])
                    
    print('Done')

    # You can also pass a storage
    scrapper = Scrapper()
    scrapper.scrap_process(storage)                   
                
def convert_data_to_table_format():
    logger.info("transform")

    with open('list_to_csv.csv', "r") as f_obj:
        reader = csv.reader(f_obj)

        user_frame = pd.DataFrame(columns=['user_id','first_name','last_name','age','city','sex','flower_in_about','got_about',
                                  'got_skype','got_twitter','got_instagram','got_homephone'])
        for user in reader:

            user_dict = {}
            l = user[0].replace('{','').replace('}','').split(",")
            user_dict_file = {k.replace("'","").strip():v.replace("'","").strip() for k,v in (el.split(':') for el in l)}
            
            user_dict['user_id'] = user_dict_file['id']
            user_dict['first_name'] = user_dict_file['first_name']
            user_dict['last_name'] = user_dict_file['last_name']
            
            if 'bdate' in user_dict_file:
                user_dict['age'] = calculate_age(user_dict_file['bdate'])
            else:
                user_dict['age'] = ''
            
            if 'city_name' in user_dict_file:
                user_dict['city'] = user_dict_file['city_name']
            else:
                user_dict['city'] = ''
            
            if 'sex' in user_dict_file:
                user_dict['sex'] = user_dict_file['sex']
            else:
                user_dict['sex'] = 0
            
            if 'about' in user_dict_file:
                if ('цветы' in user_dict_file['about']) or ('цветок' in user_dict_file['about']) or ('роза' in user_dict_file['about']) or ('орхидея' in user_dict_file['about']):
                    user_dict['flower_in_about'] = 1
                else:
                    user_dict['flower_in_about'] = 0
            else:
                user_dict['flower_in_about'] = 0
            
            if 'about' in user_dict_file:
                if user_dict_file['about'] != '':
                    user_dict['got_about'] = 1
                else:
                    user_dict['got_about'] = 0
                    #del user_dict_file['about']
            else:
                user_dict['got_about'] = 0
            
            if 'skype' in user_dict_file:
                user_dict['got_skype'] = 1
            else:
                user_dict['got_skype'] = 0
            
            if 'twitter' in user_dict_file:
                user_dict['got_twitter'] = 1
            else:
                user_dict['got_twitter'] = 0
            
            if 'instagram' in user_dict_file:
                user_dict['got_instagram'] = 1
            else:
                user_dict['got_instagram'] = 0
            
            if 'home_phone' in user_dict_file:
                user_dict['got_homephone'] = 1
            else:
                user_dict['got_homephone'] = 0
            
            
            user_frame.loc[len(user_frame)] = user_dict

    
    user_frame.to_csv('dataframe.csv')
    # Your code here
    # transform gathered data from txt file to pandas DataFrame and save as csv

    pass


def stats_of_data():
    logger.info("stats")
    
    user_frame = pd.read_csv('dataframe.csv')
    
    age_user_frame = user_frame[pd.notnull(user_frame['age'])]
    
    print('average age = '+str(round(age_user_frame['age'].mean(),2)))
    print('maximum age = '+str(age_user_frame['age'].max()))
    print('minimum age = '+str(age_user_frame['age'].min()))
    
    print('female percentage = ' + str(user_frame[user_frame['sex'] == 1]['user_id'].count()/user_frame['user_id'].count()*100))
    print('male percentage = ' + str(user_frame[user_frame['sex'] == 2]['user_id'].count()/user_frame['user_id'].count()*100))
    
    print('users has about block percentage = ' + str(round(user_frame[user_frame['got_about'] == 1]['user_id'].count()/user_frame['user_id'].count()*100,2)))
    print('users that really love flowers percentage = ' + str(round(user_frame[user_frame['flower_in_about'] == 1]['user_id'].count()/user_frame['user_id'].count()*100,2)))
    
    print('users got skype percentage = ' + str(round(user_frame[user_frame['got_skype'] == 1]['user_id'].count()/user_frame['user_id'].count()*100,2)))
    print('users got skype average age = ' + str(round(age_user_frame[age_user_frame['got_skype'] == 1]['age'].mean(),2)))
    
    print('users got twitter about block percentage = ' + str(round(user_frame[user_frame['got_twitter'] == 1]['user_id'].count()/user_frame['user_id'].count()*100,2)))
    print('users got twitter average age = ' + str(round(age_user_frame[age_user_frame['got_twitter'] == 1]['age'].mean(),2)))
    
    print('users got instagram block percentage = ' + str(round(user_frame[user_frame['got_instagram'] == 1]['user_id'].count()/user_frame['user_id'].count()*100,2)))
    print('users got instagram average age = ' + str(round(age_user_frame[age_user_frame['got_instagram'] == 1]['age'].mean(),2)))
    
    print('users got homephone block percentage = ' + str(round(user_frame[user_frame['got_homephone'] == 1]['user_id'].count()/user_frame['user_id'].count()*100,2)))
    print('users got homephone average age = ' + str(round(age_user_frame[age_user_frame['got_homephone'] == 1]['age'].mean(),2)))

    # Your code here
    # Load pandas DataFrame and print to stdout different statistics about the data.
    # Try to think about the data and use not only describe and info.
    # Ask yourself what would you like to know about this data (most frequent word, or something else)


if __name__ == '__main__':
    """
    why main is so...?
    https://stackoverflow.com/questions/419163/what-does-if-name-main-do
    """
    logger.info("Work started")

    if sys.argv[1] == 'gather':
        gather_process()

    elif sys.argv[1] == 'transform':
        convert_data_to_table_format()

    elif sys.argv[1] == 'stats':
        stats_of_data()

    logger.info("work ended")
