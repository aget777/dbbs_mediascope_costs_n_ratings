#!/usr/bin/env python
# coding: utf-8

# In[1]:


# добавляем библиотеки для работы с ТВ индексом
get_ipython().run_line_magic('reload_ext', 'autoreload')
get_ipython().run_line_magic('autoreload', '2')

import os
import gc
import sys
import re
import json
from datetime import datetime, date, timedelta, time
import time
from dateutil.relativedelta import relativedelta
import warnings
import pandas as pd
import numpy as np

from sqlalchemy import create_engine
import pyodbc
import turbodbc
from turbodbc import connect

from IPython.display import JSON
from mediascope_api.core import net as mscore
from mediascope_api.mediavortex import tasks as cwt
from mediascope_api.mediavortex import catalogs as cwc

import config
import config_tv_index
from config_search_funcs import get_subbrand_id_str
from normalize_funcs import normalize_columns_types, append_custom_columns
from db_funcs import createDBTable, downloadTableToDB, get_mssql_table, removeRowsFromDB
from create_dicts import get_cleaning_dict, get_media_discounts
from create_dicts_tv_index import download_tv_index_default_dicts


# Cоздаем объекты для работы с TVI API
mnet = mscore.MediascopeApiNetwork()
mtask = cwt.MediaVortexTask()
cats = cwc.MediaVortexCats()

# Забриае название БД
db_name = config.db_name

subbrand_id_str = get_subbrand_id_str(mon_num='360', media_type='tv')
special_ad_filter = f'{config_tv_index.nat_tv_ad_filter} AND  or subbrandId IN ({subbrand_id_str})'
# nat_tv_ad_dict = config.nat_tv_ad_dict

# список метрик для отчета по Баинговой аудитории
# nat_tv_bying_statistics = config.nat_tv_bying_statistics

# создаем список с названиями типов медиа, по которым отфильтровать гугл докс с чисткой
# media_type_lst = ['TV']
# tv_index_dicts = config.tv_index_dicts
# start_date = '2023-01-01'
# start_date = datetime.strptime(start_date, '%Y-%m-%d').date()

# end_date = '2023-01-02'
# end_date = datetime.strptime(end_date, '%Y-%m-%d').date()

# print(f'start_date: {start_date} / end_date: {end_date}')


# In[ ]:





# In[2]:


# Включаем отображение всех колонок
pd.set_option('display.max_columns', None)
# Задаем ширину столбцов по контенту
pd.set_option('display.max_colwidth', None)
pd.set_option('display.max_rows', None)

warnings.simplefilter(action='ignore', category=FutureWarning)
# убираем лишние предупреждения
pd.set_option('mode.chained_assignment', None)

sep_str = '*' * 50


# In[ ]:





# In[ ]:





# In[ ]:


# основная функция для получения данных из Медиаскоп
# отчеты Simple / Buying
# если это первая загрузка, то создаем нужные таблицы в БД
# добавляем флаг Чистки из гугл докса / если объявление НЕ встречалось, ставим флаг 2
# номализуем данные - приводим к верхнему регистру / округляем до 2-х знаков после запятой
def get_nat_tv_reports(start_date='', end_date='', flag='regular'):
    start_time = datetime.now()
    print(f'Скрипт запущен {start_time}')
    
    if flag.lower()=='first':
        # специально не стал делать цикл, чтобы явно показать, какие именно создаем таблицы
        createDBTable(config.db_name, config_tv_index.nat_tv_simple , config_tv_index.nat_tv_simple_vars_list, flag='create')
        createDBTable(config.db_name, config_tv_index.nat_tv_buying , config_tv_index.nat_tv_buying_vars_list, flag='create')
        # создаем пустую таблицу-справочник объявлений
        createDBTable(config.db_name, config_tv_index.nat_tv_ad_dict , config_tv_index.nat_tv_ad_dict_vars_list, flag='create')
        # создаем и заполняем данными словари по умолчанию
        download_tv_index_default_dicts()
        
        # создаем пустые словари справочников через цикл
        # забираем из файла create_dicts - словарь, где 
        # ключ - это название поля из отчета Simpe(для дальнейшего удобства так сделано)
        # значение - это список, который содержит:
        # [0] - название таблицы в БД
        # [1] - список полей с типами данных для БД
        # [2] - список полей с целочисленными значениями для нормализации
        for key, value in config_tv_index.tv_index_dicts.items():
            createDBTable(config.db_name, value[0] , value[1], flag='create')

    # получаем данные из гугл диска с чисткой объявлений 
    # - передаем список Типов медиа, чтобы оставить нужные значения
    # custom_cleaning_dict = get_cleaning_dict(media_type_lst)
    # список полей из гугл таблицы Чистка, которые нужно добавить в справочник объявлений
    custom_cols_list = [col[:col.find(' ')] for col in config.custom_ad_dict_vars_list]
    # убираем некоторые поля, т.е. их заберем из отчета Simple
    custom_cols_list = list(set(custom_cols_list) - set(['media_type', 'include_exclude', 'ad_id']))
    custom_cols_str = ', '.join(custom_cols_list)
    
    print(sep_str)
    print('Забираем из БД НЕ ОБНОВЛЕННЫЙ справочник объявлений')
   
    query = f"select {custom_cols_str}  from {config_tv_index.nat_tv_ad_dict}"
    nat_tv_ad_dict_df = get_mssql_table(db_name, query=query)
    print(sep_str)
    
    # # Итоговый список полей, который нам нужен в словаре объявлений
    nat_tv_ad_dict_cols_list = [col[:col.find(' ')] for col in config_tv_index.nat_tv_ad_dict_vars_list]
    
# Если start_date НЕ передается в функцию - мы считаем, что это еженедельное обновление
# Данные в БД TV Index идут с отставанием 3 дня от текущей даты
# Нам необходимо перезаписать последние 14 дней
# Из текуще даты мы вычитаем 3 дня задержки и 14 дней для перезаписи
# это будет считаться датой начала загрузки
# удалаем в БД все строки ничиная с этой даты
    
    if not start_date:
        start_date = (datetime.now().date()  - timedelta(days=14+3))

# Если дата окончани загрузки НЕ задана, то мы считаем минус 3 дня от текущей даты
    if not end_date:
        end_date = (datetime.now().date()  - timedelta(days=3))
        

    
    start_date = datetime.strptime(str(start_date), '%Y-%m-%d').date()
    end_date = datetime.strptime(str(end_date), '%Y-%m-%d').date()


    # если это НЕ первая загрузка, то удаляем строки из БД начиная с даты начала текущей загрузки
    # для отчетов Buying - расходы приходят с запозданием на неделю, а так же страхуемся от возможных дублей в БД при новой загрузке
    # Например если сегодня 30-е число месяца
    # БД еще не обновилась, значит у нас даты до 23-го числа месяца
    # Причем с 16 по 23 Расходов НЕТ
    # мы запускаем обновление, т.е. забираем данные с 24 по 30
    # При этом нам нужно обновить Расхходы с 16 по 23
    
    if flag=='regular':
        cond = f"researchDate >= '{str(start_date)}' and researchDate <= '{str(end_date)}'"
        
        print()
        print(sep_str)
        print(f'Удалем строки из таблицы: nat_tv_simple и nat_tv_buying по условию: {cond}')
        print()
    
        removeRowsFromDB(config.db_name, config_tv_index.nat_tv_simple, cond)
        removeRowsFromDB(config.db_name, config_tv_index.nat_tv_buying, cond)
        print()
    # считаем кол-во дней в периоде
    # каждый день мы будем забирать по отдельности и записывать его в БД
    count_days = (end_date - start_date).days
    
    print()
    print(f'Загружаем отчет за период {start_date} - {end_date}. Общее количество дней: {count_days+1}')
    print(sep_str)
    print()
    
    # проходимся по общему количеству дней
    for i in range(count_days+1):
        # формируем отдельную дату для загрузки
        cur_date = str(start_date + relativedelta(days=i))
        date_filter = [(cur_date, cur_date)]
         # Посчитаем задания в цикле
        tasks = []
        
        print()
        print(f'{"="*10}Загружаем {cur_date}. Отправляем задания на расчет')
        print()
        
        print(f'{"-"*20} Забираем статистику по отчету Buying')
        print()
        # Забираем статистику по отчету Buying
        df_buying_final = get_nat_tv_buying_report(ad_filter=special_ad_filter, date_filter=date_filter)
        df_buying_final['prj_name'] = 'Total_buying_aud'
        # df_buying_final['media_type'] = 'TV'
        # df_buying_final['media_key_id'] = df_buying_final['media_type'] + '_' + df_buying_final['adId']
        # Добавляем тип медиа и Дисконты к расходым по годам
        df_buying_final = append_custom_columns(df_buying_final, report='buying')
        # нормализуем отчет по Баинговой аудитории
        df_buying_final = normalize_columns_types(df_buying_final, config_tv_index.nat_tv_buying_int_lst, config_tv_index.nat_tv_buying_float_lst)
        
        # загружаем данные в БД по отчету Buying
        print('Записываем данные в таблицу фактов Buying')
        print()
        # записываем в БД отчет по Баинговой аудитории
        downloadTableToDB(config.db_name, config_tv_index.nat_tv_buying, df_buying_final)
        # удаляем этот датаФрейм
        del df_buying_final
        
        print()
        print(sep_str)
        
        print()
        print(f'{"-"*20} Забираем статистику по отчету Simple')
        print()
        # Забираем статистику по отчету Simple
        df_simple_final = get_nat_tv_simple_report(ad_filter=special_ad_filter, date_filter=date_filter)
        # создаем отдельный датаФрейм с ИД объявлений и флагом чистки
        # df_cleaning_custom_id = custom_cleaning_dict[['ad_id', 'cleaning_flag']]
        # # забираем таблицу Медиа дисконтов
        # media_discounts = get_media_discounts(['TV'])
        # добавляем флаг чистки в датаФрейм
        df_simple_final = append_custom_columns(df_simple_final, report='simple', nat_tv_ad_dict=nat_tv_ad_dict_df)
        # Нормализуем все поля в датаФрейме
        df_simple_final = normalize_columns_types(df_simple_final, config_tv_index.nat_tv_simple_int_lst, config_tv_index.nat_tv_simple_float_lst)
        
        
        # создаем таблицу с уникальными объявлениями и их характеристиками
        simple_ad_dict_df = df_simple_final[nat_tv_ad_dict_cols_list].drop_duplicates('media_key_id')
       
        # забираем из БД из справочника объявлений уникальные ИД
        query = f"select distinct adId  from {config_tv_index.nat_tv_ad_dict}"
        nat_tv_ad_id_dict = get_mssql_table(config.db_name, query=query)
        # создаем список Уникальных ИД Объявлений, которые уже есть в справочнике в БД
        nat_tv_ad_id_lst = list(nat_tv_ad_id_dict['adId'])
        # оставляем только те объявления, которых нет в справочнике
        simple_ad_dict_df = simple_ad_dict_df.query('adId not in @nat_tv_ad_id_lst')
        
        # на всякий случай обрезаем описание объявления до 500 символов, чтобы не было переполнения строки
        simple_ad_dict_df['adNotes'] = simple_ad_dict_df['adNotes'].str.slice(0, 500)
        # нормализуем типы данных
        simple_ad_dict_df = normalize_columns_types(simple_ad_dict_df, config_tv_index.nat_tv_ad_dict_int_lst)
        
        print()
        print(sep_str)
        print('Записываем данные в справочник объявлений TV_Index')
        print()
        
        downloadTableToDB(config.db_name, config_tv_index.nat_tv_ad_dict, simple_ad_dict_df)
        
        print(sep_str)
        print()

        # забираем список полей, которые созданы в БД в таблице фактов Simple
        nat_tv_simple_cols_lst = [col[:col.find(' ')] for col in config_tv_index.nat_tv_simple_vars_list]
        # оставляем только нужные поля в датаФрейме для загрузки в БД
        df_simple_final = df_simple_final[nat_tv_simple_cols_lst]
        
        # загружаем данные в БД по отчету Simple
        print('Записываем данные в таблицу фактов Simple')
        print()
        downloadTableToDB(config.db_name, config_tv_index.nat_tv_simple, df_simple_final)
        
        print(sep_str)
        print()
        
    finish_time = datetime.now()
    print(sep_str)
    print(f'Время окончания работы скрипта {finish_time}')
    print(f'Период загрузки {start_date} - {end_date}. Общее время работы скрипта: {finish_time - start_time}')
    print(sep_str)
    
    
    # return df_simple_final


# In[ ]:


# УКАЗЫВАЕМ на листе config
# targets - список аудиторий, по которым нужно собрать датаФрейм
# ad_filter - условия фильтрации объявлений 


# weekday_filter - Задаем дни недели / Задаем тип дня - daytype_filter / Задаем временной интервал - time_filter
# Задаем ЦА - basedemo_filter / 
# Доп фильтр ЦА, нужен только в случае расчета отношения между ЦА, например, при расчете Affinity Index - targetdemo_filter
# Задаем место просмотра - location_filter / Задаем каналы - company_filter
# Указываем фильтр программ: продолжительность от 5 минут (300 секунд) - program_filter / Фильтр блоков - 
def get_nat_tv_simple_report(ad_filter=None, weekday_filter=None, date_filter=None,time_filter=None, basedemo_filter=None, targetdemo_filter=None, daytype_filter=None,
                             location_filter=None, company_filter=None, program_filter=None, break_filter=None, sortings=None):
    # Cоздаем объекты для работы с TVI API
    mnet = mscore.MediascopeApiNetwork()
    mtask = cwt.MediaVortexTask()
    cats = cwc.MediaVortexCats()
    tasks = []
    # список аудиторий, по которым собираем статистику
    targets = config_tv_index.nat_tv_targets
    # список срезов, по которым будет разбивка отчета
    slices = config_tv_index.nat_tv_slices
    # список метрик для отчета Simple
    statistics = config_tv_index.nat_tv_simple_statistics
    # Здесь указаны логические условия ad_filter 
    # Они применяются для получения статистики в отчетах Simple и Buying# условия фильтрации для запроса 
    if not ad_filter:
        ad_filter = config_tv_index.nat_tv_ad_filter
    # Опции для расчета - вся рф и тд.
    options = config_tv_index.nat_tv_options

    if targets:
        # Для каждой ЦА формируем задание и отправляем на расчет
        for target, syntax in targets.items():
            # Подставляем значения словаря в параметры
            project_name = f"{target}" 
            basedemo_filter = f"{syntax}"
    
            # Формируем задание для API TV Index в формате JSON
            task_json = mtask.build_simple_task(date_filter=date_filter, weekday_filter=weekday_filter, 
                                                daytype_filter=daytype_filter, company_filter=company_filter, 
                                                location_filter=location_filter, basedemo_filter=basedemo_filter, 
                                                targetdemo_filter=targetdemo_filter,program_filter=program_filter, 
                                                break_filter=break_filter, ad_filter=ad_filter, 
                                                slices=slices, statistics=statistics, sortings=sortings, options=options)
            
            # Для каждого этапа цикла формируем словарь с параметрами и отправленным заданием на расчет
            tsk = {}
            tsk['project_name'] = project_name    
            tsk['task'] = mtask.send_simple_task(task_json)
            tasks.append(tsk)
            time.sleep(2)
            # print('.', end = '')
            
        # print(f"\nid: {[i['task']['taskId'] for i in tasks]}")
        print('')
        # Ждем выполнения
        print('Ждем выполнения')
        tsks = mtask.wait_task(tasks)
        print('Расчет завершен, получаем результат')
        
        # Получаем результат
        results = []
        print('Собираем таблицу')
        
        for t in tasks:
            tsk = t['task'] 
            df_result = mtask.result2table(mtask.get_result(tsk), project_name = t['project_name'])        
            results.append(df_result)
            print('.', end = '')
    
        df = pd.concat(results)
        df = df[['prj_name']+slices+statistics]

        return df


# In[ ]:


# УКАЗЫВАЕМ на листе config
# targets - здесь Обнуляем, т.к. для нам нужен Тотал
# ad_filter - условия фильтрации объявлений 


# weekday_filter - Задаем дни недели / Задаем тип дня - daytype_filter / Задаем временной интервал - time_filter
# Задаем ЦА - basedemo_filter / 
# Доп фильтр ЦА, нужен только в случае расчета отношения между ЦА, например, при расчете Affinity Index - targetdemo_filter
# Задаем место просмотра - location_filter / Задаем каналы - company_filter
# Указываем фильтр программ: продолжительность от 5 минут (300 секунд) - program_filter / Фильтр блоков - 
def get_nat_tv_buying_report(ad_filter=None, weekday_filter=None, date_filter=None,time_filter=None, basedemo_filter=None, targetdemo_filter=None, daytype_filter=None,
                             location_filter=None, company_filter=None, program_filter=None, break_filter=None, sortings=None):
    # Cоздаем объекты для работы с TVI API
    mnet = mscore.MediascopeApiNetwork()
    mtask = cwt.MediaVortexTask()
    cats = cwc.MediaVortexCats()
    tasks = []

    # список срезов, по которым будет разбивка отчета по Баинговым аудиториям
    slices = config_tv_index.nat_tv_buying_slices
    # список метрик для отчета Buying
    statistics = config_tv_index.nat_tv_bying_statistics
    # Здесь указаны логические условия ad_filter 
    # Они применяются для получения статистики в отчетах Simple и Buying# условия фильтрации для запроса 
    if not ad_filter:
        ad_filter = config_tv_index.nat_tv_ad_filter
    # Опции для расчета - вся рф и тд.
    options = config_tv_index.nat_tv_options
   
    # Формируем задание для API TV Index в формате JSON
    task_json = mtask.build_simple_task(date_filter=date_filter, weekday_filter=weekday_filter, 
                                        daytype_filter=daytype_filter, company_filter=company_filter, 
                                        location_filter=location_filter, basedemo_filter=basedemo_filter, 
                                        targetdemo_filter=targetdemo_filter,program_filter=program_filter, 
                                        break_filter=break_filter, ad_filter=ad_filter, 
                                        slices=slices, statistics=statistics, sortings=sortings, options=options)


    # Отправляем задание на расчет и ждем выполнения
    task_timeband = mtask.wait_task(mtask.send_simple_task(task_json))
    # Получаем результат
    df = mtask.result2table(mtask.get_result(task_timeband))
    
    return df


# In[ ]:





# In[ ]:


# get_nat_tv_reports(flag='first')


# In[ ]:


# start_date = '2023-01-01' #'2023-01-01' '2025-03-25'
# start_date = datetime.strptime(start_date, '%Y-%m-%d').date()

# end_date = '2023-01-02' #'2023-01-02' '2025-03-27'
# end_date = datetime.strptime(end_date, '%Y-%m-%d').date()

# print(f'start_date: {start_date} / end_date: {end_date}')


# In[ ]:


# get_nat_tv_reports(start_date=start_date, end_date=end_date, flag='regular')


# In[ ]:


# get_nat_tv_reports(start_date=start_date, end_date=end_date, flag='first')


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:




