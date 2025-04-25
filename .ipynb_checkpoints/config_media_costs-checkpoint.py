#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import numpy as np
from datetime import datetime

import config

start_of_the_time = datetime.strptime('1990-01-01', '%Y-%m-%d') # указываем дату начала сбора данных, для преобразования номера месяца


# In[2]:


# segments_main_link = 'https://docs.google.com/spreadsheets/d/e/2PACX-1vR2WJjY1kEsKluiXddRiFrqKFOVGO06eJY4CVt33OhJ7KdALHHr3sYsqXyeoVxmNI08VNUP6mAlnQKt/pub?gid=0&single=true&output=csv'
# media_type_detail_link = 'https://docs.google.com/spreadsheets/d/e/2PACX-1vTmkq1SXPqmHqCx9MfiMlbf3nV3PRKctT0r1RItnLxnKt7MYzBg56V99obmqbjWek3ux8gdjhvLQR8E/pub?gid=0&single=true&output=csv'
# tv_net_group_link = 'https://docs.google.com/spreadsheets/d/e/2PACX-1vTvelFTRLTMGnlLWLmXvLXRLVdBe7magXnTfncl72nXYN0l6s2NfPLLe-M2ipdpV94JesJ-ZuA6lejj/pub?gid=0&single=true&output=csv'


# In[3]:


"""
создаем фильтр, который будем применять во воложенном запросе для фильтрации ВСЕХ ТАБЛИЦ по ВСЕМ ИСТОЧНИКАМ
этот фильтр используем в ТВ, Радио, ООН, Пресса
Префикс для фильтруемой таблицы задан с запасом = t10
"""
main_filter_str = config.main_filter_str


# In[4]:


"""
этот справочник нужен для формирования запроса таблицы Фактов, который мы отправляем в БД Медиаскоп
для каждого типа медиа будет свой набор полей, который необходиом получить
"""
# first_row_query_dict = {
#     'tv': 't1.vid, t1.cid, t1.distr, t1.mon, t1.from_mon, t1.from_cid, t1.estat, t1.cnd_cost_rub,  t1.vol, t1.cnt'
# }


# In[5]:


"""
создаем словарь, с помощью которого будем переименовывать поля в таблицах Фактов и Справочниках
Названия приводим к стандарту из ТВ индекс
"""
rename_cols_dict = {
    'vid':          'adId',
    'distr':        'breaksDistributionType',
    'mon':          'mon_num',
    'cnd_cost_rub': 'ConsolidatedCostRUB',
    'cnt':          'Quantity',
    'notes':        'adNotes',
    'atid':         'adTypeId',
    'stdur':        'adStandardDuration',
    'alid':         'advertiserListId',
    'blid':         'brandListId',
    'sblid':        'subbrandListId',
    'mlid':         'modelListId',
    'slid2':        'articleList2Id',
    'slid3':        'articleList3Id',
    'slid4':        'articleList4Id', 
    'slaid':        'adSloganAudioId',
    'slvid':        'adSloganVideoId',
    'aid':          'advertiserId',
    'bid':          'brandId',
    'sbid':         'subbrandId',
    'mid':          'modelId',
    'sid2':         'article2Id',
    'sid3':         'article3Id',
    'sid4':         'article4Id', 
    'rid':          'regionId',
    'rgn_name':     'regionName',
}


# In[6]:


# этот список используем, чтобы добавить единые кастомные ключи, котороые можно будет использовать для всех типов медиа
# Наример tv_123

custom_cols_dict = {'cid': 'cid_custom', 'netId': 'nid_custom', 'atid': 'ad_type_custom'}            


# In[7]:


"""
таблица фактов по отчету TV расходы
"""

media_tv_costs = 'media_tv_costs'

media_tv_costs_vars_list = [
            'media_key_id nvarchar(40)',
            'media_type nvarchar(20)',
            'media_type_long nvarchar(40)',
            'media_type_detail nvarchar(20)',
            'adId int',
            'researchDate nvarchar(10)',
            'breaksDistributionType nvarchar(1)',
            'cid smallint',
            'from_cid smallint',
            'mon_num smallint',
            'regionId smallint',
            'netId smallint',
            'adTypeId smallint',
            'ad_type_custom nvarchar(15)',
            'cid_custom nvarchar(15)',
            'nid_custom nvarchar(15)',
            'from_mon smallint',
            'estat nvarchar(1)',
            'ConsolidatedCostRUB bigint',
            'year smallint',
            'vol int',
            'Quantity smallint',
             'disc float', 
            'ConsolidatedCostRUB_disc float'
]

media_tv_costs_int_lst = ['adId', 'netId', 'regionId', 'adTypeId', 'cid', 'from_cid', 'mon_num', 'from_mon',  
                          'ConsolidatedCostRUB', 'year', 'vol', 'Quantity']
media_tv_costs_float_lst = ['disc', 'ConsolidatedCostRUB_disc']


# In[8]:


"""
таблица фактов по отчету Radio расходы
"""

media_radio_costs = 'media_radio_costs'

media_radio_costs_vars_list = [
            'media_key_id nvarchar(40)',
            'media_type nvarchar(20)',
            'media_type_long nvarchar(40)',
            'media_type_detail nvarchar(20)',
            'adId int',
            'researchDate nvarchar(10)',
            'breaksDistributionType nvarchar(1)',
            'cid smallint',
            'from_cid smallint',
            'mon_num smallint',
            'regionId smallint',
            'netId smallint',
            'adTypeId smallint',
            'ad_type_custom nvarchar(15)',
            'cid_custom nvarchar(15)',
            'nid_custom nvarchar(15)',
            'from_mon smallint',
            'estat nvarchar(1)',
            'ConsolidatedCostRUB bigint',
            'year smallint',
            'vol int',
            'Quantity smallint',
             'disc float', 
            'ConsolidatedCostRUB_disc float'
]

media_radio_costs_int_lst = ['adId', 'netId', 'regionId', 'adTypeId', 'cid', 'from_cid', 'mon_num', 'from_mon',  
                          'ConsolidatedCostRUB', 'year', 'vol', 'Quantity']
media_radio_costs_float_lst = ['disc', 'ConsolidatedCostRUB_disc']


# In[9]:


"""
таблица фактов по отчету Outdoor расходы
"""

media_outdoor_costs = 'media_outdoor_costs'

media_outdoor_costs_vars_list = [
            'media_key_id nvarchar(40)',
            'media_type nvarchar(20)',
            'media_type_long nvarchar(40)',
            'media_type_detail nvarchar(20)',
            'adId int',
            'researchDate nvarchar(10)',
            'breaksDistributionType nvarchar(1)',
            'cid smallint',
            'from_cid smallint',
            'mon_num smallint',
            'regionId smallint',
            'netId smallint',
            'adTypeId smallint',
            'ad_type_custom nvarchar(15)',
            'cid_custom nvarchar(15)',
            'nid_custom nvarchar(15)',
            'from_mon smallint',
            'estat nvarchar(1)',
            'ConsolidatedCostRUB bigint',
            'year smallint',
            'vol int',
            'Quantity smallint',
             'disc float', 
            'ConsolidatedCostRUB_disc float'
]

media_outdoor_costs_int_lst = ['adId', 'netId', 'regionId', 'adTypeId', 'cid', 'from_cid', 'mon_num', 'from_mon',  
                          'ConsolidatedCostRUB', 'year', 'vol', 'Quantity']
media_outdoor_costs_float_lst = ['disc', 'ConsolidatedCostRUB_disc']


# In[10]:


"""
таблица фактов по отчету Press расходы
"""

media_press_costs = 'media_press_costs'

media_press_costs_vars_list = [
            'media_key_id nvarchar(40)',
            'media_type nvarchar(20)',
            'media_type_long nvarchar(40)',
            'media_type_detail nvarchar(20)',
            'adId int',
            'researchDate nvarchar(10)',
            'breaksDistributionType nvarchar(1)',
            'cid smallint',
            'from_cid smallint',
            'mon_num smallint',
            'regionId smallint',
            'netId smallint',
            'adTypeId smallint',
            'ad_type_custom nvarchar(15)',
            'cid_custom nvarchar(15)',
            'nid_custom nvarchar(15)',
            'from_mon smallint',
            'estat nvarchar(1)',
            'ConsolidatedCostRUB bigint',
            'year smallint',
            'vol int',
            'Quantity smallint',
             'disc float', 
            'ConsolidatedCostRUB_disc float'
]

media_press_costs_int_lst = ['adId', 'netId', 'regionId', 'adTypeId', 'cid', 'from_cid', 'mon_num', 'from_mon',  
                          'ConsolidatedCostRUB', 'year', 'vol', 'Quantity']
media_press_costs_float_lst = ['disc', 'ConsolidatedCostRUB_disc']


# In[ ]:





# In[11]:


"""
Словарь с характеристиками TV Объявлений - тип Лист (brandListId, articleList2Id и тд.)
"""
adex_ad_dict_list_tv = 'adex_ad_dict_list_tv'

adex_ad_dict_list_tv_vars_list = [
            'media_key_id nvarchar(40)',
            'media_type nvarchar(10)',
            'adId int',
            'adName nvarchar(100)',
            'adNotes nvarchar(250)',
            'adTypeId smallint',
            'adStandardDuration smallint',
            'advertiserListId int',
            'brandListId int',
            'subbrandListId int',
            'modelListId int', 
            'articleList2Id int',
            'articleList3Id int',
            'articleList4Id int',           
            'adSloganAudioId int',
            'adSloganVideoId int',         
            'adFirstIssueDate nvarchar(10)',
            'ad_type_custom nvarchar(15)',
            'cleaning_flag tinyint',
            'clip_type_region nvarchar(100)',
            'advertiser_type nvarchar(20)',
            'advertiser_main nvarchar(100)',
            'brand_main nvarchar(150)',
            'competitor nvarchar(20)',
            'category_general nvarchar(50)',
            'delivery nvarchar(30)',
            'product_category nvarchar(50)',
            ]

adex_ad_dict_list_tv_int_lst = ['adId', 'adTypeId', 'adStandardDuration', 'advertiserListId', 'brandListId', 'subbrandListId', 
    'modelListId', 'articleList2Id', 'articleList3Id', 'articleList4Id', 'adSloganAudioId', 'adSloganVideoId', 'cleaning_flag']


# In[12]:


"""
Словарь с характеристиками Radio Объявлений - тип Лист (brandListId, articleList2Id и тд.)
"""
adex_ad_dict_list_radio = 'adex_ad_dict_list_radio'

adex_ad_dict_list_radio_vars_list = [
            'media_key_id nvarchar(40)',
            'media_type nvarchar(10)',
            'adId int',
            'adName nvarchar(100)',
            'adNotes nvarchar(250)',
            'adTypeId smallint',
            'adStandardDuration smallint',
            'advertiserListId int',
            'brandListId int',
            'subbrandListId int',
            'modelListId int', 
            'articleList2Id int',
            'articleList3Id int',
            'articleList4Id int',   
            'adFirstIssueDate nvarchar(10)',
            'ad_type_custom nvarchar(15)',
            'cleaning_flag tinyint',
            'clip_type_region nvarchar(100)',
            'advertiser_type nvarchar(20)',
            'advertiser_main nvarchar(100)',
            'brand_main nvarchar(150)',
            'competitor nvarchar(20)',
            'category_general nvarchar(50)',
            'delivery nvarchar(30)',
            'product_category nvarchar(50)',
            ]

adex_ad_dict_list_radio_int_lst = ['adId', 'adTypeId', 'adStandardDuration', 'advertiserListId', 'brandListId', 'subbrandListId', 
    'modelListId', 'articleList2Id', 'articleList3Id', 'articleList4Id', 'cleaning_flag']


# In[ ]:





# In[13]:


"""
Словарь с характеристиками Outdoor Объявлений - тип Лист (brandListId, articleList2Id и тд.)
"""
adex_ad_dict_list_outdoor = 'adex_ad_dict_list_outdoor'

adex_ad_dict_list_outdoor_vars_list = [
            'media_key_id nvarchar(40)',
            'media_type nvarchar(10)',
            'adId int',
            'adName nvarchar(100)',
            'adNotes nvarchar(250)',
            'adTypeId smallint',
            'adStandardDuration smallint',
            'advertiserListId int',
            'brandListId int',
            'subbrandListId int',
            'modelListId int', 
            'articleList2Id int',
            'articleList3Id int',
            'articleList4Id int',                
            'adFirstIssueDate nvarchar(10)',
            'ad_type_custom nvarchar(15)',
            'cleaning_flag tinyint',
            'clip_type_region nvarchar(100)',
            'advertiser_type nvarchar(20)',
            'advertiser_main nvarchar(100)',
            'brand_main nvarchar(150)',
            'competitor nvarchar(20)',
            'category_general nvarchar(50)',
            'delivery nvarchar(30)',
            'product_category nvarchar(50)',
            ]

adex_ad_dict_list_outdoor_int_lst = ['adId', 'adTypeId', 'adStandardDuration', 'advertiserListId', 'brandListId', 'subbrandListId', 
    'modelListId', 'articleList2Id', 'articleList3Id', 'articleList4Id', 'cleaning_flag']


# In[14]:


"""
Словарь с характеристиками Press Объявлений - тип Лист (brandListId, articleList2Id и тд.)
"""
adex_ad_dict_list_press = 'adex_ad_dict_list_press'

adex_ad_dict_list_press_vars_list = [
            'media_key_id nvarchar(40)',
            'media_type nvarchar(10)',
            'adId int',
            'adName nvarchar(100)',
            'adNotes nvarchar(250)',
            'adTypeId smallint',
            'adStandardDuration smallint',
            'advertiserListId int',
            'brandListId int',
            'subbrandListId int',
            'modelListId int', 
            'articleList2Id int',
            'articleList3Id int',
            'articleList4Id int',                
            'adFirstIssueDate nvarchar(10)',
            'ad_type_custom nvarchar(15)',
            'cleaning_flag tinyint',
            'clip_type_region nvarchar(100)',
            'advertiser_type nvarchar(20)',
            'advertiser_main nvarchar(100)',
            'brand_main nvarchar(150)',
            'competitor nvarchar(20)',
            'category_general nvarchar(50)',
            'delivery nvarchar(30)',
            'product_category nvarchar(50)',
            ]

adex_ad_dict_list_press_int_lst = ['adId', 'adTypeId', 'adStandardDuration', 'advertiserListId', 'brandListId', 'subbrandListId', 
    'modelListId', 'articleList2Id', 'articleList3Id', 'articleList4Id', 'cleaning_flag']


# In[15]:


"""
vid - ИД объявлений в данном справочнике НЕ являются уникальными
т.к. на 1 ИД может приходиться несколько BrandID, modelId и тд.
таким образом строки дублируются
словарь с оригиналами характеристик TV Объявлений - brandId, articleLevel2Id и тд.
"""
# adex_ad_appendix_dict_tv = 'adex_ad_appendix_dict_tv'

# adex_ad_appendix_dict_tv_vars_list = [
#             'media_key_id nvarchar(40)',
#             'media_type nvarchar(10)',
#             'adId int',          
#             'advertiserId int',
#             'brandId int',
#             'subbrandId int',
#             'modelId int', 
#             'article2Id int',
#             'article3Id int',
#             'article4Id int',           
#             'cleaning_flag tinyint',
#             'clip_type_region nvarchar(100)',
#             'advertiser_type nvarchar(20)',
#             'advertiser_main nvarchar(100)',
#             'brand_main nvarchar(150)',
#             'competitor nvarchar(20)',
#             'category_general nvarchar(50)',
#             'delivery nvarchar(30)',
#             'product_category nvarchar(50)',
#             ]

# adex_ad_appendix_dict_tv_int_lst = ['adId', 'advertiserId', 'brandId', 'subbrandId', 
#     'modelId', 'article2Id', 'article3Id', 'article4Id', 'cleaning_flag']


# In[16]:


"""
словарь с расшифровкой Аудио
"""

adex_audio_slogan_dict_tv = 'adex_audio_slogan_dict_tv'

adex_audio_slogan_tv_vars_list = [
            'adSloganAudioId int',
            'audioSloganName nvarchar(100)',
            ]

adex_audio_slogan_tv_int_lst = ['adSloganAudioId']


# In[17]:


"""
словарь с расшифровкой Видео
"""

adex_video_slogan_dict_tv = 'adex_video_slogan_dict_tv'

adex_video_slogan_tv_vars_list = [
            'adSloganVideoId int',
            'videoSloganName nvarchar(100)',
            ]

adex_video_slogan_tv_int_lst = ['adSloganVideoId']


# In[ ]:





# In[18]:


"""
словарь с оригиналами характеристик TV Компаний
"""

adex_company_dict_tv = 'adex_company_dict_tv'

adex_company_dict_tv_vars_list = [
            'cid smallint',
            'media_type nvarchar(10)',
            'companyName nvarchar(100)',
            'holdingId smallint',
            'netId smallint',
            'netName nvarchar(50)',
            'regionId smallint',
            'regionName nvarchar(50)',
            'cid_custom nvarchar(15)',
            'nid_custom nvarchar(15)',
            ]

adex_company_dict_tv_int_lst = ['cid', 'holdingId', 'netId',  'regionId']


# In[19]:


"""
словарь с оригиналами характеристик Радио станций
"""

adex_company_dict_radio = 'adex_company_dict_radio'

adex_company_dict_radio_vars_list = [
            'cid smallint',
            'media_type nvarchar(10)',
            'companyName nvarchar(100)',
            # 'holdingId smallint',
            'netId smallint',
            'netName nvarchar(50)',
            'regionId smallint',
            'regionName nvarchar(50)',
            'cid_custom nvarchar(15)',
            'nid_custom nvarchar(15)',
            ]

adex_company_dict_radio_int_lst = ['cid', 'netId',  'regionId']


# In[20]:


"""
словарь с оригиналами характеристик Outdoor Agency
"""

adex_company_dict_outdoor = 'adex_company_dict_outdoor'

adex_company_dict_outdoor_vars_list = [
            'cid smallint',
            'media_type nvarchar(10)',
            'companyName nvarchar(100)',
            # 'holdingId smallint',
            'netId smallint',
            'netName nvarchar(50)',
            # 'regionId smallint',
            # 'regionName nvarchar(50)',
            'cid_custom nvarchar(15)',
            'nid_custom nvarchar(15)',
            ]

adex_company_dict_outdoor_int_lst = ['cid', 'netId']


# In[21]:


"""
словарь с оригиналами характеристик Press Edition
"""

adex_company_dict_press = 'adex_company_dict_press'

adex_company_dict_press_vars_list = [
            'cid smallint',
            'media_type nvarchar(10)',
            'companyName nvarchar(100)',
            # 'holdingId smallint',
            'netId smallint',
            'netName nvarchar(50)',
            'regionId smallint',
            'regionName nvarchar(50)',
            'cid_custom nvarchar(15)',
            'nid_custom nvarchar(15)',
            ]

adex_company_dict_press_int_lst = ['cid', 'netId', 'regionId']


# In[23]:


"""
словарь с оригиналами характеристик с типами TV Объявлений - Ролик, Спонсор, Телемагазин и тд
"""

adex_ad_type_dict_tv = 'adex_ad_type_dict_tv'

adex_ad_type_dict_tv_vars_list = [
            'adTypeId smallint',
            'media_type nvarchar(10)',
            'adTypeName nvarchar(50)',
            'ad_type_custom nvarchar(15)',
            ]

adex_ad_type_dict_tv_int_lst = ['adTypeId']


# In[24]:


"""
словарь с оригиналами характеристик с типами Radio Объявлений - Ролик, Спонсор, Самореклама и тд
"""

adex_ad_type_dict_radio = 'adex_ad_type_dict_radio'

adex_ad_type_dict_radio_vars_list = [
            'adTypeId smallint',
            'media_type nvarchar(10)',
            'adTypeName nvarchar(50)',
            'ad_type_custom nvarchar(15)',
            ]

adex_ad_type_dict_radio_int_lst = ['adTypeId']


# In[25]:


"""
словарь с оригиналами характеристик с типами Outdoor Объявлений - City Формат, Щиты, Супер щиты и тд
"""

adex_ad_type_dict_outdoor = 'adex_ad_type_dict_outdoor'

adex_ad_type_dict_outdoor_vars_list = [
            'adTypeId smallint',
            'media_type nvarchar(10)',
            'adTypeName nvarchar(50)',
            'ad_type_custom nvarchar(15)',
            ]

adex_ad_type_dict_outdoor_int_lst = ['adTypeId']


# In[26]:


"""
словарь с оригиналами характеристик с типами Press Объявлений - Коммерческая реклама, Самореклама, Свободное вложение
"""

adex_ad_type_dict_press = 'adex_ad_type_dict_press'

adex_ad_type_dict_press_vars_list = [
            'adTypeId smallint',
            'media_type nvarchar(10)',
            'adTypeName nvarchar(50)',
            'ad_type_custom nvarchar(15)',
            ]

adex_ad_type_dict_press_int_lst = ['adTypeId']


# In[27]:


"""
словарь с расшифровкой Outdoor Network
"""

adex_network_dict_outdoor = 'adex_network_dict_outdoor'

adex_network_dict_outdoor_vars_list = [
             'netId smallint',
            'networkName nvarchar(50)',
            'media_type nvarchar(10)',
            'nid_custom nvarchar(15)'
            ]

adex_network_dict_outdoor_int_lst = ['netId']


# In[28]:


"""
словарь с оригиналами характеристик Объявлений - 
R - реальный / V - виртуальный
"""

adex_estate_dict = 'adex_estate_dict'

adex_estate_dict_vars_list = [
            'estat nvarchar(1)',
            'name nvarchar(50)',
            ]

adex_estate_dict_int_lst = []


# In[29]:


"""
словарь с оригиналами характеристик Блок распространения - 
N - сетевой / O - орбитальный
L - локальный / U - N/A
"""

adex_distribution_dict = 'adex_distribution_dict'

adex_distribution_dict_vars_list = [
            'breaksDistributionType nvarchar(1)',
            'name nvarchar(50)',
            'ename nvarchar(50)',
            ]

adex_distribution_dict_int_lst = []


# In[30]:


"""
словарь Регионов (всего 71 запись)
"""

adex_regions_dict = 'adex_regions_dict'

adex_regions_dict_vars_list = [
            'regionId smallint',
            'regionName nvarchar(50)',
            ]

adex_regions_dict_int_lst = ['regionId']


# In[ ]:





# In[ ]:





# In[31]:


"""
словарь Лист AdvertiserList
"""

adex_advertiser_list_dict = 'adex_advertiser_list_dict'

adex_advertiser_list_dict_vars_list = [
            'advertiserListId int',
            'advertiserListName nvarchar(255)',
            ]

adex_advertiser_list_dict_int_lst = ['advertiserListId']


# In[32]:


"""
словарь с оригиналами характеристик Advertiser
"""

# adex_advertiser_dict = 'adex_advertiser_dict'

# adex_advertiser_dict_vars_list = [
#             'advertiserId int',
#             'advertiserName nvarchar(50)',
#             ]

# adex_advertiser_dict_int_lst = ['advertiserId']


# In[33]:


"""
словарь Лист BrandList
"""

adex_brand_list_dict = 'adex_brand_list_dict'

adex_brand_list_dict_vars_list = [
            'brandListId int',
            'brandListName nvarchar(255)',
            ]

adex_brand_list_dict_int_lst = ['brandListId']


# In[34]:


"""
словарь с оригиналами характеристик Brand
"""

# adex_brand_dict = 'adex_brand_dict'

# adex_brand_dict_vars_list = [
#             'brandId int',
#             'brandName nvarchar(50)',
#             ]

# adex_brand_dict_int_lst = ['brandId']


# In[35]:


"""
словарь Лист SubBrandList
"""

adex_subbrand_list_dict = 'adex_subbrand_list_dict'

adex_subbrand_list_dict_vars_list = [
            'subbrandListId int',
            'subbrandListName nvarchar(255)',
            ]

adex_subbrand_list_dict_int_lst = ['subbrandListId']


# In[36]:


"""
словарь с оригиналами характеристик SubBrand
"""

# adex_subbrand_dict = 'adex_subbrand_dict'

# adex_subbrand_dict_vars_list = [
#             'subbrandId int',
#             'brandId int',
#             'subbrandName nvarchar(50)',
#             ]

# adex_subbrand_dict_int_lst = ['subbrandId', 'brandId']


# In[37]:


"""
словарь Лист ModelList
"""

adex_model_list_dict = 'adex_model_list_dict'

adex_model_list_dict_vars_list = [
            'modelListId int',
            'modelListName nvarchar(255)',
            ]

adex_model_list_dict_int_lst = ['modelListId']


# In[38]:


"""
словарь с оригиналами характеристик Model
"""

# adex_model_dict = 'adex_model_dict'

# adex_model_dict_vars_list = [
#             'modelId int',
#             'subbrandId int',
#             'sid int',
#             'modelName nvarchar(110)',
#             ]

# adex_model_dict_int_lst = ['modelId', 'subbrandId', 'sid']


# In[39]:


"""
словарь Лист ArticleList2
"""
adex_article_list2_dict = 'adex_article_list2_dict'

adex_article_list2_dict_vars_list = [
            'articleList2Id int',
            'articleList2Name nvarchar(255)',
            ]

adex_article_list2_dict_int_lst = ['articleList2Id']


# In[40]:


"""
словарь Лист ArticleList3
"""

adex_article_list3_dict = 'adex_article_list3_dict'

adex_article_list3_dict_vars_list = [
            'articleList3Id int',
            'articleList3Name nvarchar(255)',
            ]

adex_article_list3_dict_int_lst = ['articleList3Id']


# In[41]:


"""
словарь Лист ArticleList4
"""

adex_article_list4_dict = 'adex_article_list4_dict'

adex_article_list4_dict_vars_list = [
            'articleList4Id int',
            'articleList4Name nvarchar(255)',
            ]

adex_article_list4_dict_int_lst = ['articleList4Id']


# In[42]:


"""
словарь ArticleLevel2
"""

# adex_article_level2_dict = 'adex_article_level2_dict'

# adex_article_level2_dict_vars_list = [
#             'articleLevel2Id int',
#             'grid int',
#             'lev tinyint',
#             'articleLevel2Name nvarchar(255)',
#             ]

# adex_article_level2_dict_int_lst = ['articleLevel2Id', 'grid', 'lev']


# In[43]:


"""
словарь ArticleLevel3
"""

# adex_article_level3_dict = 'adex_article_level3_dict'

# adex_article_level3_dict_vars_list = [
#             'articleLevel3Id int',
#             'grid int',
#             'lev tinyint',
#             'articleLevel3Name nvarchar(255)',
#             ]

# adex_article_level3_dict_int_lst = ['articleLevel3Id', 'grid', 'lev']


# In[44]:


"""
словарь ArticleLevel4
"""

# adex_article_level4_dict = 'adex_article_level4_dict'

# adex_article_level4_dict_vars_list = [
#             'articleLevel4Id int',
#             'grid int',
#             'lev tinyint',
#             'articleLevel4Name nvarchar(255)',
#             ]

# adex_article_level4_dict_int_lst = ['articleLevel4Id', 'grid', 'lev']


# In[45]:


"""
для каждого тип Медиа отдельный ключ tv / ra / od / pr - это префикс из Медиаскопа
Список параметров словарей ТВ Индекс для создания таблиц в БД и нормализации данных
Название таблицы / Список названий полей  в БД и типы данных / Список целочисденных полей
"""

media_dicts_costs = {
    'tv': [media_tv_costs, media_tv_costs_vars_list, media_tv_costs_int_lst, media_tv_costs_float_lst],
    'ra': [media_radio_costs, media_radio_costs_vars_list, media_radio_costs_int_lst, media_radio_costs_float_lst],
    'od': [media_outdoor_costs, media_outdoor_costs_vars_list, media_outdoor_costs_int_lst, media_outdoor_costs_float_lst],
    'pr': [media_press_costs, media_press_costs_vars_list, media_press_costs_int_lst, media_press_costs_float_lst],
}


# In[46]:


"""
Общие справочники, которые подходят для всех типов Медиа
Загружаем только 1 раз при первой загрузке 
НЕОБНОВЛЯЕМ!
Ключ - это название таблицы, которую нужно забрать из Медиаскопа при Первой загрузке
значение - это список, который содержит:
[0] - название таблицы в нашей БД
[1] - список полей с типами данных для нашей БД mssql
[2] - список полей с целочисленными значениями для нормализации
[3] - поле, по которому выполняется фильтрация в исходной БД Медиаскоп при ОБНОВЛЕНИИ справочника
[4] - поле в нашей БД, по которому проверяем наличие новых ИД в таблицах Фактов
[5] - поля, которые запрашиваем в Медиаскопе
"""

adex_defult_dicts = {
'BreakDistr': [adex_distribution_dict, adex_distribution_dict_vars_list, adex_distribution_dict_int_lst, 'distr', 'breaksDistributionType', 
               'distr, name, ename'],
'EState': [adex_estate_dict, adex_estate_dict_vars_list, adex_estate_dict_int_lst, 'estat', 'estat', 'estat, name'],
'Region': [adex_regions_dict, adex_regions_dict_vars_list, adex_regions_dict_int_lst, 'rid', 'regionId', 'rid, name as regionName'],
}


# In[47]:


"""
Загружаем только 1 раз при первой загрузке 
НЕОБНОВЛЯЕМ!
для каждого тип Медиа отдельный ключ tv / ra / od / pr - это префикс из Медиаскопа
Ключ - это название таблицы, которую нужно забрать из Медиаскопа при Первой загрузке
значение - это список, который содержит:
[0] - название таблицы в нашей БД
[1] - список полей с типами данных для нашей БД mssql
[2] - список полей с целочисленными значениями для нормализации
[3] - поле, по которому выполняется фильтрация в исходной БД Медиаскоп при ОБНОВЛЕНИИ справочника
[4] - поле в нашей БД, по которому проверяем наличие новых ИД в таблицах Фактов
[5] - поля, которые запрашиваем в Медиаскопе
"""

adex_unique_defult_media_dicts = {
    'tv': {'tv_Company': [adex_company_dict_tv, adex_company_dict_tv_vars_list, adex_company_dict_tv_int_lst, 'cid', 'cid',
                                    'cid, name as companyName, hid as holdingId, nid as netId, net_name as netName, rid, rgn_name'],
           'tv_Ad_type': [adex_ad_type_dict_tv, adex_ad_type_dict_tv_vars_list, adex_ad_type_dict_tv_int_lst, 'atid', 'adTypeId',
                                    'atid, name as adTypeName'],
          },
    'ra': {'ra_Station': [adex_company_dict_radio, adex_company_dict_radio_vars_list, adex_company_dict_radio_int_lst, 'stid', 'cid',
                                'stid as cid, name as companyName, hlid as netId, hold_name as netName, rid, rgn_name'],
           'ra_Ad_type': [adex_ad_type_dict_radio, adex_ad_type_dict_radio_vars_list, adex_ad_type_dict_radio_int_lst, 'atid', 'adTypeId',
                                'atid, name as adTypeName'],
          },
    'od': {'od_Agency': [adex_company_dict_outdoor, adex_company_dict_outdoor_vars_list, adex_company_dict_outdoor_int_lst, 'agid', 'cid',
                            'agid as cid, name as companyName, nid as netId, net_name as netName'],
           'od_Ad_type': [adex_ad_type_dict_outdoor, adex_ad_type_dict_outdoor_vars_list, adex_ad_type_dict_outdoor_int_lst, 'atid', 'adTypeId',
                            'atid, name as adTypeName'],
           'od_Network': [adex_network_dict_outdoor, adex_network_dict_outdoor_vars_list, adex_network_dict_outdoor_int_lst, 'nid', 'netId',
                            'nid as netId, name as networkName']
          },
    'pr': {'pr_Edition': [adex_company_dict_press, adex_company_dict_press_vars_list, adex_company_dict_press_int_lst, 'eid', 'cid',
                                'eid as cid, name as companyName, syid as netId, synd_name as netName, rid, rgn_name'],
           'pr_Ad_type': [adex_ad_type_dict_press, adex_ad_type_dict_press_vars_list, adex_ad_type_dict_press_int_lst, 'atid', 'adTypeId',
                                'atid, name as adTypeName'],
          }
}


# In[48]:


"""
для каждого тип Медиа отдельный ключ tv / ra / od / pr - это префикс из Медиаскопа
для каждого тип Медиа существует 2 набора справочников с объявлениями
Первый _ad - здесь содержатся ИД листов - brandListId, advertiserListId и тд
Второй _Appendix - содержит простые ИД - brandId, advertiserId и тд.
ключ - это название таблицы в Медиаскоп (для дальнейшего удобства так сделано)
значение - это список, который содержит:
[0] - название таблицы в нашей БД
[1] - список полей с типами данных для БД
[2] - список полей с целочисленными значениями для нормализации
[3] - поле, по которому выполняется фильтрация в исходной БД Медиаскоп при ОБНОВЛЕНИИ справочника
[4] - поле в нашей БД, по которому проверяем наличие новых ИД в таблицах Фактов
[5] - поля, которые запрашиваем в Медиаскопе
"""

adex_ad_dicts = {
    'tv': {
        'tv_Ad': [adex_ad_dict_list_tv, adex_ad_dict_list_tv_vars_list, adex_ad_dict_list_tv_int_lst, 'vid', 'adId', 
                 'vid, name as adName, notes, atid, stdur, alid, blid, sblid, mlid, slid2, slid3, slid4, fiss, slaid, slvid'],
        # 'tv_Appendix': [adex_ad_appendix_dict_tv, adex_ad_appendix_dict_tv_vars_list, adex_ad_appendix_dict_tv_int_lst, 'vid', 'adId',
        #                'vid, aid, bid, sbid, mid, sid2, sid3, sid4'],
    },
       'ra': {
        'ra_Ad': [adex_ad_dict_list_radio, adex_ad_dict_list_radio_vars_list, adex_ad_dict_list_radio_int_lst, 'vid', 'adId', 
                 """vid, name as adName, notes, atid, stdur, alid, blid, sblid, mlid, slid2, slid3, slid4, fiss"""]
    },
    'od': {
        'od_Ad': [adex_ad_dict_list_outdoor, adex_ad_dict_list_outdoor_vars_list, adex_ad_dict_list_outdoor_int_lst, 'vid', 'adId', 
                 'vid, name as adName, notes, agrid as atid, 0 as stdur, alid, blid, sblid, mlid, slid2, slid3, slid4, fiss']
    },

    'pr': {
        'pr_Ad': [adex_ad_dict_list_press, adex_ad_dict_list_press_vars_list, adex_ad_dict_list_press_int_lst, 'vid', 'adId', 
                 'vid, name as adName, notes, atid, 0 as stdur, alid, blid, sblid, mlid, slid2, slid3, slid4, fiss']
    },
}


# In[49]:


"""
для каждого тип Медиа отдельный ключ tv / ra / od / pr - это префикс из Медиаскопа
Словарь справочников, которые уникальный для каждого отдельного типа Медиа
для каждого тип Медиа создаем словарь
Ключ - это название таблицы, которую нужно забрать из Медиаскопа при Первой загрузке
значение - это список, который содержит:
[0] - название таблицы в нашей БД
[1] - список полей с типами данных для нашей БД mssql
[2] - список полей с целочисленными значениями для нормализации
[3] - поле, по которому выполняется фильтрация в исходной БД Медиаскоп при ОБНОВЛЕНИИ справочника
[4] - поле в нашей БД, по которому проверяем наличие новых ИД в таблицах Фактов
[5] - поля, которые запрашиваем в Медиаскопе
"""

adex_unique_media_dicts = {
    'tv': {
        'tv_Ad_slogan_audio': [adex_audio_slogan_dict_tv, adex_audio_slogan_tv_vars_list, adex_audio_slogan_tv_int_lst, 'slaid', 'adSloganAudioId',
                              'slaid, name as audioSloganName'],
        'tv_Ad_slogan_video': [adex_video_slogan_dict_tv, adex_video_slogan_tv_vars_list, adex_video_slogan_tv_int_lst, 'slvid', 'adSloganVideoId',
                              'slvid, name as videoSloganName'],
    }
}


# In[ ]:





# In[50]:


"""
Словарь общих справочников с типом List, которые подходят для любого типа Медиа
Ключ - это название таблицы, которую нужно забрать из Медиаскопа при Первой загрузке
значение - это список, который содержит:
[0] - название таблицы в нашей БД
[1] - список полей с типами данных для нашей БД mssql
[2] - список полей с целочисленными значениями для нормализации
[3] - поле, по которому выполняется фильтрация в исходной БД Медиаскоп при ОБНОВЛЕНИИ справочника
[4] - поле в нашей БД, по которому проверяем наличие новых ИД в таблицах Фактов
[5] - поля, которые запрашиваем в Медиаскопе
"""
adex_all_media_list_dicts = {
    'AdvertiserList': [adex_advertiser_list_dict, adex_advertiser_list_dict_vars_list, adex_advertiser_list_dict_int_lst, 'alid', 'advertiserListId',
                      'alid, name as advertiserListName'],
    'BrandList': [adex_brand_list_dict, adex_brand_list_dict_vars_list, adex_brand_list_dict_int_lst, 'blid', 'brandListId',
                     'blid, name as brandListName'],
    'Subbrandlist': [adex_subbrand_list_dict, adex_subbrand_list_dict_vars_list, adex_subbrand_list_dict_int_lst, 'sblid', 'subbrandListId',
                    'sblid, name as subbrandListName'],
    'ModelList': [adex_model_list_dict, adex_model_list_dict_vars_list, adex_model_list_dict_int_lst, 'mlid', 'modelListId',
                 'mlid, name as modelListName'],    
    'ArticleList2': [adex_article_list2_dict, adex_article_list2_dict_vars_list, adex_article_list2_dict_int_lst, 'slid2', 'articleList2Id',
                    'slid2, name as articleList2Name'],
    'ArticleList3': [adex_article_list3_dict, adex_article_list3_dict_vars_list, adex_article_list3_dict_int_lst, 'slid3', 'articleList3Id',
                    'slid3, name as articleList3Name'],
    'ArticleList4': [adex_article_list4_dict, adex_article_list4_dict_vars_list, adex_article_list4_dict_int_lst, 'slid4', 'articleList4Id',
                    'slid4, name as articleList4Name'],
}


# In[ ]:





# In[ ]:





# In[51]:


"""
Словарь общих справочников Оригинал - не Лист, которые подходят для любого типа Медиа
vid - ИД объявлений в данном справочнике НЕ являются уникальными
т.к. на 1 ИД может приходиться несколько BrandID, modelId и тд.
таким образом строки дублируются

Ключ - это название таблицы, которую нужно забрать из Медиаскопа при Первой загрузке
значение - это список, который содержит:
[0] - название таблицы в нашей БД
[1] - список полей с типами данных для нашей БД mssql
[2] - список полей с целочисленными значениями для нормализации
[3] - поле, по которому выполняется фильтрация в исходной БД Медиаскоп при ОБНОВЛЕНИИ справочника
[4] - поле в нашей БД, по которому проверяем наличие новых ИД в таблицах Фактов
[5] - поля, которые запрашиваем в Медиаскопе
"""
# adex_all_media_level_dicts = {
#     'Advertiser': [adex_advertiser_dict, adex_advertiser_dict_vars_list, adex_advertiser_dict_int_lst, 'aid', 'advertiserId',
#                       'aid, name as advertiserName'],
#     'Brand': [adex_brand_dict, adex_brand_dict_vars_list, adex_brand_dict_int_lst, 'bid', 'brandId',
#                         'bid, name as brandName'],
#     'Subbrand': [adex_subbrand_dict, adex_subbrand_dict_vars_list, adex_subbrand_dict_int_lst, 'sbid', 'subbrandId',
#                 'sbid, name as subbrandName, bid'],
#     'Model': [adex_model_dict, adex_model_dict_vars_list, adex_model_dict_int_lst, 'mid', 'modelId',
#              'mid, name as modelName, sbid, sid'],
#     'Article': {'2': [adex_article_level2_dict, adex_article_level2_dict_vars_list, adex_article_level2_dict_int_lst, 'sid', 'articleLevel2Id',
#                      'sid as articleLevel2Id, name as articleLevel2Name, grid, lev'],
#                 '3': [adex_article_level3_dict, adex_article_level3_dict_vars_list, adex_article_level3_dict_int_lst, 'sid', 'articleLevel3Id',
#                      'sid as articleLevel3Id, name as articleLevel3Name, grid, lev'],
#                 '4': [adex_article_level4_dict, adex_article_level4_dict_vars_list, adex_article_level4_dict_int_lst, 'sid', 'articleLevel4Id',
#                      'sid as articleLevel4Id, name as articleLevel4Name, grid, lev'],
#                }
# }


# In[52]:


"""
Словарь для создания Представлений
ключ - название предаставления (слово _view добавляется автомтически)
значение - запрос, на основании которого создается представление
"""

media_view_dicts = {

'media_ad_dict':  """
                        select media_key_id,media_type,media_type_long,adId,adName,adNotes,adTypeId,adStandardDuration,advertiserListId,
                        brandListId,subbrandListId,modelListId,articleList2Id,articleList3Id,articleList4Id,
                        adFirstIssueDate,ad_type_custom,cleaning_flag,clip_type_region,advertiser_type,
                        advertiser_main,brand_main,competitor,category_general,delivery,product_category
                        from adex_ad_dict_list_tv
                        union all 
                        select * from adex_ad_dict_list_radio
                        union all 
                        select * from adex_ad_dict_list_outdoor
                        union all 
                        select * from adex_ad_dict_list_press
                        """,

'media_ad_type_dict':    """
                        select * from adex_ad_type_dict_tv union all
                        select * from adex_ad_type_dict_radio union all
                        select * from adex_ad_type_dict_outdoor union all
                        select * from adex_ad_type_dict_press
                        """,

'media_costs_union': """select * from media_tv_costs union all
                        select * from media_radio_costs union all
                        select * from media_outdoor_costs union all
                        select * from media_press_costs
                        """
# 'media_company_dict':   """
#                         select cid, media_type, companyName, netId, netName, cid_custom, nid_custom from adex_company_dict_tv union all
#                         select cid, media_type, companyName, netId, netName, cid_custom, nid_custom from adex_company_dict_radio union all
#                         select * from adex_company_dict_outdoor union all
#                         select * from adex_company_dict_press
#                         """
}



# In[ ]:


"""
Прописываем условия для сохранения ИД и характеристик новых объявлений в Гугл докс
query - параметры запроса к нашей БД, чтобы сформировать нужную таблицу
worksheet - название листа для сохранения
"""


google_new_ads_media_invest = {

'query' : """
            select t5.media_key_id, t11.articleList2Name as article_list_2,
            t12.articleList3Name as article_list_3, t13.articleList4Name as article_list_4,
            t7.advertiserListName as advertiser_list, t8.brandListName as brand_list,
            t9.subbrandListName as subbrand_list, t10.modelListName as model_list,
            t5.adId as ad_id, t5.adName as ad_name, t5.adNotes as ad_description,
            t5.adFirstIssueDate as first_issue_date, t5.media_type, t6.media_type_long, 'new' as include_exclude
            from
            	(select * from media_ad_dict_view where cleaning_flag=2) t5
            	left join (select distinct adId, media_type_long from media_costs_union_view) t6
            	on t5.adId=t6.adId
            	left join adex_advertiser_list_dict t7 on t5.advertiserListId=t7.advertiserListId
                left join adex_brand_list_dict t8 on t5.brandListId=t8.brandListId
                left join adex_subbrand_list_dict t9 on t5.subbrandListId=t9.subbrandListId
                left join adex_model_list_dict t10 on t5.modelListId=t10.modelListId
                left join adex_article_list2_dict t11 on t5.articleList2Id=t11.articleList2Id
                left join adex_article_list3_dict t12 on t5.articleList3Id=t12.articleList3Id
                left join adex_article_list4_dict t13 on t5.articleList4Id=t13.articleList4Id
            """,

'worksheet' : config.new_ads_media_invest_sheet
}


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:




