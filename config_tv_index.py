#!/usr/bin/env python
# coding: utf-8

# In[3]:


import pandas as pd
import numpy as np


import config


# In[4]:


"""
Здесь прописываем логические условия ad_filter 
Они применяются для получения статистики в отчетах Simple и Buying
"""

nat_tv_ad_filter = config.nat_tv_ad_filter


# In[ ]:


# subbrand_id_str = get_subbrand_id_str(mon_num='360', media_type='tv')
# special_ad_filter = f'{config_tv_index.nat_tv_ad_filter} AND  or subbrandId IN ({subbrand_id_str})'


# In[5]:


"""
Указываем список статистик для расчета
"""

# Статистики для отчета Simple
nat_tv_simple_statistics =['Rtg000', 'RtgPer', 'StandRtgPer', 'Quantity'] 

# Статистики для отчета Buying
nat_tv_bying_statistics = ['Quantity', 'SalesRtgPer', 'StandSalesRtgPer', 'ConsolidatedCostRUB']


# In[2]:


"""
Срезы для отчета Simple и Buying по сути одинаковые
Поэтому задаем их только 1 раз здесь
ЗДЕСЬ НИЧЕГО НЕ МЕНЯЕМ 
эти срезы зашиты в таблицы БД, если их изменить
Создаем список срезов по Nat_tv
Указываем список срезов - в задаче не может быть больше 25 срезов
"""

nat_tv_slices = [
    'advertiserListId', #Список рекламодателей id
    # 'advertiserListName', 
    'brandListId', #Список брендов id
    # 'brandListName', 
    'subbrandListId', #Список суббрендов id
    # 'subbrandListName', 
    'modelListId', #Список продуктов id
    # 'modelListName',
    'articleList2Id', #Список товарных категорий 2 id
    # 'articleList2Name',
    'articleList3Id', #Список товарных категорий 3 id
    # 'articleList3Name',
    'articleList4Id', #Список товарных категорий 4 id
    # 'articleList4Name', 
    'adId',
    'adName',
    'adNotes',
    'adFirstIssueDate',
    'adSloganAudioId', #Ролик аудио слоган id
    # 'adSloganAudioName',
    'adSloganVideoId', #Ролик видео слоган id
    # 'adSloganVideoName',
    'researchDate',
    'regionId', # ИД Региона или Сетевое вещание
    # 'regionName',
    'adDistributionTypeName',
    # 'tvNetId', # id телесети
    # 'tvNetName',
    'tvCompanyId',
    # 'tvCompanyName',
    'adTypeId', #Ролик тип ID 
    # 'adTypeName',
    'adStandardDuration', 
    # 'adPositionType', #Ролик позиционирование id 
    'adPositionTypeName',
    # 'adPrimeTimeStatusId', #Прайм/ОфПрайм роликов id
    'adPrimeTimeStatusName',
    'adStartTime',
    'adSpotId'
         ]


# In[1]:


"""
Задаем опции расчета
"""

nat_tv_options = {
    "kitId": 1, # набора данных (1-Russia all, 2-Russia 100+, 3-Cities, # 4-TVI+ Russia all, 5-TVI+ Russia 100+, 6-Moscow) 
    # "totalType": "TotalChannels" #база расчета тотал статистик: Total Channels. Возможны опции: TotalTVSet, TotalChannelsThem
    "standRtg" : {
      "useRealDuration" : False,
      "standardDuration" : 20
    }
}


# In[6]:


"""
Задаем все демографические группы, чтобы каждую из них вывести в отдельном поле
Задаем необходимые группы
"""

# targets = {
# 'W 25-54'       :'sex = 2 AND age >= 25 AND age <= 54',
# 'All 18-64'     :'age >= 18 AND age <= 64',
# 'M 25-45 BC'    :'sex = 1 AND age >= 25 AND age <= 45 AND incomeGroupRussia IN (2,3)',
# 'ALL'           :'age >= 4'
# }


nat_tv_targets = {
'Total'         :'age >= 4',
'ALL_25_45'     :'age >= 25 AND age <= 45',
'ALL_20_50'     :'age >= 20 AND age <= 50',
'ALL_25_55'     :'age >= 25 AND age <= 55',
'ALL_25_60'     :'age >= 25 AND age <= 60',
'ALL_25_65'     :'age >= 25 AND age <= 65',
'ALL_45_65'     :'age >= 45 AND age <= 65',
'ALL_35_50'     :'age >= 35 AND age <= 50',
'M_35_50'       :'sex = 1 AND age >= 35 AND age <= 50',
'ALL_18_35_bc_mat_st'  :'age >= 18 AND age <= 35 AND incLevel IN (3, 4, 5)',
'ALL_25_45_bc_mat_st'  :'age >= 25 AND age <= 45 AND incLevel IN (3, 4, 5)',
'ALL_25_54_bc_mat_st'  :'age >= 25 AND age <= 54 AND incLevel IN (3, 4, 5)',
'ALL_25_55_bc_mat_st'  :'age >= 25 AND age <= 55 AND incLevel IN (3, 4, 5)',
'ALL_18_60_bc_inc_gr'  :'age >= 18 AND age <= 60 AND incomeGroupRussia IN (2, 3)',
'ALL_20_50_bc_inc_gr'  :'age >= 20 AND age <= 50 AND incomeGroupRussia IN (2, 3)',
'ALL_21_55_bc_inc_gr'  :'age >= 21 AND age <= 55 AND incomeGroupRussia IN (2, 3)',
'ALL_20_60_bc_inc_gr'  :'age >= 20 AND age <= 60 AND incomeGroupRussia IN (2, 3)',
'ALL_25_45_bc_inc_gr'  :'age >= 25 AND age <= 45 AND incomeGroupRussia IN (2, 3)',
'ALL_25_55_business'   :'age >= 25 AND age <= 55 AND property IN (1, 3)',
'ALL_18_60'     :'age >= 18 AND age <= 60',


}


# In[7]:


"""
таблица фактов по отчету Simple

При создании 2-х таблиц БД - отчет Simple / отчет Buying
Большая Часть названий полей повторяется
Чтобы сохранить последовательность, если потребуется быстро заменить какое-то поле в БД сразу в 3-х таблицах
будем использовать список названий полей из Справочника - как базовый и в отчете Simple и Buying - просто добавим отличающиеся поля
"""

nat_tv_simple = 'nat_tv_simple'

nat_tv_simple_vars_list = [
            'prj_name nvarchar(50)',
            'media_key_id nvarchar(40)',
            'media_type nvarchar(10)',
    'media_type_long nvarchar(40)',
     # 'media_type_long nvarchar(40)',
            'adId int',
            'researchDate nvarchar(10)',
            'regionId smallint',
            # 'regionName nvarchar(100)',
            'adDistributionTypeName nvarchar(30)',
            # 'tvNetId smallint',
            # 'tvNetName nvarchar(100)',
            'tvCompanyId smallint',
            # 'tvCompanyName nvarchar(100)',
            'adTypeId smallint',
            # 'adTypeName nvarchar(100)',
            'adStandardDuration int',
            'adPositionTypeName  nvarchar(50)',
            'adPrimeTimeStatusName nvarchar(50)',
            'adStartTime nvarchar(8)',
            'adSpotId bigint',
            'Rtg000 float',
            'RtgPer float',
            'StandRtgPer float',
]

nat_tv_simple_int_lst = ['adId', 'regionId', 'tvCompanyId', 'adTypeId', 'adStandardDuration', 'adSpotId']
nat_tv_simple_float_lst = ['Rtg000', 'RtgPer', 'StandRtgPer']


# In[8]:


"""
таблица фактов по отчету Buying

Создаем список названий полей и типов данных для таблицы Buying в БД MSSQL
берем все поля из таблицы Simple и убрибаем из них метрики отчета Simple
добавляем метрики отчета Buying
"""

nat_tv_buying = 'nat_tv_buying'
nat_tv_buying_vars_list = [col for col in nat_tv_simple_vars_list if col[:col.find(' ')] not in nat_tv_simple_float_lst]
nat_tv_buying_vars_list = nat_tv_buying_vars_list + ['Quantity int', 'SalesRtgPer float', 'StandSalesRtgPer float', 
                    'ConsolidatedCostRUB float', 'year smallint', 'disc float', 'ConsolidatedCostRUB_disc float']

nat_tv_buying_int_lst = ['adId', 'adStandardDuration', 'adSpotId', 'regionId', 'adTypeId', 'tvCompanyId', 'year', 'Quantity']
nat_tv_buying_float_lst = ['SalesRtgPer', 'StandSalesRtgPer', 'ConsolidatedCostRUB', 'ConsolidatedCostRUB_disc', 'disc']


# In[9]:


"""
для срезов по Баинговой аудитории нам НЕ нужно запрашивать разбивку по ArticleList2, 3, 4 / ModelList / BrandList и тд
т.к. это все является характеристиками конкретного объявления и находится в справочнике Объявлений nat_tv_ad_dict
в таблице Баинг у нас есть ИД объявления, это означает, что оно НЕ может быть разбито на какие-то более мелкие части
т.е. нет смысла тянуть доп. характеристики, т.к. они НЕ дадут большей детализации
посему мы исключаем их из запроса к ТВ Индекс
"""

nat_tv_buying_slices = [col[:col.find(' ')] for col in nat_tv_buying_vars_list]
nat_tv_buying_slices = list(set(nat_tv_buying_slices) - set(nat_tv_buying_float_lst) - set(['prj_name', 'media_key_id', 'media_type', 'media_type_long',
                                                                                            'year', 'Quantity']))


# In[ ]:


# # # Справочник Объявлений с собственными названиями
# # # сотрудники ведут гугл докс с чисткой
# # # в этом же файле добавили новые поля с собственными группировками
# # # загружаем эту кастомную таблицу в БД, чтобы потом использовать в дашборде
# custom_ad_dict = 'custom_ad_dict'

# custom_ad_dict_vars_list = [
#             'media_key_id nvarchar(40)',
#             'media_type nvarchar(20)',
#             'ad_id int',
#             # 'advertiser_list nvarchar(150)',
#             # 'brand_list nvarchar(300)',
#             # 'subbrand_list nvarchar(300)',
#             # 'model_list nvarchar(500)',
#             # 'article_list_2 nvarchar(300)',
#             # 'article_list_3 nvarchar(400)',
#             # 'article_list_4 nvarchar(500)',
#             # 'ad_name nvarchar(200)',
#             # 'ad_description nvarchar(500)',
#             # 'first_issue_date nvarchar(10)',
#             'clip_type_region nvarchar(100)',
#             'advertiser_type nvarchar(20)',
#             'advertiser_main nvarchar(100)',
#             'brand_main nvarchar(150)',
#             'competitor nvarchar(20)',
#             'category_general nvarchar(50)',
#             'include_exclude nvarchar(15)',
#             'delivery nvarchar(30)',
#             'product_category nvarchar(50)',
#             'cleaning_flag tinyint'
# ]

# custom_ad_dict_int_lst = ['ad_id', 'cleaning_flag']


# In[10]:


"""
словарь с характеристиками Объявлений
забираем через отчет Simple

пересоздаем пустую таблицу Справочников в БД
"""

nat_tv_ad_dict = 'nat_tv_ad_dict'

nat_tv_ad_dict_vars_list = [
            'media_key_id nvarchar(40)',
            'media_type nvarchar(10)',
            'media_type_long nvarchar(40)',
            'adId int',
            'adName nvarchar(100)',
            'adNotes nvarchar(250)',
            'advertiserListId int',
            'brandListId int',
            'subbrandListId int',
            'modelListId int',
            'articleList2Id int',
            'articleList3Id int',
            'articleList4Id int',
            'adFirstIssueDate nvarchar(10)',
            'adSloganAudioId int',
            'adSloganVideoId int',
            'cleaning_flag tinyint',

            'clip_type_region nvarchar(100)',
            'advertiser_type nvarchar(20)',
            'advertiser_main nvarchar(100)',
            'brand_main nvarchar(150)',
            'competitor nvarchar(20)',
            'category_general nvarchar(50)',
            'delivery nvarchar(30)',
            'product_category nvarchar(50)',
            'category nvarchar(50)',
            'segment_full nvarchar(150)',
            'segment_level1 nvarchar(50)',
            'segment_level2 nvarchar(75)',
            'segment_level3 nvarchar(75)',
            'segment_general nvarchar(90)',
            ]

nat_tv_ad_dict_int_lst = ['adId', 'advertiserListId', 'brandListId', 'subbrandListId', 'modelListId', 'articleList2Id', 'articleList3Id',
                         'articleList4Id', 'adSloganAudioId', 'adSloganVideoId', 'cleaning_flag']


# In[11]:


"""
Словарь Лист Рекламодателей TV_Index
Используем для объединения данных из отчетов Simple / Buying /  Costs
"""

tv_index_advertiser_list_dict = 'tv_index_advertiser_list_dict'

tv_index_advertiser_list_dict_vars_list = [
            'advertiserListId int',
            'advertiserListName nvarchar(200)',
            'advertiserListEName nvarchar(200)'
]

tv_index_advertiser_list_dict_int_lst = ['advertiserListId']


# In[12]:


"""
Словарь Лист Брендов TV_Index
Используем для объединения данных из отчетов Simple / Buying /  Costs
"""

tv_index_brand_list_dict = 'tv_index_brand_list_dict'

tv_index_brand_list_dict_vars_list = [
            'brandListId int',
            'brandListName nvarchar(300)',
            'brandListEName nvarchar(300)'
]

tv_index_brand_list_dict_int_lst = ['brandListId']


# In[13]:


"""
Словарь Лист СубБрендов TV_Index
Используем для объединения данных из отчетов Simple / Buying /  Costs
"""

tv_index_subbrand_list_dict = 'tv_index_subbrand_list_dict'

tv_index_subbrand_list_dict_vars_list = [
            'subbrandListId int',
            'subbrandListName nvarchar(350)',
            'subbrandListEName nvarchar(350)'
]

tv_index_subbrand_list_dict_int_lst = ['subbrandListId']


# In[14]:


"""
Словарь Лист Model TV_Index
Используем для объединения данных из отчетов Simple / Buying /  Costs
"""

tv_index_model_list_dict = 'tv_index_model_list_dict'

tv_index_model_list_dict_vars_list = [
            'modelListId int',
            'modelListName nvarchar(350)',
            'modelListEName nvarchar(350)'
]

tv_index_model_list_dict_int_lst = ['modelListId']


# In[15]:


"""
Словарь Лист Article List 2 TV_Index
Используем для объединения данных из отчетов Simple / Buying /  Costs
"""

tv_index_article_list2_dict = 'tv_index_article_list2_dict'

tv_index_article_list2_dict_vars_list = [
            'articleList2Id smallint',
            'articleList2Name nvarchar(300)',
            'articleList2EName nvarchar(300)'
]

tv_index_article_list2_dict_int_lst = ['articleList2Id']


# In[16]:


"""
Словарь Лист Article List 3 TV_Index
Используем для объединения данных из отчетов Simple / Buying /  Costs
"""

tv_index_article_list3_dict = 'tv_index_article_list3_dict'

tv_index_article_list3_dict_vars_list = [
            'articleList3Id smallint',
            'articleList3Name nvarchar(300)',
            'articleList3EName nvarchar(300)'
]

tv_index_article_list3_dict_int_lst = ['articleList3Id']


# In[17]:


"""
Словарь Лист Article List 4 TV_Index
Используем для объединения данных из отчетов Simple / Buying /  Costs
"""

tv_index_article_list4_dict = 'tv_index_article_list4_dict'

tv_index_article_list4_dict_vars_list = [
            'articleList4Id int',
            'articleList4Name nvarchar(350)',
            'articleList4EName nvarchar(350)'
]

tv_index_article_list4_dict_int_lst = ['articleList4Id']


# In[ ]:


# # Словарь Названий Объявлений TV_Index
# # Используем для объединения данных из отчетов Simple / Buying /  Costs

# tv_index_ad_name_dict = 'tv_index_ad_name_dict'

# tv_index_ad_name_dict_vars_list = [
#             'adId int',
#             'adName nvarchar(300)',
#             'adEName nvarchar(300)'
# ]

# tv_index_ad_name_dict_int_lst = ['adId']


# In[18]:


"""
Словарь Аудио Слоган TV_Index
Используем для объединения данных из отчетов Simple / Buying /  Costs
"""

tv_index_audio_slogan_dict = 'tv_index_audio_slogan_dict'

tv_index_audio_slogan_dict_vars_list = [
            'adSloganAudioId int',
            'adSloganAudioName nvarchar(200)',
            'adSloganAudioNotes nvarchar(200)'
]

tv_index_audio_slogan_dict_int_lst = ['adSloganAudioId']


# In[19]:


"""
Словарь Аудио Слоган TV_Index
Используем для объединения данных из отчетов Simple / Buying /  Costs
"""

tv_index_video_slogan_dict = 'tv_index_video_slogan_dict'

tv_index_video_slogan_dict_vars_list = [
            'adSloganVideoId int',
            'adSloganVideoName nvarchar(200)',
            'adSloganVideoNotes nvarchar(200)'
]

tv_index_video_slogan_dict_int_lst = ['adSloganVideoId']


# In[20]:


"""
Словарь Аудио Слоган TV_Index
Используем для объединения данных из отчетов Simple / Buying /  Costs
"""

tv_index_region_dict = 'tv_index_region_dict'

tv_index_region_dict_vars_list = [
            'regionId smallint',
            'regionName nvarchar(50)',
            'regionEName nvarchar(50)'
]

tv_index_region_dict_int_lst = ['regionId']


# In[21]:


"""
Словарь ТВ Сети TV_Index
Используем для объединения данных из отчетов Simple / Buying /  Costs
"""

tv_index_tv_net_dict = 'tv_index_tv_net_dict'

tv_index_tv_net_dict_vars_list = [
            'tvNetId smallint',
            'tvNetName nvarchar(100)',
            'tvNetEName nvarchar(100)'
]

tv_index_tv_net_dict_int_lst = ['tvNetId']


# In[22]:


"""
Словарь ТВ Компании TV_Index
Используем для объединения данных из отчетов Simple / Buying /  Costs
"""

tv_index_tv_company_dict = 'tv_index_tv_company_dict'

tv_index_tv_company_dict_vars_list = [
            'tvCompanyId smallint',
            'tvNetId smallint',
            'regionId tinyint',
            'tvCompanyHoldingId int',
            'tvCompanyMediaHoldingId tinyint',
            'tvThematicId tinyint',
            'tvCompanyGroupId tinyint',
            'tvCompanyCategoryId tinyint',
            'tvCompanyName nvarchar(100)',
            'tvCompanyEName nvarchar(100)',
            'tvCompanyMediaType nvarchar(5)',
            'information nvarchar(50)'
]

tv_index_tv_company_dict_int_lst = ['tvCompanyId', 'tvNetId', 'regionId', 'tvCompanyHoldingId', 'tvCompanyMediaHoldingId', 
                                    'tvThematicId', 'tvCompanyGroupId', 'tvCompanyCategoryId']


# In[23]:


"""
Словарь Типа Объявления TV_Index
Используем для объединения данных из отчетов Simple / Buying /  Costs
"""

tv_index_ad_type_dict = 'tv_index_ad_type_dict'

tv_index_ad_type_dict_vars_list = [
            'adTypeId smallint',
            'adTypeName nvarchar(60)',
            'adTypeEName nvarchar(60)',
            'notes nvarchar(110)',
            'accountingDurationType nvarchar(5)',
            'isOverride nvarchar(5)',
            'isPrice nvarchar(5)',
            'positionType nvarchar(5)'
]

tv_index_ad_type_dict_int_lst = ['adTypeId']


# In[25]:


"""
Список параметров словарей ТВ Индекс для создания таблиц в БД и нормализации данных
Название таблицы / Список названий полей  в БД и типы данных / Список целочисденных полей
"""

tv_index_dicts = {
    'advertiserListId': [tv_index_advertiser_list_dict, tv_index_advertiser_list_dict_vars_list, tv_index_advertiser_list_dict_int_lst],
    'brandListId': [tv_index_brand_list_dict, tv_index_brand_list_dict_vars_list, tv_index_brand_list_dict_int_lst],
    'subbrandListId': [tv_index_subbrand_list_dict, tv_index_subbrand_list_dict_vars_list, tv_index_subbrand_list_dict_int_lst],
    'modelListId': [tv_index_model_list_dict, tv_index_model_list_dict_vars_list, tv_index_model_list_dict_int_lst],
    'articleList2Id': [tv_index_article_list2_dict, tv_index_article_list2_dict_vars_list, tv_index_article_list2_dict_int_lst],
    'articleList3Id': [tv_index_article_list3_dict, tv_index_article_list3_dict_vars_list, tv_index_article_list3_dict_int_lst],
    'articleList4Id': [tv_index_article_list4_dict, tv_index_article_list4_dict_vars_list, tv_index_article_list4_dict_int_lst],
    'adSloganAudioId': [tv_index_audio_slogan_dict, tv_index_audio_slogan_dict_vars_list, tv_index_audio_slogan_dict_int_lst],
    'adSloganVideoId': [tv_index_video_slogan_dict, tv_index_video_slogan_dict_vars_list, tv_index_video_slogan_dict_int_lst],
    # 'regionId': [tv_index_region_dict, tv_index_region_dict_vars_list, tv_index_region_dict_int_lst],
    # 'tvNetId': [tv_index_tv_net_dict, tv_index_tv_net_dict_vars_list, tv_index_tv_net_dict_int_lst],
    # 'tvCompanyId': [tv_index_tv_company_dict, tv_index_tv_company_dict_vars_list, tv_index_tv_company_dict_int_lst],
    # 'adTypeId': [tv_index_ad_type_dict, tv_index_ad_type_dict_vars_list, tv_index_ad_type_dict_int_lst],
}


# In[26]:


"""
Список словарей, которые НЕ требуют обновления
Они загружается только 1 раз при первой загрузке Проекта
"""

tv_index_default_dicts = {
    'regionId': [tv_index_region_dict, tv_index_region_dict_vars_list, tv_index_region_dict_int_lst],
    'tvNetId': [tv_index_tv_net_dict, tv_index_tv_net_dict_vars_list, tv_index_tv_net_dict_int_lst],
    'tvCompanyId': [tv_index_tv_company_dict, tv_index_tv_company_dict_vars_list, tv_index_tv_company_dict_int_lst],
    'adTypeId': [tv_index_ad_type_dict, tv_index_ad_type_dict_vars_list, tv_index_ad_type_dict_int_lst],
}


# In[ ]:


"""
Прописываем условия для сохранения ИД и характеристик новых объявлений в Гугл докс
query - параметры запроса к нашей БД, чтобы сформировать нужную таблицу
worksheet - название листа для сохранения
"""


google_new_ads_nat_tv = {

'query' : """
            select 
                t1.media_key_id, t6.articleList2Name as article_list_2, t7.articleList3Name as article_list_3, t8.articleList4Name as article_list_4,
                 t2.advertiserListName as advertiser_list, t3.brandListName as brand_list, t4.subbrandListName as subbrand_list,
                t5.modelListName as model_list, t1.adId as ad_id, t1.adName as ad_name, t1.adNotes as ad_description,
                t1.adFirstIssueDate as first_issue_date, t1.media_type, t1.media_type_long, 'new' as include_exclude
            from 
                (select * from nat_tv_ad_dict
                where cleaning_flag=2) t1 
                    left join tv_index_advertiser_list_dict t2
                    on t1.advertiserListId=t2.advertiserListId
                    left join tv_index_brand_list_dict t3
                    on t1.brandListId=t3.brandListId
                    left join tv_index_subbrand_list_dict t4
                    on t1.subbrandListId=t4.subbrandListId
                    left join tv_index_model_list_dict t5
                    on t1.modelListId=t5.modelListId
                    left join tv_index_article_list2_dict t6
                    on t1.articleList2Id=t6.articleList2Id
                    left join tv_index_article_list3_dict t7
                    on t1.articleList3Id=t7.articleList3Id
                    left join tv_index_article_list4_dict t8
                    on t1.articleList4Id=t8.articleList4Id
                    left join tv_index_audio_slogan_dict t9
                    on t1.adSloganAudioId=t9.adSloganAudioId
                    left join tv_index_video_slogan_dict t10
                    on t1.adSloganVideoId=t10.adSloganVideoId
            """,

'worksheet' : config.new_ads_nat_tv_sheet
}

