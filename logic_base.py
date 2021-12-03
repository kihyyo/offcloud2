# -*- coding: utf-8 -*-
#########################################################
# python
import os
import traceback
import time
import threading

# third-party

# sjva 공용
from framework import db, scheduler, path_app_root
from framework.job import Job
from framework.util import Util
from framework.common.rss import RssUtil

# 패키지
from .plugin import logger, package_name
from .model import ModelSetting, ModelOffcloud2Account, ModelOffcloud2Job,  ModelOffcloud2Item, ModelOffcloud2Cache
from .offcloud_api import Offcloud

#########################################################


class LogicBase(object): 

    @staticmethod
    def hash(req):
        try:
            apikey = ModelSetting.get('apikey')
            cache_hash = req.form['hash'].strip().lower()
            if cache_hash.startswith('magnet'):
                cache_hash = cache_hash[20:60]
            cached_list = Offcloud.get_cache_list(apikey, [cache_hash])
            if len(cached_list) == 1:
                if cached_list[0] == cache_hash:
                    return "true"
            return "false"
        except Exception as e: 
            logger.error('Exception:%s', e)
            logger.error(traceback.format_exc())
            return "fail"


    @staticmethod
    def request_download(req):
        try:
            apikey = ModelSetting.get('apikey')
            h = req.form['hash'].strip().lower()
            if len(h) == 40:
                h = 'magnet:?xt=urn:btih:' + h
            username = req.form['default_username']
            entity = ModelOffcloud2Account.get(username)
            if entity is not None:
                remoteOptionId = entity.option_id
            else:
                return 'not exist username'
            folder_id = req.form['folder_id']
            ret = Offcloud.add_remote_by_magnet(apikey, h, remoteOptionId, folder_id)
            return ret
        except Exception as e:
            logger.debug('Exception:%s', e)
            logger.debug(traceback.format_exc())
            return str(e)


    @staticmethod
    def add_remote_default_setting(req):
        try:
            apikey = ModelSetting.get('apikey')
            h = req.form['hash'].strip().lower()
            if len(h) == 40:
                h = 'magnet:?xt=urn:btih:' + h
            
            username = ModelSetting.get('default_username')
            entity = ModelOffcloud2Account.get(username)
            if entity is not None:
                remoteOptionId = entity.option_id
            else:
                return 'not exist username'
            folder_id = ModelSetting.get('default_folder_id')
            ret = Offcloud.add_remote_by_magnet(apikey, h, remoteOptionId, folder_id)
            return ret
        except Exception as e:
            logger.debug('Exception:%s', e)
            logger.debug(traceback.format_exc())
            return str(e)

