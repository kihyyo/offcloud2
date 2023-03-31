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

# 패키지
from .plugin import logger, package_name
from .model import ModelSetting, ModelOffcloud2Item, ModelOffcloud2Cache
from .logic_rss import LogicRss
#########################################################


class Logic(object): 
    # 플러그인 로딩시 DB에 해당 값이 없으면 추가해 줌
    # 플러그인에서 새로 기능을 추가하여 DB컬럼이 필요한 경우 여기에 추가해서 컬럼을 생성해야 함
    db_default = {
        'db_version' : '6',
        'apikey' : '',
        'web_page_size': "30", 

        # Direct
        'default_username' : '',
        'default_folder_id' : '',
        'upload_folder_list' : '',
        
        # RSS
        'request_http_start_link' : 'False',
        'auto_start_rss' : 'False',
        'interval_rss' : '10',
        'tracer_max_day' : '3',
        'last_list_option_rss' : '',
        'remove_history_on_web' : 'False',
        'remove_cloud_history_on_web' : 'False',

        # 10월 9일 새로 추가
        'dedupe_on_remote' : 'False',
        'dedupe_on_cloud' : 'False',

        # 20230331 새로 추가
        'alt_download' : 'False',
        'alt_download_time' : '4', 
        'alt_upload_time' : '12',
        # cache
        'cache_save_type_list' : '',
        'cache_receive_info_send_telegram' : 'False'
    }

    @staticmethod
    def db_init():
        try:
            for key, value in Logic.db_default.items():
                if db.session.query(ModelSetting).filter_by(key=key).count() == 0:
                    db.session.add(ModelSetting(key, value))
            db.session.commit()
            Logic.migration()
        except Exception as e: 
            logger.error('Exception:%s', e)
            logger.error(traceback.format_exc())


    @staticmethod
    def plugin_load():
        try:
            Logic.db_init()
            if ModelSetting.get_bool('auto_start_rss'):
                Logic.scheduler_start(sub='rss')
            from .plugin import plugin_info
            Util.save_from_dict_to_json(plugin_info, os.path.join(os.path.dirname(__file__), 'info.json'))   
        except Exception as e: 
            logger.error('Exception:%s', e)
            logger.error(traceback.format_exc())


    @staticmethod
    def plugin_unload():
        pass

      
    @staticmethod
    def scheduler_start(sub=None):
        if sub == 'rss':
            job = Job(package_name, '%s_rss' % package_name, ModelSetting.get('interval_rss'), LogicRss.scheduler_function, u"Offcloud RSS 스케쥴링", False)
            scheduler.add_job_instance(job)
        elif sub == 'cache':
            job = Job(package_name, '%s_cache' % package_name, ModelSetting.get('interval_cache'), LogicCache.scheduler_function, u"Offcloud Cache 스케쥴링", False)
            scheduler.add_job_instance(job)

    @staticmethod
    def scheduler_stop(sub=None):
        try:
            tmp = '%s_%s' % (package_name, sub)
            logger.debug('%s scheduler_stop' % tmp)
            scheduler.remove_job(tmp)
        except Exception as e: 
            logger.error('Exception:%s', e)
            logger.error(traceback.format_exc())


    """
    @staticmethod
    def scheduler_function():
        pass
    """
    
    @staticmethod
    def reset_db(sub=None):
        try:
            logger.debug(sub)
            if sub == 'rss':
                db.session.query(ModelOffcloud2Item).delete()
                db.session.commit()
            elif sub == 'cache':
                db.session.query(ModelOffcloud2Cache).delete()
                db.session.commit()
            return True
        except Exception as e: 
            logger.error('Exception:%s', e)
            logger.error(traceback.format_exc())
            return False

    
    @staticmethod
    def one_execute(sub=None):
        logger.debug(sub)
        try:
            tmp = '%s_%s' % (package_name, sub)
            if scheduler.is_include(tmp):
                if scheduler.is_running(tmp):
                    ret = 'is_running'
                else:
                    scheduler.execute_job(tmp)
                    ret = 'scheduler'
            else:
                def func():
                    time.sleep(2)
                    if sub == 'rss':
                        LogicRss.scheduler_function()
                    elif sub == 'cache':
                        LogicCache.scheduler_function()
                threading.Thread(target=func, args=()).start()
                ret = 'thread'
        except Exception as e: 
            logger.error('Exception:%s', e)
            logger.error(traceback.format_exc())
            ret = 'fail'
        return ret
    
    """
    @staticmethod
    def process_telegram_data(data):
        try:
            logger.debug(data)
        except Exception as e: 
            logger.error('Exception:%s', e)
            logger.error(traceback.format_exc())
    """
    

    @staticmethod
    def migration():
        try:
            if ModelSetting.get('db_version') == '1':
                import sqlite3
                db_file = os.path.join(path_app_root, 'data', 'db', '%s.db' % package_name)
                connection = sqlite3.connect(db_file)
                cursor = connection.cursor()
                query = 'ALTER TABLE %s_rss ADD completed_time DATETIME' % (package_name)
                cursor.execute(query)
                connection.close()
                ModelSetting.set('db_version', '2')
                db.session.flush()
            if ModelSetting.get('db_version') == '2':
                import sqlite3
                db_file = os.path.join(path_app_root, 'data', 'db', '%s.db' % package_name)
                connection = sqlite3.connect(db_file)
                cursor = connection.cursor()
                query = 'ALTER TABLE %s_rss ADD link_to_notify_status VARCHAR' % (package_name)
                cursor.execute(query)
                connection.close()
                ModelSetting.set('db_version', '3')
                db.session.flush()
            if ModelSetting.get('db_version') == '3':
                import sqlite3
                db_file = os.path.join(path_app_root, 'data', 'db', '%s.db' % package_name)
                connection = sqlite3.connect(db_file)
                cursor = connection.cursor()
                try: cursor.execute('ALTER TABLE %s_job ADD use_tracer INTEGER' % (package_name))
                except: pass
                try: cursor.execute('ALTER TABLE %s_job ADD mount_path VARCHAR' % (package_name))
                except: pass
                try: cursor.execute('ALTER TABLE %s_job ADD move_path VARCHAR' % (package_name))
                except: pass
                try: cursor.execute('ALTER TABLE %s_job ADD call_job VARCHAR' % (package_name))
                except: pass
                try: cursor.execute('ALTER TABLE %s_rss ADD torrent_info JSON' % (package_name))
                except: pass
                try: cursor.execute('ALTER TABLE %s_rss ADD filename VARCHAR' % (package_name))
                except: pass
                try: cursor.execute('ALTER TABLE %s_rss ADD dirname VARCHAR' % (package_name))
                except: pass
                try: cursor.execute('ALTER TABLE %s_rss ADD filecount INTEGER' % (package_name))
                except: pass

                connection.close()
                ModelSetting.set('db_version', '4')
                db.session.flush()
            
            if ModelSetting.get('db_version') == '4':
                import sqlite3
                db_file = os.path.join(path_app_root, 'data', 'db', '%s.db' % package_name)
                connection = sqlite3.connect(db_file)
                cursor = connection.cursor()
                try: cursor.execute('ALTER TABLE %s_job ADD is_http_torrent_rss INTEGER' % (package_name))
                except: pass
                connection.close()
                ModelSetting.set('db_version', '5')
                db.session.flush()
            if ModelSetting.get('db_version') == '5':
                import sqlite3
                db_file = os.path.join(path_app_root, 'data', 'db', '%s.db' % package_name)
                connection = sqlite3.connect(db_file)
                cursor = connection.cursor()
                try: cursor.execute('ALTER TABLE %s_job ADD rss_mode INTEGER' % (package_name))
                except: pass
                try: cursor.execute('ALTER TABLE %s_job ADD rss_regex VARCHAR' % (package_name))
                except: pass
                connection.close()
                ModelSetting.set('db_version', '6')
                db.session.flush()
            
            if ModelSetting.get('db_version') == '6':
                import sqlite3
                db_file = os.path.join(path_app_root, 'data', 'db', '%s.db' % package_name)
                connection = sqlite3.connect(db_file)
                cursor = connection.cursor()
                try: cursor.execute('ALTER TABLE %s_job ADD username2 VARCHAR' % (package_name))
                except: pass
                connection.close()
                ModelSetting.set('db_version', '7')
                db.session.flush()

            if ModelSetting.get('db_version') == '7':
                import sqlite3
                db_file = os.path.join(path_app_root, 'data', 'db', '%s.db' % package_name)
                connection = sqlite3.connect(db_file)
                cursor = connection.cursor()
                try: cursor.execute('ALTER TABLE %s_job ADD add_to_cloud INTEGER' % (package_name))
                except: pass
                connection.close()
                ModelSetting.set('db_version', '7')
                db.session.flush()                
            
            
        except Exception as e:
            logger.error('Exception:%s', e)
            logger.error(traceback.format_exc())
