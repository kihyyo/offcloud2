# -*- coding: utf-8 -*-
#########################################################
# python
import os
import traceback
import time
import threading
import json
import datetime
import re
import requests

# third-party
from sqlalchemy import desc
from sqlalchemy import or_, and_, func, not_
from sqlalchemy.sql.expression import null

# sjva 공용
from framework import db, scheduler, path_app_root, SystemModelSetting, celery, app, Util
from framework.job import Job
from framework.common.rss import RssUtil
import framework.common.celery as celery_task

# 패키지
from .plugin import logger, package_name
from .model import ModelSetting, ModelOffcloud2Account, ModelOffcloud2Job,  ModelOffcloud2Item, ModelOffcloud2Cache
from .offcloud_api import Offcloud

#########################################################


class LogicRss(object): 

    @staticmethod
    def process_insert_feed():
        try:
            job_list = ModelOffcloud2Job.get_list()
            for job in job_list:
                try:
                    logger.debug('Offcloud job:%s', job.id)
                    feed_list = RssUtil.get_rss(job.rss_url)
                    if not feed_list:
                        continue
                    flag_commit = False
                    count = 0
                    #
                    try:
                        values = [x.strip() for x in job.rss_regex.split('\n')]
                        regex_list = Util.get_list_except_empty(values)
                    except:
                        regex_list = []
                    #logger.warning(regex_list)
                    #logger.warning(job.rss_mode)
                    for feed in reversed(feed_list):
                        if db.session.query(ModelOffcloud2Item).filter_by(job_id=job.id, link=feed.link).first() is None:
                            # 2021-05-21
                            if job.rss_mode: #화이트리스트
                                flag_append = False
                                try:
                                    for regex in regex_list:
                                        if re.compile(regex).search(feed.title):
                                            flag_append = True
                                            break
                                except Exception as e:
                                    logger.error(e)
                                    logger.error(traceback.format_exc())
                                if flag_append == False:
                                    continue
                            else: #블랙리스트
                                flag_append = True
                                try:
                                    for regex in regex_list:
                                        if re.compile(regex).search(feed.title):
                                            flag_append = False
                                            break
                                except Exception as e:
                                    logger.error(e)
                                    logger.error(traceback.format_exc())
                            if flag_append == False:
                                continue

                            r = ModelOffcloud2Item()
                            r.title = u'%s' % feed.title
                            r.link = feed.link
                            #db.session.add(r)
                            job.rss_list.append(r)
                            flag_commit = True
                            count += 1
                    if flag_commit:
                        db.session.commit()
                    logger.debug('Offcloud job:%s flag_commit:%s count:%s', job.id, flag_commit, count)
                except Exception as e:
                    logger.error(e)
                    logger.error(traceback.format_exc())
        except Exception as e:
            logger.error(e)
            logger.error(traceback.format_exc())
    
    # 캐쉬 상태를 확인하는 리퀘스트
    @staticmethod
    def process_cached_list(rss_list):
        magnet_list = []
        for r in rss_list:
            if r.link.startswith('magnet'):# and r.oc_cached is not True:
                try:
                    magnet_list.append(r.link[20:60])
                except:
                    logger.error(e)
                    logger.error(traceback.format_exc())
        cached_list = Offcloud.get_cache_list(ModelSetting.get('apikey'), magnet_list)     
        return cached_list


    # Cache 확인되고 다운로드 요청한 피드 처리
    @staticmethod
    def process_cached_feed(feed):
        try:
            # 이미 봇으로 받았던거면 패스
            entity = ModelOffcloud2Cache.get_by_magnet(feed.link)
            if entity is not None:
                return
            if feed.link_to_notify_status is not None and feed.link_to_notify_status == '1':
                return
            # sjva server에게만 알리는 것으로 변경. 서버가 대신 뿌림
            telegram = {
                'title' : feed.title,
                'magnet' : feed.link,
                'who' : SystemModelSetting.get('id')
            }
            telegram_text = json.dumps(telegram, indent=2)
            try:
                import requests
                sjva_server_url = 'https://server.sjva.me/ss/api/off_cache2'
                data = {'data':telegram_text}
                res = requests.post(sjva_server_url, data=data)
                tmp = res.text
                feed.link_to_notify_status = '1'
                db.session.add(feed)
                db.session.commit()
                if res.text == 'append':
                    return True
                elif res.text == 'exist':
                    return False
            except Exception as e:
                logger.error('Exception:%s', e)
                logger.error(traceback.format_exc())    
            return True
        except Exception as e:
            logger.error(e)
            logger.error(traceback.format_exc())


    @staticmethod
    def scheduler_function():
        try:
            if app.config['config']['use_celery']:
                result = LogicRss.scheduler_function2.apply_async()
                result.get()
            else:
                result = LogicRss.scheduler_function2()
        except Exception as e: 
            logger.error('Exception:%s', e)
            logger.error(traceback.format_exc())

            
    @staticmethod
    @celery.task
    def scheduler_function2():
        # LogicRss.additional()
        if ModelSetting.get_bool('dedupe_on_remote'):
            LogicRss.scheduler_function_remove_duplicated_job('remote')
        if ModelSetting.get_bool('dedupe_on_cloud'):
            LogicRss.scheduler_function_remove_duplicated_job('cloud')
        LogicRss.scheduler_function_rss_request()
        LogicRss.scheduler_function_tracer()
        if ModelSetting.get_bool('remove_history_on_web'):
            LogicRss.scheduler_function_remove_history()
        if ModelSetting.get_bool('remove_cloud_history_on_web'):
            LogicRss.scheduler_function_remove_cloud_history('cloud')
        if ModelSetting.get_bool('alt_download'):
            LogicRss.scheduler_function_alt_download('remote')
    # status
    # 0 : 초기 값
    # 1 : 마그넷. 캐쉬. 오버
    # 2 : 마그넷. 노캐쉬. 오버
    # 3 : 일반파일. 오버
    # 4 : 수동요청. 오버
    # 6 : 마그넷. 캐쉬. 요청 성공
    # 7 : 마그넷. 노캐쉬. 요청 성공
    # 8 : 일반파일. 요청 성공 
    # 9 : 수동요청 성공
    # 11 : 완료
    # 12 : job 모드 캐시확인만. 마그넷. 캐시확인 완료.
    # 13 : error
    # 14 : NOSTATUS
    # 15 : Cloud로 추가 완료

    @staticmethod
    def scheduler_function_remove_history():
        try:
            apikey = ModelSetting.get('apikey')
            # cloud_history, history_status, requestIds, removeList = Offcloud.get_history(apikey, target)

            remote_history = requests.get("https://offcloud.com/api/remote/history?apikey=" + apikey)
            remote_history = remote_history.json()
            #logger.debug("===================== offcloud 작업 json 임포트 완료 ")

            for remote_item in remote_history:                
                remote_magnet = str(remote_item.get('originalLink'))
                query = db.session.query(ModelOffcloud2Item) \
                    .filter(ModelOffcloud2Item.link.like(remote_magnet))
                    #.filter(ModelOffcloud2Item.oc_status == 'downloaded' ) \
                items = query.all()
                
                if items:
                    for idx, feed in enumerate(items):
                        if feed.oc_status != 'downloaded':#offcloud 에서 완료된 아이템 삭제전 DB에 downloaded 상태로 업데이트
                            Offcloud.refresh_status(apikey, feed)
                            
                        if feed.oc_status == 'downloaded':
                            remote_remove_req = "https://offcloud.com/remote/remove/" + str(remote_item.get('requestId')) + "?apikey=" + apikey
                            # logger.debug("================ 마그넷 링크 매치됨, 리모트 작업 삭제 리퀘스트 전송")
                            # logger.debug('remote_remove_req : %s', remote_remove_req)
                            remote_remove_req_result = requests.get(remote_remove_req)
                            remote_remove_req_result = remote_remove_req_result.json()                            
                            if remote_remove_req_result.get('success'):
                                remote_remove_req_result = "offcloud 완료 | " + str(feed.title) + " | " + str(feed.link) + " | " + str(feed.oc_requestId) + " | " + str(remote_remove_req) + " | 결과: " + str(remote_remove_req_result)
                                logger.debug(remote_remove_req_result)

        except Exception as e:
            logger.error(e)
            logger.error(traceback.format_exc())        


    @staticmethod
    def scheduler_function_remove_cloud_history(target):
        try:
            apikey = ModelSetting.get('apikey')
            # cloud_history, history_status, requestIds, removeList, errorList = Offcloud.get_history(apikey, target)
            history_status = Offcloud.get_history(apikey, target)
            history_status = history_status['history_status']

            for cloud_item in history_status:                
                cloud_magnet = str(cloud_item.get('originalLink'))
                query = db.session.query(ModelOffcloud2Item) \
                    .filter( or_(ModelOffcloud2Item.oc_status == 'downloaded' , ModelOffcloud2Item.oc_status == 'uploading' , ModelOffcloud2Item.oc_status == 'NOSTATUS' )) \
                    .filter(ModelOffcloud2Item.oc_cached == True ) \
                    .filter(ModelOffcloud2Item.link.like(cloud_magnet))
                items = query.all()
                
                if items:
                    for idx, feed in enumerate(items):
                        cloud_remove_req_result = Offcloud.remove(apikey, target, cloud_item.get('requestId'))
                        cloud_remove_req_result = str(target) + " history 삭제 | " + str(feed.title) + " | " + str(feed.link) + " | " + str(feed.oc_requestId) + " | 결과: " + str(cloud_remove_req_result)
                        logger.debug(cloud_remove_req_result)

        except Exception as e:
            logger.error(e)
            logger.error(traceback.format_exc())        

    @staticmethod
    def scheduler_function_alt_download(target):
        try:
            from downloader.logic import Logic
            from downloader.logic_normal import LogicNormal
            LogicNormal.program_init()
            get_default_value = Logic.get_default_value()
            apikey = ModelSetting.get('apikey')
            history_status = Offcloud.get_history(apikey, target)
            history_status = history_status['history_status']
            if len(history_status) != 0:
                for remote_item in history_status:      
                    remote_magnet = str(remote_item.get('originalLink')).strip()
                    try:
                        if (remote_item.get('status') != 'uploading') and int(remote_item.get('downloadingTime')) > (ModelSetting.get_int('alt_download_time')*3600*1000) and remote_item.get('downloadingSpeed') == None :
                            Logic.add_download2(remote_magnet, get_default_value[0], get_default_value[1])
                            item = remote_item.get('requestId')
                            result = Offcloud.remove(apikey, target, item)
                            logger.debug('다운로드 중지됨 - removed : %s === %s', result, item)
                        elif (remote_item.get('status') == 'uploading') and ModelSetting.get_int('alt_upload_time') > 0 and int(remote_item.get('downloadingTime')) > (ModelSetting.get_int('alt_upload_time')*3600*1000) :
                            Logic.add_download2(remote_magnet, get_default_value[0], get_default_value[1])
                            item = remote_item.get('requestId')
                            result = Offcloud.remove(apikey, target, item)
                            logger.debug('업로드 중지됨 - removed : %s === %s', result, item)                                
                    except: 
                        continue
        except Exception as e:
            logger.error(e)
            logger.error(traceback.format_exc())   
            
    @staticmethod
    def scheduler_function_remove_duplicated_job(target):
        
        try:
            apikey = ModelSetting.get('apikey')
            removeList = Offcloud.get_history(apikey, target)
            removeList = removeList['removeList']

            if len(removeList) == 0: 
                # logger.debug('Nothing to dedupe on ' + target + ' ============================================')
                return

            result = Offcloud.remove(apikey, target, removeList)
            logger.debug(result)
            logger.debug('dedupe on ' + target + 'end ============================================')

        except Exception as e:
            logger.error(e)
            logger.error(traceback.format_exc())


    @staticmethod
    def scheduler_function_extra_request():
        # cloud에서 완료되면 캐쉬 체크 기간을 무시하고 remote에 추가
        try:
            apikey = ModelSetting.get('apikey')
            cloud_history = requests.get("https://offcloud.com/api/" + "cloud" + "/history?apikey=" + apikey)
            cloud_history = cloud_history.json()
            uniqueList = []
            dupeList = []
            dupe_remove_req = []

            logger.debug(("https://offcloud.com/api/" + target + "/history?apikey=" + apikey))
            logger.debug(cloud_history)

            for cloud_item in cloud_history:
                logger.debug(cloud_item.get('originalLink'))
                if cloud_item.get('originalLink') not in uniqueList:
                    uniqueList.append(cloud_item.get('originalLink'))
                else:
                    if cloud_item.get('fileNmae') is None or cloud_item.get('status') == 'created':
                        dupeList.append(cloud_item.get('originalLink'))
                        dupe_remove_req.append("https://offcloud.com/" + target + "/remove/" + str(cloud_item.get('requestId')) + "?apikey=" + apikey)
                        logger.debug(dupe_remove_req)

            for req in dupe_remove_req:
                requests.get(req)
                logger.debug(req)

            logger.debug('end========================================================')

        except Exception as e:
            logger.error(e)
            logger.error(traceback.format_exc())


    @staticmethod
    def scheduler_function_rss_request():
        logger.debug('1. RSS to DB')
        LogicRss.process_insert_feed()

        try:
            over_flag = False
            apikey = ModelSetting.get('apikey')
            job_list = ModelOffcloud2Job.get_list()
            # cloud_history = Offcloud.get_history(apikey, 'cloud')
            # cloud_history = cloud_history['history_status']
            # remote_history = Offcloud.get_history(apikey, 'remote')
            # remote_history = remote_history['history_status']


            for job in job_list:
                # logger.debug('============ 클라우드로 추가 기능 테스트 %s', job.add_to_cloud)
                # logger.debug('============ 클라우드로 추가 기능 테스트 %s', type(job.add_to_cloud))
                account = ModelOffcloud2Account.get(job.username)
                rss_list = ModelOffcloud2Item.get_rss_list_by_scheduler(job)
                cached_list = LogicRss.process_cached_list(rss_list)
                logger.debug('2. job name:%s count:%s, cache count:%s', job.name, len(rss_list), len(cached_list))
                
                i = 0 # 계정 분산처리를 위한 인덱스
                for feed in rss_list:
                    try:
                        i = i + 1
                        if job.username2:
                            # logger.debug('======= 계정 분산처리 =======')
                            # account2 = ModelOffcloud2Account.get(job.username2)
                            account2 = [x.strip().replace(' ', '').strip() for x in job.username2.replace('\n', '||').split('||')]
                            account2 = Util.get_list_except_empty(account2)
                            ii = i % len(account2)
                            account = ModelOffcloud2Account.get(account2[ii])                        
                        # 요청 안한 것들. 6미만은 요청안 한 것, 15는 cloud로 추가기능을 위해 새로 추가함
                        #  15는 캐쉬 생성을 위해 cloud로만 추가한 상태이므로 다운로드 요청상태가 아님
                        if feed.status < 6 or feed.status == 15:
                            feed.oc_folderid = job.folderid
                            if feed.link.startswith('magnet'):
                                if feed.link[20:60] in cached_list:
                                # 현재 아이템의 캐쉬가 있으면
                                    LogicRss.process_cached_feed(feed)
                                    feed.oc_cached = True
                                    
                                    if job.mode == '0' or job.mode == '1': 
                                        if over_flag:
                                            feed.status = 1
                                        else:
                                            feed.remote_time = datetime.datetime.now()
                                            ret = Offcloud.add_remote(apikey, feed, account.option_id)
                                            if feed.job.use_tracer:
                                                feed.make_torrent_info()
                                            #logger.debubg("요청 : %s", feed.title)
                                            if ret == 'over':
                                                over_flag = True
                                                feed.status = 1
                                            else:
                                                feed.status = 6
                                    elif job.mode == '2': #Cache 확인만
                                        feed.status = 12
                                else:
                                    # Cache 안되어 있을때
                                    if job.mode == '1': # Cache 안되어 있어도 받는 모드
                                        if over_flag:
                                            feed.status = 2
                                        else:
                                            feed.remote_time = datetime.datetime.now()
                                            ret = Offcloud.add_remote(apikey, feed, account.option_id)
                                            if feed.job.use_tracer:
                                                feed.make_torrent_info()
                                            if ret == 'over':
                                                over_flag = True
                                                feed.status = 2
                                            else:
                                                feed.status = 7

                                    # Cache가 없는 feed를 cloud로 추가
                                    try:
                                        if feed.status < 6 and feed.status > -1:
                                            if job.add_to_cloud is not None and isinstance(job.add_to_cloud, int):
                                                if datetime.datetime.now() > feed.created_time + datetime.timedelta(hours=int(job.add_to_cloud)):
                                                    # logger.debug('============ 피드 생성시간 %s', feed.created_time)
                                                    # logger.debug('============ 피드 생성시간 + 지연시간 %s', feed.created_time + datetime.timedelta(hours=job.add_to_cloud))
                                                    res_add_to_cloud = "https://offcloud.com/api/cloud?apikey=" + apikey + "&url=" + feed.link
                                                    res_add_to_cloud = requests.get(res_add_to_cloud)
                                                    # "https://offcloud.com/api/cloud?apikey=Q8wFNqlCWGjNEB2xicIb8RpiIPRONfSR&url={{url}}" 
                                                    logger.debug('Cloud로 추가: %s %s', res_add_to_cloud, feed.link)
                                                    if 'success' in res_add_to_cloud.json():
                                                        logger.debug(res_add_to_cloud.json())
                                                        logger.debug(datetime.datetime.now() + datetime.timedelta(hours=job.add_to_cloud))
                                                        feed.status = -1 # Cloud로 추가 완료
                                                    elif 'not_available' in res_add_to_cloud.json():
                                                        logger.debug('Cloud로 추가 실패')
                                                    

                                    except Exception as e:
                                        logger.error('Exception:%s', e)
                                        logger.error(traceback.format_exc())



                            elif feed.link.startswith('http'):
                                if ModelSetting.get_bool('request_http_start_link'):
                                    if not feed.link.endswith('=.torrent'):
                                        feed.remote_time = datetime.datetime.now()
                                        ret = Offcloud.add_remote(apikey, feed, account.option_id)
                                        if ret == 'over':
                                            over_flag = True
                                            feed.status = 3
                                        else:
                                            feed.status = 8
                        else:
                            if feed.oc_status == 'created' or feed.oc_status == 'uploading' or feed.oc_status == 'downloading':
                                Offcloud.refresh_status(apikey, feed)

                            if feed.oc_status == 'downloaded':
                                feed.status = 11
                                feed.completed_time = datetime.datetime.now()
                            if feed.oc_status == 'error':
                                feed.status = 13
                            if feed.oc_status == 'NOSTATUS':
                                feed.status = 14

                    except Exception as e:
                        logger.error(e)
                        logger.error(traceback.format_exc())
                    finally:
                        db.session.add(feed)
            db.session.commit()
        except Exception as e:
            logger.error(e)
            logger.error(traceback.format_exc())
        finally:
            logger.debug('==================================')


    '''
    1. 실패된 작업 삭제/개별 리스트를 만들어 관리
        중복작업 삭제
        완료나 실패된 작업 일정시간 지난뒤 강제 삭제
    2. 일정 수준 이상 다운시 클라우드에서 리모트로 미리 이동 - 클라우드 용량 full 방지
    3. cloud에서 완료됐지만 DB에 없는 작업 자동 분류 regex 이용
    4. cloud에서 완료 됐지만 캐쉬 확인기간이 지나서 remote로 추가하지 않는 경우 강제로 추가
    5. RSS수신시 일정시간 지연후 cloud로 먼저 추가 - 바로추가도 가능
        x시간 이상 지난 작업을 cloud로 추가하는 용도로도 쓰일 수 있음
    6. sjva에 의해 추가된 작업이 아니면 requestId등 정보가 DB에 없기 때문에 sjva가 관리를 하지 못하므로 DB에 업데이트 
    7. 캐쉬 수신시 캐쉬 확인 기간이 지난 건도 추가
    8. 작업추가시 이전에 추가 한적 있는지 검사 후 추가
        검사후 작업이 존재하면 중복으로 요청하지 않도로 status 업데이트
    9. 작업추가시 현재 중복 작업이 있는지 검사 후 추가
    10. 
    '''
    @staticmethod
    def additional():
        try:
            apikey = ModelSetting.get('apikey')
            target = 'cloud'
            history = Offcloud.get_history(apikey, target)
            # errorList = errorList['errorList']

            # 실패한 작업 삭제
            for item in history['errorList']:
                result = Offcloud.remove(apikey, target, item)
                logger.debug('removed - requestId: %s === %s', result, item)
                # logger.debug('remove error item on ' + target + 'end ============================================')

            # 완료된 작업 강제 삭제
            for item in history['history_status']:
                if item['status'] == 'downloaded':
                    result = Offcloud.remove(apikey, target, item)
                    logger.debug('removed - requestId: %s === %s', result, item)
                    # logger.debug('remove error item on ' + target + 'end ============================================')

        except Exception as e:
            logger.debug('Exception:%s', e)
            logger.debug(traceback.format_exc())
                
    
    @staticmethod
    def add_remote(req):
        try:
            rss_id = req.form['id']
            apikey = db.session.query(ModelSetting).filter_by(key='apikey').first().value
            feed = db.session.query(ModelOffcloud2Item).filter_by(id=rss_id).with_for_update().first()
            account = ModelOffcloud2Account.get(feed.job.username)
            
            feed.remote_time = datetime.datetime.now()
            ret = Offcloud.add_remote(apikey, feed, account.option_id)
            if feed.job.use_tracer:
                feed.make_torrent_info()
            if ret == 'over':
                feed.status = 4
            else:
                feed.status = 9
            db.session.commit()
            return True
        except Exception as e:
            logger.debug('Exception:%s', e)
            logger.debug(traceback.format_exc())
            return False


    @staticmethod
    def scheduler_function_tracer():
        try:
            job_list = ModelOffcloud2Job.get_list()
            for job in job_list:
                if not job.use_tracer:
                    continue

                # 토렌트 인포가 실패할수도 있고, 중간에 추가된 경우도 있기 때문에...
                query = db.session.query(ModelOffcloud2Item) \
                    .filter(ModelOffcloud2Item.job_id == job.id ) \
                    .filter(ModelOffcloud2Item.oc_status != '' ) \
                    .filter(ModelOffcloud2Item.created_time > datetime.datetime.now() + datetime.timedelta(days=ModelSetting.get_int('tracer_max_day')*-1)) \
                    .filter(ModelOffcloud2Item.torrent_info == None) \
                    .filter(ModelOffcloud2Item.link.like('magnet%'))
                items = query.all()
                if items:
                    for idx, feed in enumerate(items):
                        if feed.make_torrent_info():
                            db.session.add(feed)
                        
                            db.session.commit()
                        #logger.debug('%s/%s %s', idx, len(items), feed.title)
                #######################################################

                lists = os.listdir(job.mount_path)
                for target in lists:
                    try:
                        #logger.debug(target)
                        if target == 'SJVA':
                            continue
                        fullpath = os.path.join(job.mount_path, target)
 
                        # 자막파일은 바로 이동
                        if os.path.splitext(target.lower())[1] in ['.smi', '.srt', 'ass']:
                            celery_task.move_exist_remove(fullpath, job.move_path, run_in_celery=True)
                            continue
                        if os.path.splitext(target.lower())[1] == '.aria2__temp':
                            target_folder = os.path.join(job.mount_path, 'SJVA', u'기타')
                            if not os.path.exists(target_folder):
                                os.makedirs(target_folder)
                            celery_task.move_exist_remove(fullpath, target_folder, run_in_celery=True)
                            continue
                        
                        if os.path.splitext(target.lower())[1] == '.torrent':
                            target_folder = os.path.join(job.mount_path, 'SJVA', u'torrent')
                            if not os.path.exists(target_folder):
                                os.makedirs(target_folder)
                            celery_task.move_exist_remove(fullpath, target_folder, run_in_celery=True)
                            continue

                        # 해쉬 변경
                        match = re.match(r'\w{40}', target)
                        if match:
                            feeds = db.session.query(ModelOffcloud2Item).filter(ModelOffcloud2Item.link.like('%' + target)).all()
                            if len(feeds) == 1:
                                #logger.debug(feeds[0].dirname)
                                #logger.debug(len(feeds[0].dirname))
                                #logger.debug(feeds[0].filename)

                                if feeds[0].dirname != '':
                                    new_fullpath = os.path.join(job.mount_path, feeds[0].dirname)
                                else:
                                    new_fullpath = os.path.join(job.mount_path, feeds[0].filename)
                                if not os.path.exists(new_fullpath):
                                    celery_task.move(fullpath, new_fullpath, run_in_celery=True)
                                    #logger.debug('Hash %s %s', fullpath, new_fullpath)
                                    fullpath = new_fullpath
                                    target = os.path.basename(new_fullpath)
                                else:
                                    logger.debug('HASH NOT MOVED!!!!!!!')

                        if os.path.isdir(fullpath):
                            #feeds = db.session.query(ModelOffcloud2Item).filter(ModelOffcloud2Item.dirname == target).all()
                            feeds = db.session.query(ModelOffcloud2Item).filter(ModelOffcloud2Item.dirname.like(target+'%')).all()
                        else:
                            feeds = db.session.query(ModelOffcloud2Item).filter(ModelOffcloud2Item.filename == target).all()
                        logger.debug('Feeds count : %s, %s, %s', len(feeds), os.path.isdir(fullpath), target)
                        
                        # 이규연의 스포트라이트 _신천지 위장단체와 정치_.E237.200319.720p-NEXT
                        # 이규연의 스포트라이트 (신천지 위장단체와 정치).E237.200319.720p-NEXT.mp4
                        # 특수문자를 _으로 변경하는 경우 있음. 일단.. 파일만
                        if len(feeds) == 0:

                            if os.path.isdir(fullpath):
                                pass
                            else:
                                query = db.session.query(ModelOffcloud2Item)
                                for t in target.split('_'):
                                    query = query.filter(ModelOffcloud2Item.filename.like('%'+t+'%' ))
                                feeds = query.all()
                                if len(feeds) == 1:
                                    #rename
                                    new_fullpath = os.path.join(job.mount_path, feeds[0].filename)
                                    celery_task.move_exist_remove(fullpath, new_fullpath, run_in_celery=True)
                                    fullpath = new_fullpath
                                #else:
                                #    logger.debug('EEEEEEEEEEEEEEEEEEEEEEE')

                        for feed in feeds:
                            #logger.debug('제목 : %s, %s', feed.title, feed.filecount)
                            flag = True
                            for tmp in feed.torrent_info['files']:
                                #logger.debug('PATH : %s', tmp['path'])
                                #logger.debug(os.path.split(tmp['path']))
                                if os.path.split(tmp['path'])[0] != '':
                                    tmp2 = os.path.join(job.mount_path, os.path.sep.join(os.path.split(tmp['path'])))
                                else:
                                    tmp2 = os.path.join(job.mount_path, tmp['path'])
                                #logger.debug('변환 : %s', tmp2)
                                #if os.path.exists(tmp2):
                                #    logger.debug('File Exist : True')
                                #else:
                                #    logger.debug('파일 없음!!')
                                #    flag = False
                                #    break
                                if not os.path.exists(tmp2):
                                    logger.debug('NOT FIND :%s', tmp2)
                                    flag = False
                                    break

                            #2020-06-26
                            #생성이 추적기간 끝나면 이동
                            if flag == False:
                                try:
                                    ctime = int(os.path.getctime(fullpath))
                                    delta = datetime.datetime.now() - datetime.datetime.fromtimestamp(ctime)
                                    if delta.days >= ModelSetting.get_int('tracer_max_day'):
                                        flag = True
                                        logger.debug('TRACER_MAX_DAY OVER')
                                except Exception as e:
                                    logger.error('Exception:%s', e)
                                    logger.error(traceback.format_exc())

                            if flag:
                                dest_fullpath = os.path.join(job.move_path, target)
                                if os.path.exists(dest_fullpath):
                                    dup_folder = os.path.join(job.mount_path, 'SJVA', u'중복')
                                    if not os.path.exists(dup_folder):
                                        os.makedirs(dup_folder)
                                    dest_folder = dup_folder
                                else:
                                    dest_folder = job.move_path
                                #logger.debug('이동 전: %s' % fullpath)
                                celery_task.move_exist_remove(fullpath, dest_folder, run_in_celery=True)
                                logger.debug('이동 완료: %s, %s' % (fullpath, dest_folder))
                            #else:
                            #    logger.debug('대기 : %s' % fullpath)
                    except Exception as e:
                        logger.error('Exception:%s', e)
                        logger.error(traceback.format_exc())


        except Exception as e:
            logger.error('Exception:%s', e)
            logger.error(traceback.format_exc())
            return False

