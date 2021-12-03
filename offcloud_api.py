# -*- coding: utf-8 -*-
#########################################################
# python
from os import error
import re
import traceback
import json
import logging

# third-party
import requests
# sjva 공용
from framework import py_urllib, py_urllib2
# 패키지
from .plugin import logger, package_name
#########################################################

class Offcloud(object):

    @staticmethod
    def get_cache_list(key, magnet_list):
        try:
            url = 'https://offcloud.com/api/torrent/check?key=%s' % (key)
            params = {'hashes' : magnet_list}
            res = requests.post(url, json=params)
            result = res.json()
            return result['cachedItems']
        except Exception as e:
            logger.error('Exception:%s', e)
            logger.error(traceback.format_exc())
            return None 


    @staticmethod
    def get_remote_account(key):
        try:
            url = 'https://offcloud.com/api/remote/accounts'
            params = {'key' : key}
            postdata = py_urllib.urlencode(params).encode('utf-8')  
            request = py_urllib2.Request(url, postdata)
            response = py_urllib2.urlopen(request)
            data = json.load(response)
            if 'data' in data:
                return data['data']
            else:
                return None
        except Exception as e:
            logger.error('Exception:%s', e)
            logger.error(traceback.format_exc())
            return None

    # 피드로
    @staticmethod
    def add_remote(key, feed, remote_options_id):
        try:
            result = {}
            url = 'https://offcloud.com/api/remote'
            params = {
                'key' : key,
                'url' : feed.link,
                'remoteOptionId' : remote_options_id,
                'folderId' : feed.oc_folderid,
            }
            postdata = py_urllib.urlencode(params).encode('utf-8') 
            request = py_urllib2.Request(url, postdata)
            response = py_urllib2.urlopen(request)
            data = response.read()
            result = json.loads(data)
            logger.debug('ADD REMOTE ret: %s', result)
            if 'error' in result and result['error'].startswith("You have more than 100"):
                return 'over'
            feed.oc_requestId = result['requestId'] if 'requestId' in result else ''
            feed.oc_status = result['status'] if 'status' in result else ''
            feed.oc_createdOn =  result['createdOn'] if 'createdOn' in result else ''
            feed.oc_error = result['error'] if 'error' in result else ''
            feed.oc_json = result
        except Exception as e:
            logger.error('Exception:%s', e)
            logger.error(traceback.format_exc())
  
    
    # 걍 마그넷으로
    @staticmethod
    def add_remote_by_magnet(key, magnet, remote_options_id, folder_id):
        try:
            url = 'https://offcloud.com/api/remote'
            params = {
                'key' : key,
                'url' : magnet,
                'remoteOptionId' : remote_options_id,
                'folderId' : folder_id,
            }
            postdata = py_urllib.urlencode(params).encode('utf-8')  
            request = py_urllib2.Request(url, postdata)
            response = py_urllib2.urlopen(request)
            data = response.read()
            result = json.loads(data)
            return result
        except Exception as e:
            logger.error('Exception:%s', e)
            logger.error(traceback.format_exc())


    # 삭제 리퀘스트
    @staticmethod
    def remove(key, target, requestIds):
        try:
            # url = 'https://offcloud.com/' + target + '/remove?key=' + key # 복수의 작업 리퀘스트 한번에 모두 삭제 - 로그인필요
            # data = {'requestIds': requestIds}
            # result = requests.post(url) # 다중작업은 post에 data에 넣어서 보내야 함

            if type(requestIds) is str or type(requestIds) is int:
                url = 'https://offcloud.com/' + target + '/remove/' + requestIds + '?key=' + key            
                result = requests.get(url)
                result = result.json()
            else:
                result = []
                for item in requestIds:
                    url = 'https://offcloud.com/' + target + '/remove/' + item + '?key=' + key            
                    res = requests.get(url)
                    result.append(res.json())

            return result
        except Exception as e:
            logger.error('Exception:%s', e)
            logger.error(traceback.format_exc())


    @staticmethod
    def refresh_status(key, feed):
        try:
            url = 'https://offcloud.com/api/remote/status'
            params = {'key' : key, 'requestId' : feed.oc_requestId}
            postdata =py_urllib.urlencode(params).encode('utf-8')  
            request = py_urllib2.Request(url, postdata)
            response = py_urllib2.urlopen(request)
            data = response.read()
            data = json.loads(data)
            feed.oc_json = data
            if 'status' in data:
                feed.oc_status = data['status']['status']
                if feed.oc_status == 'error':
                    pass
                elif feed.oc_status == 'downloaded':
                    feed.oc_fileSize = int(data['status']['fileSize'])
                    feed.oc_fileName = data['status']['fileName']
            else:
                logger.debug('NO STATUS %s %s', url, params)
                feed.oc_status = 'NOSTATUS'
        except Exception as e:
            logger.error('Exception:%s', e)
            logger.error(traceback.format_exc())


    # offcloud.com 에서 작업 목록을 가져옴
    @staticmethod
    def get_history(apikey, target):
        try:
            # apikey = ModelSetting.get('apikey')
            history = requests.get("https://offcloud.com/api/" + target + "/history?apikey=" + apikey)
            history = history.json()
            requestIds = []
            dupeList = {}
            errorList = {}

            for item in history:
                requestIds.append(item.get('requestId'))

            url = "https://offcloud.com/api/" + target + "/status?apikey=" + apikey
            data = {'requestIds': requestIds}
            history_status = requests.post(url, data)
            history_status = history_status.json()
            history_status = history_status['requests']
            requestIds = {} # history_stauts에 반영된 requestId만 가져오기 위해서 초기화

            i = 0
            for status in history_status:
                for item in history:
                    if status.get('requestId') == item.get('requestId'):
                        history_status[i]['originalLink'] = item.get('originalLink')
                        history_status[i]['createdOn'] = item.get('createdOn')
                
                # 마그넷 주소를 기준으로각 작업의 json data를 하나의 배열로 모음 {마그넷: [{json1}, {json2},...]}
                if status.get('originalLink') not in dupeList:  # 비교할 마그넷 주소가 없으면 키와 값 추가
                    dupeList[status.get('originalLink')] =  [status]
                else:
                    dupeList[status.get('originalLink')].append(status) # 이미 마그넷 주소가 있다면 값만 추가

                if status.get('status') == 'error':
                    errorList[status.get('requestId')] = status
                
                i = i + 1

            removeList = {}
            for dupeItem in dupeList:
                if len(dupeList[dupeItem]) > 1: # 같은 마그넷 주소가 같고 requestId가 여러개면 - 중복작업
                    removeList_temp = {}
                    for x in dupeList[dupeItem]: # 중복작업 삭제대상 모음
                        # requestIds[x['requestId']] = x['amount']
                        removeList[x['requestId']] = x['amount']
                        removeList_temp[x['requestId']] = x['amount']
                    d = max(removeList_temp, key=removeList_temp.get)
                    del removeList[d]

                requestIds[dupeList[dupeItem][0]['requestId']] = dupeList[dupeItem][0]['amount']


            result = ({
                'history': history, 
                'history_status': history_status, 
                'requestIds': requestIds, 
                'removeList': removeList, 
                'errorList': errorList
                })
            # return history, history_status, requestIds, removeList, errorList
            return result

        except Exception as e:
            logger.error(e)
            logger.error(traceback.format_exc())
            # print('abort')
        
























    
    
    @staticmethod
    def retry(key, entity):
        try:
            url = 'https://offcloud.com/api/remote/retry/%s?key=%s' % (entity.requestId, key)
            request = py_urllib2.Request(url)
            response = py_urllib2.urlopen(request)
        except Exception as e:
            logger.debug('Exception:%s', e)
            logger.debug(traceback.format_exc())
            return None
    
    


    @staticmethod
    def cache(key, magnet_list, remoteOptionId):
        try:
            url = 'https://offcloud.com/api/torrent/check?key=%s' % (key)
            params = {'hashes' : magnet_list}
            res = requests.post(url, json=params)
            result = res.json()
            for i in result['cachedItems']:
                url = 'https://offcloud.com/api/remote'
                params = {
                    'key' : key,
                    'url' : 'magnet:?xt=urn:btih:%s' % i,
                    'remoteOptionId' : remoteOptionId,
                    'folderId' : '10b5Z-0RA08d6p2iMNQ9D__7e5gWOV6v5',
                }
                postdata = py_urllib.urlencode(params).encode('utf-8')  
                request = py_urllib2.Request(url, postdata)
                response = py_urllib2.urlopen(request)
                data = response.read()
                result = json.loads(data)
        except Exception as e:
            logger.error('Exception:%s', e)
            logger.error(traceback.format_exc())
            return None