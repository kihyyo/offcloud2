# -*- coding: utf-8 -*-
#########################################################
# python
import os
import traceback

# third-party
import requests
from flask import Blueprint, request, Response, send_file, render_template, redirect, jsonify
from flask_login import login_required

# sjva 공용
from framework.logger import get_logger
from framework import app, db, scheduler, check_api
from framework.util import Util

# 패키지
package_name = __name__.split('.')[0]
logger = get_logger(package_name)
from .logic import Logic
from .logic_base import LogicBase
from .logic_rss import LogicRss
from .logic_cache import LogicCache
from .model import ModelSetting, ModelOffcloud2Account, ModelOffcloud2Job, ModelOffcloud2Item, ModelOffcloud2Cache
from .offcloud_api import Offcloud
#########################################################


#########################################################
# 플러그인 공용    
#########################################################
blueprint = Blueprint(package_name, package_name, url_prefix='/%s' %  package_name, template_folder=os.path.join(os.path.dirname(__file__), 'templates'))
menu = {
    'main' : [package_name, u'Offcloud2'],
    'sub' : [
        ['account', u'계정 설정'], ['direct', u'기본 다운로드'], ['rss', u'RSS 다운로드'], ['cache', u'Cache'], ['log', u'로그']
    ],
    'sub2' : {
        'direct' : [
            ['setting', u'설정'], ['request', u'다운로드 요청']
        ],
        'rss' : [
            ['setting', u'설정'], ['job', u'작업'], ['list', u'목록']
        ],
        'cache' : [
            ['setting', u'설정'], ['list', u'목록']
        ]
    },
    'category' : 'torrent'
}  

plugin_info = {
    'version' : '0.1.0.0',
    'name' : u'offcloud2',
    'category_name' : 'torrent',
    'developer' : 'soju6jan, summer',
    'description' : u'Offcloud2 이용 플러그인 forked by summer',
    'home' : 'https://github.com/ei8/offcloud2',
    'more' : '',
}

def plugin_load():
    logger.info('%s plugin load' % package_name)
    Logic.plugin_load()

def plugin_unload():
    Logic.plugin_unload()

def process_telegram_data(data):
    LogicCache.process_telegram_data(data)
        
#########################################################
# WEB Menu   
#########################################################
@blueprint.route('/')
def home():
    return redirect('/%s/rss/list' % package_name)

@blueprint.route('/<sub>')
@login_required
def first_menu(sub):
    arg = None
    if sub == 'account':
        arg = ModelSetting.to_dict()
        arg['package_name']  = package_name
        return render_template('%s_%s.html' % (package_name, sub), arg=arg)
    elif sub == 'direct':
        return redirect('/%s/%s/request' % (package_name, sub))
    elif sub == 'rss':
        return redirect('/%s/%s/list' % (package_name, sub))
    elif sub == 'cache':
        return redirect('/%s/%s/list' % (package_name, sub))
    elif sub == 'log':
        return render_template('log.html', package=package_name)
    return render_template('sample.html', title='%s - %s' % (package_name, sub))


@blueprint.route('/<sub>/<sub2>')
@login_required
def second_menu(sub, sub2):
    try:
        arg = ModelSetting.to_dict()
        arg['package_name']  = package_name
        if sub == 'direct':
            if sub2 == 'setting':
                return render_template('%s_%s_%s.html' % (package_name, sub, sub2), arg=arg)
            elif sub2 in ['request']:
                return render_template('%s_%s_%s.html' % (package_name, sub, sub2), arg=arg)
        elif sub == 'rss':
            if sub2 == 'setting':
                arg['sub']  = sub
                arg['scheduler'] = str(scheduler.is_include('%s_%s' % (package_name, sub)))
                arg['is_running'] = str(scheduler.is_running('%s_%s' % (package_name, sub)))
                from system.model import ModelSetting as SystemModelSetting
                ddns = SystemModelSetting.get('ddns')
                arg['rss_api'] = '%s/%s/api/%s' % (ddns, package_name, sub)
                if SystemModelSetting.get_bool('auth_use_apikey'):
                    arg['rss_api'] += '?apikey=%s' % SystemModelSetting.get('auth_apikey')
                return render_template('%s_%s_%s.html' % (package_name, sub, sub2), arg=arg)
            elif sub2 in ['job', 'list']:
                arg['is_available_normal_download'] = False
                try:
                    import downloader
                    arg['is_available_normal_download'] = downloader.Logic.is_available_normal_download()
                except:
                    pass
                if sub2 == 'list':
                    arg['jobs'] = ModelOffcloud2Job.get_list(by_dict=True)
                return render_template('%s_%s_%s.html' % (package_name, sub, sub2), arg=arg)
        elif sub == 'cache':
            if sub2 == 'setting':
                arg['sub'] = sub
                from system.model import ModelSetting as SystemModelSetting
                ddns = SystemModelSetting.get('ddns')
                arg['rss_api'] = '%s/%s/api/%s' % (ddns, package_name, sub)
                if SystemModelSetting.get_bool('auth_use_apikey'):
                    arg['rss_api'] += '?apikey=%s' % SystemModelSetting.get('auth_apikey')
                return render_template('%s_%s_%s.html' % (package_name, sub, sub2), arg=arg)
            elif sub2 == 'list':
                arg['type'] = ['KTV', 'KTV_ETC', 'MOVIE', 'MOVIE_ETC', 'MUSIC', 'SHOW', 'ANI', 'PROGRAM', 'JAV_CENSORED_DMM', 'JAV_CENSORED_JAVDB', 'JAV_CENSORED_ETC', 'JAV_UNCENSORED', 'AV_WEST', 'AV_EAST', 'ETC']
                return render_template('%s_%s_%s.html' % (package_name, sub, sub2), arg=arg)
        return render_template('sample.html', title='%s - %s - %s' % (package_name, sub, sub2))
    except Exception as e: 
        logger.error('Exception:%s', e)
        logger.error(traceback.format_exc())

#########################################################
# For UI
#########################################################
@blueprint.route('/ajax/<sub>', methods=['GET', 'POST'])
@login_required
def ajax(sub):
    try:
        if sub == 'apikey':
            apikey = request.form['apikey']
            ret = Offcloud.get_remote_account(apikey)
            ModelSetting.set('apikey', apikey)
            ModelOffcloud2Account.save(ret)
            ret = ModelOffcloud2Account.get_list(by_dict=True)
            return jsonify(ret)
        elif sub == 'setting_save':
            ret = ModelSetting.setting_save(request)
            return jsonify(ret)

        elif sub == 'accounts':
            ret = ModelOffcloud2Account.get_list(by_dict=True)
            return jsonify(ret)
        elif sub == 'hash':
            ret = LogicBase.hash(request)
            return jsonify(ret)
        # 직접 추가
        elif sub == 'request_download':
            ret = LogicBase.request_download(request)
            return jsonify(ret)
        elif sub == 'scheduler':
            go = request.form['scheduler']
            target = request.form['sub']
            if go == 'true':
                Logic.scheduler_start(sub=target)
            else:
                Logic.scheduler_stop(sub=target)
            return jsonify(go)
        elif sub == 'one_execute':
            target = request.form['sub']
            ret = Logic.one_execute(sub=target)
            return jsonify(ret)
        elif sub == 'reset_db':
            target = request.form['sub']
            ret = Logic.reset_db(sub=target)
            return jsonify(ret)

        # RSS - 설정
        elif sub == 'save_job':
            ret = {}
            ret['ret'] = ModelOffcloud2Job.save(request)
            ret['accounts'] = ModelOffcloud2Account.get_list(by_dict=True)
            ret['jobs'] = ModelOffcloud2Job.get_list(by_dict=True)
            return jsonify(ret)
        elif sub == 'job_list':
            ret = {}
            ret['accounts'] = ModelOffcloud2Account.get_list(by_dict=True)
            ret['jobs'] = ModelOffcloud2Job.get_list(by_dict=True)
            return jsonify(ret)
        elif sub == 'job_remove':
            ret = {}
            job_id = request.form['id']
            ModelOffcloud2Item.remove(job_id)
            ret['ret'] = ModelOffcloud2Job.remove(job_id)
            ret['accounts'] = ModelOffcloud2Account.get_list(by_dict=True)
            ret['jobs'] = ModelOffcloud2Job.get_list(by_dict=True)
            return jsonify(ret)
        
        # RSS - 목록
        elif sub == 'rss_list':
            ret = ModelOffcloud2Item.web_list(request)
            ret['jobs'] = ModelOffcloud2Job.get_list(by_dict=True)
            return jsonify(ret)
        elif sub == 'add_remote_rss':
            ret = LogicRss.add_remote(request)
            return jsonify(ret)
    

        # cache - 목록
        elif sub == 'cache_list':
            ret = ModelOffcloud2Cache.web_list(request)
            return jsonify(ret)


        # 타 플러그인에서 호출
        elif sub == 'add_remote':
            ret = LogicBase.add_remote_default_setting(request)
            return jsonify(ret)
        
    except Exception as e: 
        logger.error('Exception:%s', e)
        logger.error(traceback.format_exc())     

#########################################################
# API
#########################################################
@blueprint.route('/api/<sub>', methods=['GET', 'POST'])
@check_api
def api(sub):
    try:
        # 텔레그램 & sjva.me
        if sub == 'cache_download':
            ret = LogicCache.cache_download(request)
            return jsonify(ret)
        elif sub == 'rss':
            ret = ModelOffcloud2Item.api_list(request)
            data = []
            for t in ret:
                item = {}
                item['title'] = t.title
                item['link'] = t.link
                item['created_time'] = t.created_time
                data.append(item)
            from .rss_nyaa import RssUtil
            xml = RssUtil.make_rss(package_name, data)
            return Response(xml, mimetype='application/xml')
        elif sub == 'cache':
            ret = ModelOffcloud2Cache.api_list(request)
            data = []
            for t in ret:
                item = {}
                item['title'] = t.name
                item['link'] = t.magnet
                item['created_time'] = t.created_time
                data.append(item)
            from .rss_nyaa import RssUtil
            xml = RssUtil.make_rss(package_name, data)
            return Response(xml, mimetype='application/xml')
        elif sub == 'hash':
            ret = LogicBase.hash(request)
            return jsonify(ret)
        elif sub == 'add_remote':
            ret = LogicBase.add_remote_default_setting(request)
            logger.debug(ret)
            return jsonify(ret)
    except Exception as e: 
        logger.error('Exception:%s', e)
        logger.error(traceback.format_exc())


