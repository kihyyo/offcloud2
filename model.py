# -*- coding: utf-8 -*-
#########################################################
# python
import os
import traceback
import json
import datetime
# third-party
from sqlalchemy import or_, and_, func, not_, desc

# sjva 공용
from framework import db, app, path_app_root
from framework.util import Util

# 패키지
from .plugin import logger, package_name
from sqlalchemy.orm.attributes import flag_modified

#########################################################

app.config['SQLALCHEMY_BINDS'][package_name] = 'sqlite:///%s' % (os.path.join(path_app_root, 'data', 'db', '%s.db' % package_name))

class ModelSetting(db.Model):
    __tablename__ = '%s_setting' % package_name
    __table_args__ = {'mysql_collate': 'utf8_general_ci'}
    __bind_key__ = package_name

    id = db.Column(db.Integer, primary_key=True)
    key = db.Column(db.String(100), unique=True, nullable=False)
    value = db.Column(db.String, nullable=False)
 
    def __init__(self, key, value):
        self.key = key
        self.value = value

    def __repr__(self):
        return repr(self.as_dict())

    def as_dict(self):
        return {x.name: getattr(self, x.name) for x in self.__table__.columns}

    @staticmethod
    def get(key):
        try:
            return db.session.query(ModelSetting).filter_by(key=key).first().value.strip()
        except Exception as e:
            logger.error('Exception:%s %s', e, key)
            logger.error(traceback.format_exc())
            
    
    @staticmethod
    def get_int(key):
        try:
            return int(ModelSetting.get(key))
        except Exception as e:
            logger.error('Exception:%s %s', e, key)
            logger.error(traceback.format_exc())
    
    @staticmethod
    def get_bool(key):
        try:
            return (ModelSetting.get(key) == 'True')
        except Exception as e:
            logger.error('Exception:%s %s', e, key)
            logger.error(traceback.format_exc())

    @staticmethod
    def set(key, value):
        try:
            item = db.session.query(ModelSetting).filter_by(key=key).with_for_update().first()
            if item is not None:
                item.value = value.strip()
                db.session.commit()
            else:
                db.session.add(ModelSetting(key, value.strip()))
        except Exception as e:
            logger.error('Exception:%s %s', e, key)
            logger.error(traceback.format_exc())

    @staticmethod
    def to_dict():
        try:
            return Util.db_list_to_dict(db.session.query(ModelSetting).all())
        except Exception as e:
            logger.error('Exception:%s %s', e, key)
            logger.error(traceback.format_exc())


    @staticmethod
    def setting_save(req):
        try:
            for key, value in req.form.items():
                logger.debug('Key:%s Value:%s', key, value)
                if key in ['scheduler', 'is_running', 'global_scheduler_sub']:
                    continue
                if key == 'default_username' and value.startswith('==='):
                    continue
                entity = db.session.query(ModelSetting).filter_by(key=key).with_for_update().first()
                entity.value = value
            db.session.commit()
            return True                  
        except Exception as e: 
            logger.error('Exception:%s', e)
            logger.error(traceback.format_exc())
            logger.debug('Error Key:%s Value:%s', key, value)
            return False

#########################################################

class ModelOffcloud2Account(db.Model):
    __tablename__ = '%s_account' % package_name
    __table_args__ = {'mysql_collate': 'utf8_general_ci'}
    __bind_key__ = package_name

    id = db.Column(db.Integer, primary_key=True)
    created_time = db.Column(db.DateTime)
    reserved = db.Column(db.JSON)

    data = db.Column(db.JSON)

    username = db.Column(db.String)
    remote_type = db.Column(db.String)
    account_id = db.Column(db.String)
    option_id = db.Column(db.String)
    path = db.Column(db.String)

    def __init__(self, data):
        self.created_time = datetime.datetime.now()
        self.username = data['username']
        self.remote_type = data['type']
        self.account_id = data['accountId']
        self.option_id = data['remoteOptionId']
        self.path = data['path']

    def __repr__(self):
        return repr(self.as_dict())

    def as_dict(self):
        ret = {x.name: getattr(self, x.name) for x in self.__table__.columns}
        ret['created_time'] = self.created_time.strftime('%m-%d %H:%M:%S')
        return ret
    
    @staticmethod
    def get_list(by_dict=False):
        try:
            tmp = db.session.query(ModelOffcloud2Account).all()
            if by_dict:
                tmp = [x.as_dict() for x in tmp]
            return tmp
        except Exception as e:
            logger.error('Exception:%s', e)
            logger.error(traceback.format_exc())

    @staticmethod
    def save(data):
        try:
            db.session.query(ModelOffcloud2Account).delete()
            if data:
                for tmp in data:
                    entity = ModelOffcloud2Account(tmp)
                    db.session.add(entity)
            db.session.commit()
        except Exception as e:
            logger.error('Exception:%s', e)
            logger.error(traceback.format_exc())


    @staticmethod
    def get(username):
        try:
            tmp = db.session.query(ModelOffcloud2Account).filter_by(username=username).first()
            return tmp
        except Exception as e:
            logger.error('Exception:%s', e)
            logger.error(traceback.format_exc())









# 1버전은 키워드별 폴더를 다르게 가능했으나, 2버전은 그냥 하나의 폴더에서 받게 함.
class ModelOffcloud2Job(db.Model):
    __tablename__ = '%s_job' % package_name
    __table_args__ = {'mysql_collate': 'utf8_general_ci'}
    __bind_key__ = package_name

    id = db.Column(db.Integer, primary_key=True)
    created_time = db.Column(db.DateTime)
    reserved = db.Column(db.JSON)

    name = db.Column(db.String)
    rss_url = db.Column(db.String)
    username = db.Column(db.String)
    folderid = db.Column(db.String)
    mode = db.Column(db.String) #0:cache, 1:all, 2:on_cache_test
    cache_confirm_day = db.Column(db.Integer) # 며칠간 cache를 확인하겠는가?

    include_keyword = db.Column(db.String)
    exclude_keyword = db.Column(db.String)
    rss_list = db.relationship('ModelOffcloud2Item', backref='job', lazy=True) 
    
    # db_version 4
    use_tracer = db.Column(db.Boolean)
    mount_path = db.Column(db.String)
    move_path = db.Column(db.String)
    call_job = db.Column(db.String)

    # db_version 5
    is_http_torrent_rss = db.Column(db.Boolean)

    # db_version 6
    rss_mode = db.Column(db.Boolean) # True : 화이트리스트  , False : 블랙리스트
    rss_regex = db.Column(db.String) # \n 구분  블랙리스트이고 비어있으면 다 받음. 블랙리스트 매칭되면 안 받음. 
    # 화이트리스트이고 비어있으면 안 받음
    # 화이트리스트고 매칭되면 받음
    
    # db_version 7
    username2 = db.Column(db.String) # 계정 분산 처리용
    add_to_cloud = db.Column(db.String) # Cloud 추가 지연시간
    

    def __init__(self):
        self.created_time = datetime.datetime.now()
        self.use_tracer = False
        self.is_http_torrent_rss = False

    def __repr__(self):
        return repr(self.as_dict())

    def as_dict(self):
        ret = {x.name: getattr(self, x.name) for x in self.__table__.columns}
        ret['created_time'] = self.created_time.strftime('%m-%d %H:%M:%S')
        return ret

    @staticmethod
    def save(req):
        try:
            job_id = req.form['job_id']
            if job_id == '-1':
                job = ModelOffcloud2Job()
            else:
                job = db.session.query(ModelOffcloud2Job).filter_by(id=job_id).first()
            job.name = req.form['job_name']
            job.rss_url = req.form['job_rss_url']
            job.rss_mode = (req.form['job_rss_mode'] == 'True')
            job.rss_regex = req.form['job_rss_regex']
            job.username = req.form['job_username']
            job.username2 = req.form['job_username2']
            job.folderid = req.form['job_folderid']
            job.mode = req.form['job_mode']
            job.cache_confirm_day = int(req.form['job_cache_confirm_day'])
            # logger.debug(req.form['job_use_tracer'])
            job.use_tracer = (req.form['job_use_tracer'] == 'True')
            job.mount_path = req.form['job_mount_path']
            job.move_path = req.form['job_move_path']
            job.call_job = req.form['job_call_job']

            job.add_to_cloud = req.form['job_add_to_cloud']

            db.session.add(job)
            db.session.commit()
            return 'success'                  
        except Exception as e: 
            logger.error('Exception:%s', e)
            logger.error(traceback.format_exc())
            return 'fail'

    @staticmethod
    def get_list(by_dict=False):
        try:
            tmp = db.session.query(ModelOffcloud2Job).all()
            if by_dict:
                tmp = [x.as_dict() for x in tmp]
            return tmp
        except Exception as e:
            logger.error('Exception:%s', e)
            logger.error(traceback.format_exc())

    @staticmethod
    def remove(job_id):
        try:
            logger.debug('remove_job id:%s', job_id)
            job = db.session.query(ModelOffcloud2Job).filter_by(id=job_id).first()
            db.session.delete(job)
            db.session.commit()
            return 'success'
        except Exception as e: 
            logger.error('Exception:%s', e)
            logger.error(traceback.format_exc())
            return 'fail'
    
    @staticmethod
    def get_by_name(name):
        try:
            return db.session.query(ModelOffcloud2Job).filter_by(name=name).first()
        except Exception as e: 
            logger.error('Exception:%s', e)
            logger.error(traceback.format_exc())



class ModelOffcloud2Item(db.Model):
    __tablename__ = '%s_rss' % package_name
    __table_args__ = {'mysql_collate': 'utf8_general_ci'}
    __bind_key__ = package_name

    id = db.Column(db.Integer, primary_key=True)
    created_time = db.Column(db.DateTime)
    reserved = db.Column(db.JSON)

    job_id = db.Column(db.Integer, db.ForeignKey('%s_job.id' % package_name))
    created_time = db.Column(db.DateTime)
    title = db.Column(db.String())
    link = db.Column(db.String())
    status = db.Column(db.Integer)

    remote_time = db.Column(db.DateTime)
    completed_time = db.Column(db.DateTime)
    oc_folderid = db.Column(db.String)
    oc_requestId = db.Column(db.String)
    oc_status = db.Column(db.String)
    oc_createdOn = db.Column(db.String)
    oc_fileName = db.Column(db.String)
    oc_fileSize = db.Column(db.Integer)
    oc_error = db.Column(db.String)
    oc_retry_count = db.Column(db.Integer)
    oc_cached = db.Column(db.Boolean)
    oc_json = db.Column(db.JSON)
    #job = db.relationship('ModelOffcloud2Job', lazy=True)

    # DB Version 3
    link_to_notify_status = db.Column(db.String) #1이면 보고함.
    
    # DB Version 4
    torrent_info = db.Column(db.JSON)
    filename = db.Column(db.String)
    dirname = db.Column(db.String)
    filecount = db.Column(db.Integer)

    def __init__(self):
        self.created_time = datetime.datetime.now()
        self.status = 0
        self.oc_status = ''
        self.oc_cached = False


    def __repr__(self):
        return repr(self.as_dict())

    def as_dict(self):
        ret = {x.name: getattr(self, x.name) for x in self.__table__.columns}
        ret['created_time'] = self.created_time.strftime('%m-%d %H:%M:%S')
        ret['remote_time'] = self.remote_time.strftime('%m-%d %H:%M:%S') if self.remote_time is not None else ''
        ret['completed_time'] = self.completed_time.strftime('%m-%d %H:%M:%S') if self.completed_time is not None else ''
        ret['job'] = self.job.as_dict()
        try:
            ret['oc_fileSize'] = Util.sizeof_fmt(int(ret['oc_fileSize'])) if ret['oc_fileSize'] is not None else ret['oc_fileSize']
        except:
            pass
        
        return ret
    
    def make_torrent_info(self):
        try:
            if self.job.use_tracer and self.torrent_info is None and self.link.startswith('magnet'):
                from torrent_info import Logic as TorrentInfoLogic
                tmp = TorrentInfoLogic.parse_magnet_uri(self.link)
                if tmp is not None:
                    self.torrent_info = tmp
                    flag_modified(self, "torrent_info")
                    info = Util.get_max_size_fileinfo(tmp)
                    self.filename = u'%s' % info['filename']
                    self.dirname = u'%s' % info['dirname']
                    self.filecount = u'%s' % tmp['num_files']
                    return True
        except Exception as e: 
            logger.error('Exception:%s', e)
            logger.error(traceback.format_exc())
        return False


    @staticmethod
    def get_rss_list_by_scheduler(job):
        try:
            query = db.session.query(ModelOffcloud2Item) \
                .filter(ModelOffcloud2Item.job_id == job.id ) \
                .filter(ModelOffcloud2Item.status < 11 ) \
                .filter(ModelOffcloud2Item.created_time > datetime.datetime.now() + datetime.timedelta(days=job.cache_confirm_day*-1))
                # \
                #.filter(ModelOffcloud2Item.oc_status != 'NOSTATUS')
            return query.all()
        except Exception as e: 
            logger.error('Exception:%s', e)
            logger.error(traceback.format_exc())

    @staticmethod
    def remove(job_id):
        try:
            logger.debug('remove_job id:%s', job_id)
            db.session.query(ModelOffcloud2Item).filter_by(job_id=job_id).delete()
            db.session.commit()
            return 'success'
        except Exception as e: 
            logger.error('Exception:%s', e)
            logger.error(traceback.format_exc())
            return 'fail'

    @staticmethod
    def web_list(req):
        try:
            ret = {}
            page = 1
            page_size = 30
            job_id = ''
            search = ''
            if 'page' in req.form:
                page = int(req.form['page'])
            if 'search_word' in req.form:
                search = req.form['search_word']
            job_select = 'all'
            if 'job_select' in req.form:
                job_select = req.form['job_select']
            
            option = 'all'
            if 'option' in req.form:
                option = req.form['option']

            query = ModelOffcloud2Item.make_query(job_name=job_select, option=option, search=search)
            last_list_option_rss = '%s|%s|%s|%s' % (search, job_select, option, page)
            ModelSetting.set('last_list_option_rss', last_list_option_rss)

            count = query.count()
            query = (query.order_by(desc(ModelOffcloud2Item.id))
                        .limit(page_size)
                        .offset((page-1)*page_size)
                )
            logger.debug('ModelOffcloud2Item count:%s', count)

            lists = query.all()
            ret['list'] = [item.as_dict() for item in lists]
            ret['paging'] = Util.get_paging_info(count, page, page_size)
            return ret
        except Exception as e:
            logger.debug('Exception:%s', e)
            logger.debug(traceback.format_exc())

    @staticmethod
    def api_list(req):
        try:
            job = req.args.get('job')
            option = req.args.get('option')
            search = req.args.get('search')
            count = req.args.get('count')
            if count is None or count == '':
                count = 100

            query = ModelOffcloud2Item.make_query(job_name=job, option=option, search=search)
            query = (query.order_by(desc(ModelOffcloud2Item.id))
                        .limit(count)
                )
            lists = query.all()
            return lists
        except Exception as e:
            logger.debug('Exception:%s', e)
            logger.debug(traceback.format_exc())


    @staticmethod
    def make_query(job_name='all', option='all', search=''):
        try:
            query = db.session.query(ModelOffcloud2Item)
            job = None
            if job_name != 'all' and job_name is not None and job_name != '':
                job = ModelOffcloud2Job.get_by_name(job_name)
                query = query.filter_by(job_id=job.id)

            if search is not None and search != '':
                if search.startswith('magnet:?'):# 마그넷 검색을 위해서 추가한 부분
                    query = query.filter(ModelOffcloud2Item.link.like('%'+search+'%'))# 마그넷 검색을 위해서 추가한 부분
                elif search.find('|') != -1:
                    tmp = search.split('|')
                    conditions = []
                    for tt in tmp:
                        if tt != '':
                            conditions.append(ModelOffcloud2Item.title.like('%'+tt.strip()+'%') )
                    query = query.filter(or_(*conditions))
                elif search.find(',') != -1:
                    tmp = search.split(',')
                    for tt in tmp:
                        if tt != '':
                            query = query.filter(ModelOffcloud2Item.title.like('%'+tt.strip()+'%'))
                else:
                    query = query.filter(ModelOffcloud2Item.title.like('%'+search+'%'))

            if option == 'request_false':
                query = query.filter(ModelOffcloud2Item.status<6)
            elif option == 'request':
                query = query.filter(ModelOffcloud2Item.status<11, ModelOffcloud2Item.status>=6 )
            elif option == 'completed':
                query = query.filter_by(status=11)
            elif option == 'expire':
                if job is not None:
                    query = query.filter(ModelOffcloud2Item.created_time <= datetime.datetime.now() + datetime.timedelta(days=job.cache_confirm_day*-1))
                    query = query.filter(ModelOffcloud2Item.status<6)

                pass
            elif option == 'no_status':
                query = query.filter(ModelOffcloud2Item.status>=13)
           


            return query
        except Exception as e:
            logger.debug('Exception:%s', e)
            logger.debug(traceback.format_exc())

    





class ModelOffcloud2Cache(db.Model):
    __tablename__ = '%s_cache2' % package_name
    __table_args__ = {'mysql_collate': 'utf8_general_ci'}
    __bind_key__ = package_name

    id = db.Column(db.Integer, primary_key=True)
    created_time = db.Column(db.DateTime)
    reserved = db.Column(db.JSON)
    
    data = db.Column(db.JSON)
    content_type = db.Column(db.String)
    magnet = db.Column(db.String)
    name = db.Column(db.String)
    file_count = db.Column(db.Integer)
    size = db.Column(db.Integer)
    filename = db.Column(db.String)
    content_info = db.Column(db.JSON)


    def __init__(self):
        self.created_time = datetime.datetime.now()

    def __repr__(self):
        return repr(self.as_dict())

    def as_dict(self):
        ret = {x.name: getattr(self, x.name) for x in self.__table__.columns}
        ret['created_time'] = self.created_time.strftime('%m-%d %H:%M:%S')
        return ret
    

    @staticmethod
    def add(data):
        try:
            magnet = 'magnet:?xt=urn:btih:%s' % data['h']
            if db.session.query(ModelOffcloud2Cache).filter_by(magnet=magnet).count() == 0:
                e = ModelOffcloud2Cache()
                e.data = data
                e.content_type = data['t']
                e.magnet = magnet
                e.name = data['n']
                e.file_count = data['c']
                e.size = data['s']
                e.filename = data['f']
                if 'i' in data:
                    e.content_info = data['i']
                db.session.add(e)
                db.session.commit()
                return e
        except Exception as e: 
            logger.error('Exception:%s', e)
            logger.error(traceback.format_exc())
    
    @staticmethod
    def get_by_magnet(magnet):
        try:
            return db.session.query(ModelOffcloud2Cache).filter_by(magnet=magnet).first()
        except Exception as e: 
            logger.error('Exception:%s', e)
            logger.error(traceback.format_exc())

    @staticmethod
    def web_list(req):
        try:
            ret = {}
            page = 1
            page_size = 30
            job_id = ''
            search = ''
            if 'page' in req.form:
                page = int(req.form['page'])
            if 'search_word' in req.form:
                search = req.form['search_word']
            content_type = 'all'
            if 'type' in req.form:
                content_type = req.form['type']
            
            query = ModelOffcloud2Cache.make_query(content_type=content_type, search=search)
            count = query.count()
            query = (query.order_by(desc(ModelOffcloud2Cache.id))
                        .limit(page_size)
                        .offset((page-1)*page_size)
                )
            logger.debug('ModelOffcloud2Cache count:%s', count)
            lists = query.all()
            ret['list'] = [item.as_dict() for item in lists]
            ret['paging'] = Util.get_paging_info(count, page, page_size)
            return ret
        except Exception as e:
            logger.debug('Exception:%s', e)
            logger.debug(traceback.format_exc())

    @staticmethod
    def api_list(req):
        try:
            content_type = req.args.get('type')
            search = req.args.get('search')
            count = req.args.get('count')
            id_mod = req.args.get('id_mod')
            if count is None or count == '':
                count = 100
            query = ModelOffcloud2Cache.make_query(content_type=content_type, search=search, id_mod=id_mod)
            query = (query.order_by(desc(ModelOffcloud2Cache.id))
                        .limit(count)
                )
            lists = query.all()
            logger.debug('RET count : %s', len(lists))
            return lists
        except Exception as e:
            logger.debug('Exception:%s', e)
            logger.debug(traceback.format_exc())


    @staticmethod
    def make_query(content_type='all', search='', id_mod=''):
        try:
            query = db.session.query(ModelOffcloud2Cache)
            if content_type is not None and content_type != '' and content_type != 'all':
                if content_type.find('|') != -1:
                    tmp = content_type.split('|')
                    conditions = []
                    for tt in tmp:
                        if tt != '':
                            conditions.append(ModelOffcloud2Cache.content_type == tt.strip())
                    query = query.filter(or_(*conditions))
                else:
                    query = query.filter_by(content_type=content_type)

            if search is not None and search != '':
                if search.find('|') != -1:
                    tmp = search.split('|')
                    conditions = []
                    for tt in tmp:
                        if tt != '':
                            conditions.append(ModelOffcloud2Cache.name.like('%'+tt.strip()+'%') )
                    query = query.filter(or_(*conditions))
                elif search.find(',') != -1:
                    tmp = search.split(',')
                    for tt in tmp:
                        if tt != '':
                            query = query.filter(ModelOffcloud2Cache.name.like('%'+tt.strip()+'%'))
                else:
                    query = query.filter(ModelOffcloud2Cache.name.like('%'+search+'%'))
            if id_mod is not None and id_mod != '':
                tmp = id_mod.split('_')
                if len(tmp) == 2:
                    query = query.filter(ModelOffcloud2Cache.id % int(tmp[0]) == int(tmp[1]))

            return query
        except Exception as e:
            logger.debug('Exception:%s', e)
            logger.debug(traceback.format_exc())
