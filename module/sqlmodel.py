import math
import os
from enum import Enum
from peewee import *
from datetime import datetime
from loguru import logger
from utils import format


source_db = os.path.join(os.path.abspath("."), "downloaded.db")
# memory_db = ":memory:"
# copyfile(source_db, memory_db)
# db = SqliteDatabase(memory_db)

db = SqliteDatabase(source_db)

class UnknownField(object):
    def __init__(self, *_, **__): pass

class BaseModel(Model):
    class Meta:
        database = db

class MsgStatusDB(Enum):
    """Download status"""

    Downloaded = 1
    Downloading = 2
    SkipDownload = 3
    MissingDownload = 4


class Downloaded(BaseModel):
    id = AutoField(primary_key=True, column_name='ID', null=True)
    chat_id = IntegerField(column_name='CHAT_ID', null=True)
    message_id = IntegerField(column_name='MESSAGE_ID', null=True)
    filename = CharField(max_length=200, column_name='FILENAME', null=True)
    caption = CharField(max_length=200, column_name='CAPTION', null=True)
    title = CharField(max_length=200, column_name='TITLE', null=True)
    mime_type = CharField(max_length=200, column_name='MIME_TYPE', null=True)
    media_size = IntegerField(column_name='MEDIA_SIZE', null=True)
    media_duration = IntegerField(column_name='MEDIA_DURATION', null=True)
    media_addtime = CharField(max_length=200, column_name='MEDIA_ADDTIME', null=True)
    chat_username = CharField(max_length=200, column_name='CHAT_USERNAME')
    chat_title = CharField(max_length=200, column_name='CHAT_TITLE', null=True)
    addtime = CharField(max_length=200, column_name='ADDTIME', null=True)
    status = IntegerField(column_name='STATUS') #
    type = CharField(max_length=200, column_name='TYPE', null=True) #

    class Meta:
        table_name = 'Downloaded'

    def create_table(table):
        u"""
        如果table不存在，新建table
        """
        if not table.table_exists():
            table.create_table()


    def getMsg(self, chat_username: str, message_id: int, status = 1, chat_id: int =None):
        if db.autoconnect == False:
            db.connect()
        if chat_username:
            try:
                downloaded = Downloaded.get(Downloaded.chat_username==chat_username, Downloaded.message_id==message_id, Downloaded.status == status)
                if downloaded:
                    return downloaded  # 说明存在此条数据
            except DoesNotExist:
                return None
        elif chat_id:
            try:
                downloaded = Downloaded.get(Downloaded.chat_id==chat_id, Downloaded.message_id==message_id, Downloaded.status == status)
                if downloaded:
                    return downloaded  # 说明存在此条数据
            except DoesNotExist:
                return None
        return None # 0为不存在

    def get2Down(self, chat_username: str):
        if db.autoconnect == False:
            db.connect()
        if chat_username:
            try:
                downloaded = Downloaded.select(Downloaded.message_id).where(Downloaded.chat_username == chat_username, Downloaded.status ==2)
                if downloaded:
                    return downloaded  # 说明存在此条数据
            except DoesNotExist:
                return None
        return None # 0为不存在

    def getStatusById(self, id: int):
        if db.autoconnect == False:
            db.connect()
        try:
            downloaded = Downloaded.get(id=id)
            if downloaded:
                return downloaded.status# 说明存在此条数据
        except DoesNotExist:
            return 0
        return 0 #0为不存在 1未已完成 2为下载中 3为跳过未系在 4为下载后丢失

    def getStatus(self, chat_id: int, message_id: int, chat_username = None ):
        if db.autoconnect == False:
            db.connect()
        if chat_id:
            try:
                downloaded = Downloaded.get(chat_id=chat_id, message_id=message_id)
                if downloaded:
                    return downloaded.status# 说明存在此条数据
            except DoesNotExist:
                if chat_username:
                    try:
                        downloaded = Downloaded.get(chat_username=chat_username, message_id=message_id)
                        if downloaded:
                            return downloaded.status  # 说明存在此条数据
                    except DoesNotExist:
                        return 0
        return 0 #0为不存在 1未已完成 2为下载中 3为跳过未系在 4为下载后丢失

    def msg_insert_to_db(self, dictit :dict):
        if db.autoconnect == False:
            db.connect()
        try:
            downloaded = Downloaded.get(chat_id=dictit['chat_id'], message_id=dictit['message_id'])
            if downloaded:
                return False# 说明存在此条数据，无法插入
        except DoesNotExist:
            pass
        try:
            # 出错说明不存在此条数据，需写入
            downloaded = Downloaded()
            downloaded.chat_id = dictit['chat_id']
            downloaded.message_id = dictit['message_id']
            downloaded.filename = dictit['filename']
            downloaded.caption = dictit['caption']
            downloaded.title = dictit['title']
            downloaded.mime_type = dictit['mime_type']
            downloaded.media_size = dictit['media_size']
            downloaded.media_duration = dictit['media_duration']
            downloaded.media_addtime = dictit['media_addtime']
            if not dictit['chat_username']:
                dictit['chat_username'] = ''
            downloaded.chat_username = dictit['chat_username']
            downloaded.chat_title = dictit['chat_title']
            downloaded.addtime = datetime.now().strftime("%Y-%m-%d %H:%M")
            downloaded.status = dictit['status']
            downloaded.type = dictit['msg_type']
            downloaded.save()
            # db.close()
            return True
        except Exception as e:
            logger.error(
                f"[{e}].",
                exc_info=True,
            )
            # db.close()
            return False

    def msg_update_to_db(self, dictit :dict):
        if db.autoconnect == False:
            db.connect()
        try:
            downloaded = Downloaded.get(chat_id=dictit['chat_id'], message_id=dictit['message_id'])
            if downloaded:
                downloaded.chat_id = dictit['chat_id']
                downloaded.message_id = dictit['message_id']
                downloaded.filename = dictit['filename']
                downloaded.caption = dictit['caption']
                downloaded.title = dictit['title']
                downloaded.mime_type = dictit['mime_type']
                downloaded.media_size = dictit['media_size']
                downloaded.media_duration = dictit['media_duration']
                downloaded.media_addtime = dictit['media_addtime']
                if not dictit['chat_username']:
                    dictit['chat_username'] = ''
                downloaded.chat_username = dictit['chat_username']
                downloaded.chat_title = dictit['chat_title']
                downloaded.addtime = datetime.now().strftime("%Y-%m-%d %H:%M")
                downloaded.status = dictit['status']
                downloaded.type = dictit['msg_type']
                downloaded.save()
                # db.close()
                return True
        except DoesNotExist:
            return False
        try:
            # 出错说明不存在此条数据，需写入
            downloaded = Downloaded()

            return True
        except Exception as e:
            logger.error(
                f"[{e}].",
                exc_info=True,
            )
            # db.close()
            return False

    def get_all_message_id(self):
        if db.autoconnect == False:
            db.connect()
        try:
            downloaded = Downloaded.select(Downloaded.id).where(Downloaded.status == 1)
            # db.close()
            return downloaded
        except DoesNotExist:
            # db.close()
            return False

    def get_all_message(self):
        if db.autoconnect == False:
            db.connect()
        try:
            downloaded = Downloaded.select().where(Downloaded.status == 1).order_by(Downloaded.id)
            # db.close()
            return downloaded
        except DoesNotExist:
            # db.close()
            return False

    def get_all_finished_message_from(self, start_id):
        if db.autoconnect == False:
            db.connect()
        try:
            downloaded = Downloaded.select().where(Downloaded.id >= start_id, Downloaded.status == 1).order_by(Downloaded.id)
            # db.close()
            return downloaded
        except DoesNotExist:
            # db.close()
            return False

    def get_all_message_from(self, start_id):
        if db.autoconnect == False:
            db.connect()
        try:
            downloaded = Downloaded.select().where(Downloaded.id >= start_id).order_by(Downloaded.id)
            # db.close()
            return downloaded
        except DoesNotExist:
            # db.close()
            return False




    def get_similar_files(self, msgdict, similar_min: float, sizerange_min: float):
        similar_file_list = []
        if db.autoconnect == False:
            db.connect()
        try:
            status_acc= [1,2] #完成下载或正在下载

            # 判断依据Step1.2： 找出类型一致 大小完全一致的文件记录
            result2 = Downloaded.select().where(Downloaded.mime_type == msgdict.get('mime_type'),
                                                   Downloaded.media_size== msgdict.get('media_size'),
                                                   Downloaded.status.in_(status_acc))

            # 判断依据Step1.3： 找出类型一致 文件名非常像 大小差别在10倍允许值范围的文件记录
            media_size_1 = math.floor(msgdict.get('media_size') * (1 - sizerange_min * 10))
            media_size_2 = math.floor(msgdict.get('media_size') * (1 + sizerange_min * 10))
            filename = msgdict.get('filename')
            if '.' in filename:
                filename = os.path.splitext(msgdict.get('filename'))[-2]

            file_core_name = format.process_string(filename)
            if not file_core_name or file_core_name =='' or len(file_core_name) <= 4:
                file_core_name = format.validate_title_clean(filename)

            result3 = Downloaded.select().where(Downloaded.mime_type == msgdict.get('mime_type'),
                                                (
                                                            Downloaded.filename % f'*{file_core_name.replace(" ", "*")}*' | Downloaded.title % f'*{file_core_name.replace(" ", "*")}*'),
                                                Downloaded.media_size.between(
                                                    media_size_1, media_size_2),
                                                Downloaded.status.in_(status_acc))

            # 判断依据Step1.4： 找出文档类型 文件名完全一致的文件记录
            result4 = Downloaded.select().where(Downloaded.type == msgdict.get('msg_type'),
                                                (
                                                            Downloaded.filename % f'*{file_core_name.replace(" ", "*")}*' | Downloaded.title % f'*{file_core_name.replace(" ", "*")}*'),
                                                Downloaded.status.in_(status_acc))



            downloaded = result2.union(result3).union(result4)
            if len(downloaded) >= 10:
                print('debug')
            # 判断依据Step2： 判断文件名是否接近
            for record in downloaded:
                if record.chat_id == msgdict.get('chat_id') and record.message_id == msgdict.get('message_id'): # 是自己
                    if record.status == 1:  # 如果是已完成状态 直接加入列表
                        similar_file_list.append(record)
                    else:
                        continue # 如果是未完成状态 则跳过
                else: # 不是自己 判断文件名是否接近
                    if '.' in msgdict.get('filename'):
                        filename = msgdict.get('filename').split(".")[-2]
                    else:
                        filename = msgdict.get('filename')
                    if '.' in record.filename:
                        filename_db = record.filename.split(".")[-2]
                    else:
                        filename_db = record.filename
                    title_db = record.title
                    similar = 0
                    break_all = False
                    for namea in [filename_db, title_db]:
                        if break_all:
                            break
                        for nameb in [filename, msgdict.get('title')]:
                            if format.string_sequence(namea, nameb):  # 是一个文件序列
                                break_all = True
                                similar = 0
                                break

                            sim_num = format.string_similar(namea, nameb)
                            if sim_num == 1:
                                similar = 1
                                break_all = True
                                break

                            namea1 = format.process_string(namea).replace(' ', '')
                            nameb1 = format.process_string(nameb).replace(' ', '')
                            if namea1 !='' and nameb1 !='' and (namea1 in nameb1 or nameb1 in namea1) and msgdict.get(
                                    'mime_type') == record.mime_type and msgdict.get('media_size') > 0 and math.isclose(
                                msgdict.get('media_size'),
                                record.media_size,
                                rel_tol=sizerange_min) and msgdict.get('media_duration') > 0 and math.isclose(
                                msgdict.get('media_duration'), record.media_duration,
                                rel_tol=sizerange_min):  # 文件名是包含关系 且大小时长都很相似
                                similar = 1
                                break_all = True
                                break

                            if similar < sim_num:
                                similar = sim_num
                    if similar >= similar_min:  # 名字高于相似度阈值
                        similar_file_list.append(record)
                        continue
            # db.close()
            if similar_file_list and len(similar_file_list)>=1:
                return similar_file_list
            else:
                return []
        except DoesNotExist:
            # db.close()
            return None
        except Exception as e:
            logger.error(
                f"[{e}].",
                exc_info=True,
            )
            # db.close()
            return None

    def get_last_read_message_id(self, chat_username: str):
        last_read_message_id = 1
        chat_username_qry = chat_username
        if db.autoconnect == False:
            db.connect()
        try:
            select_str = Downloaded.select(fn.Max(Downloaded.message_id)).where(
                Downloaded.chat_username == chat_username, Downloaded.status == 1)
            last_read_message_id = select_str.scalar()
            #print(f"==========={last_read_message_id}=============")
            # db.close()
            if last_read_message_id:
                return last_read_message_id
            else:
                return 1
        except DoesNotExist:
            logger.error(f"{chat_username}error")
            # db.close()
        except Exception as e:
            logger.error(
                f"[{chat_username}{e}].",
                exc_info=True,
            )

    def load_retry_msg_from_db(self):
        if db.autoconnect == False:
            db.connect()
        try:
            retry_ids = Downloaded.select(Downloaded.chat_id,Downloaded.chat_username,
                                          fn.GROUP_CONCAT(Downloaded.message_id).alias('retry_ids')).where(
                Downloaded.status == 2).group_by(
                Downloaded.chat_id, Downloaded.chat_username)

            dicts = []
            if retry_ids:
                for retry in retry_ids:
                    retryIds_str = str(retry.retry_ids).split(',')
                    retryIds = []
                    for retryId in retryIds_str:
                        retryIds.append(int(retryId))
                    dictit = {
                        'chat_id': retry.chat_id,
                        'chat_username' : retry.chat_username,
                        'ids_to_retry': set(retryIds)
                    }
                    dicts.append(dictit)
            # db.close()
            return dicts
        except DoesNotExist:
            return None
        except Exception as e:
            logger.error(
                f"[{e}].",
                exc_info=True,
            )
            # db.close()
            return None

    def retry_msg_insert_to_db(self, retry_chat_username :str, retry_msg_ids: []):
        if not retry_msg_ids:
            return False
        if db.autoconnect == False:
            db.connect()
        for msg_id in retry_msg_ids:
            try:
                downloaded = Downloaded.get(Downloaded.chat_username==retry_chat_username, Downloaded.message_id==int(msg_id))
                downloaded.status = 2
                downloaded.save()
                # db.close()
                return True
            except DoesNotExist:
                continue
            except Exception as e:
                logger.error(
                    f"[{e}].",
                    exc_info=True,
                )
                # db.close()
                return False



class SqliteSequence(BaseModel):
    name = BareField(null=True)
    seq = BareField(null=True)

    class Meta:
        table_name = 'sqlite_sequence'
        primary_key = False
