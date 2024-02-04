from peewee import *
from datetime import datetime

db = SqliteDatabase('./downloaded.db')

class UnknownField(object):
    def __init__(self, *_, **__): pass

class BaseModel(Model):
    class Meta:
        database = db

class Downloaded(BaseModel):
    id = AutoField(column_name='ID', null=False)
    chat_id = IntegerField(column_name='CHAT_ID', null=True)
    message_id = IntegerField(column_name='MESSAGE_ID', null=True)
    filename = TextField(column_name='FILENAME', null=True)
    caption = TextField(column_name='CAPTION', null=True)
    title = TextField(column_name='TITLE', null=True)
    mime_type = TextField(column_name='MIME_TYPE', null=True)
    media_size = IntegerField(column_name='MEDIA_SIZE', null=True)
    media_duration = IntegerField(column_name='MEDIA_DURATION', null=True)
    media_addtime = TextField(column_name='MEDIA_ADDTIME', null=True)
    chat_username = TextField(column_name='CHAT_USERNAME')
    chat_title = TextField(column_name='CHAT_TITLE', null=True)
    addtime = TextField(column_name='ADDTIME', null=True)
    status = IntegerField(column_name='STATUS', null=True)

    class Meta:
        table_name = 'Downloaded'

    def addto_localDB(self, dictit):
        try:
            db.connect()
            try:
                if Downloaded.get(chat_id = dict['chat_id'], message_id = dict['message_id'], status=1):
                    db.close()
                    return  # 成功则说明存在此条数据，无需再写入
            except:
                pass

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
            downloaded.chat_username = dictit['chat_username']
            downloaded.chat_title = dictit['chat_title']
            downloaded.adddate = datetime.now().strftime("%Y-%m-%d %H:%M")
            downloaded.status = 1
            downloaded.save()
            db.close()
        except:
            db.close()

    def search_by_ids(self, chat_id :int, message_id :int):
        try:
            db.connect()
            downloaded = Downloaded.get(chat_id = chat_id, message_id = message_id, status=1)
            if downloaded:
                # 存在，直接读取
                dictit = {
                    'chat_id': downloaded.chat_id,
                    'message_id': downloaded.message_id,
                    'filename': downloaded.filename,
                    'caption': downloaded.caption,
                    'title': downloaded.title,
                    'mime_type': downloaded.mime_type,
                    'media_size': downloaded.media_size,
                    'media_duration': downloaded.media_duration,
                    'media_addtime': downloaded.media_addtime,
                    'chat_username': downloaded.chat_username,
                    'chat_title': downloaded.chat_title,
                    'adddate': downloaded.adddate,
                    'status': downloaded.status
                }
                db.close()
                return dictit
            else:
                db.close()
                return None
        except:
            db.close()
            return None

    def is_exist_by_ids(self, chat_id :int, message_id :int):
        try:
            db.connect()
            if Downloaded.get(chat_id = chat_id, message_id = message_id, status=1):
                db.close()
                return True
            else:
                db.close()
                return False
        except:
            db.close()
            return False

    def is_exist_by_filename(self, mime_type :str, media_size :int, filename :str, title: str):
        try:
            db.connect()
            downloaded = Downloaded.get(mime_type = mime_type, media_size = media_size, status=1)
            if downloaded and (downloaded.filename == filename or downloaded.title == title):
                db.close()
                return True
            else:
                db.close()
                return False
        except:
            db.close()
            return False

    def max_by_ids(self, chat_id :int):
        max_id = 1
        try:
            db.connect()
            max_id = Downloaded.select(fn.Max(Downloaded.message_id)).scalar()
        except:
            print ('no data')
        finally:
            db.close()
        return max_id

class SqliteSequence(BaseModel):
    name = BareField(null=True)
    seq = BareField(null=True)

    class Meta:
        table_name = 'sqlite_sequence'
        primary_key = False
