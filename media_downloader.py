"""Downloads media from telegram."""
import asyncio
import datetime
import logging
import math
import os
import shutil
import time
import re
from typing import List, Optional

import pyrogram
from loguru import logger
from rich.logging import RichHandler

from module.app import Application, ChatDownloadConfig, DownloadStatus, TaskNode
from module.bot import start_download_bot, stop_download_bot
from module.download_stat import update_download_status
from module.get_chat_history_v2 import get_chat_history_v2
from module.language import _t
from module.pyrogram_extension import (
    HookClient,
    fetch_message,
    get_extension,
    record_download_status,
    report_bot_download_status,
    set_max_concurrent_transmissions,
    set_meta_data,
    upload_telegram_chat,
)
from module.web import init_web
from utils.format import truncate_filename, validate_title, validate_title_clean, process_string
from utils.log import LogFilter
from utils.meta import print_meta
from utils.meta_data import MetaData
from utils.updates import check_for_updates

import sqlmodel

from shutil import move


logging.basicConfig(
    level=logging.INFO,
    format="%(message)s",
    datefmt='%m-%d %H:%M:%S',
    handlers=[RichHandler()]
)

CONFIG_NAME = "config.yaml"
DATA_FILE_NAME = "data.yaml"
APPLICATION_NAME = "media_downloader"
app = Application(CONFIG_NAME, DATA_FILE_NAME, APPLICATION_NAME)

queue_maxsize = 0
queue: asyncio.Queue = asyncio.Queue(maxsize = queue_maxsize)

RETRY_TIME_OUT = 3

similar_set = 0.92

logging.getLogger("pyrogram.session.session").addFilter(LogFilter())
logging.getLogger("pyrogram.client").addFilter(LogFilter())

logging.getLogger("pyrogram").setLevel(logging.WARNING)

db = sqlmodel.Downloaded()

total_queues = 0
total_queues_finished = 0


def _check_download_finish(media_size: int, download_path: str, ui_file_name: str):
    """Check download task if finish

    Parameters
    ----------
    media_size: int
        The size of the downloaded resource
    download_path: str
        Resource download hold path
    ui_file_name: str
        Really show file name

    """
    download_size = os.path.getsize(download_path)
    chat_username = download_path.split('/')[-3]
    if media_size == download_size:
        logger.success(f"{chat_username}{_t('Successfully downloaded')} - {ui_file_name} | 剩余下载队列长度:{queue.qsize()} \n")
    else:
        logger.warning(
            f"{_t('Media downloaded with wrong size')}: "
            f"{download_size}, {_t('actual')}: "
            f"{media_size}, {_t('file name')}: {ui_file_name}"
        )
        os.remove(download_path)
        raise pyrogram.errors.exceptions.bad_request_400.BadRequest()


def _move_to_download_path(temp_download_path: str, download_path: str):
    """Move file to download path

    Parameters
    ----------
    temp_download_path: str
        Temporary download path

    download_path: str
        Download path

    """

    directory, _ = os.path.split(download_path)
    os.makedirs(directory, exist_ok=True)
    shutil.move(temp_download_path, download_path)


def _check_timeout(retry: int, _: int):
    """Check if message download timeout, then add message id into failed_ids

    Parameters
    ----------
    retry: int
        Retry download message times

    """
    if retry == 2:
        return True
    return False


def _can_download(_type: str, file_formats: dict, file_format: Optional[str]) -> bool:
    """
    Check if the given file format can be downloaded.

    Parameters
    ----------
    _type: str
        Type of media object.
    file_formats: dict
        Dictionary containing the list of file_formats
        to be downloaded for `audio`, `document` & `video`
        media types
    file_format: str
        Format of the current file to be downloaded.

    Returns
    -------
    bool
        True if the file format can be downloaded else False.
    """
    if _type in ["audio", "document", "video"]:
        allowed_formats: list = file_formats[_type]
        if not file_format in allowed_formats and allowed_formats[0] != "all":
            return False
    return True

def get_aka_file_by_id(media_dict: dict) -> list:
    file_path = media_dict.get('file_fullname')
    return get_aka_file_by_path(file_path)


def get_aka_file_by_path(file_path) -> list:
    file_dir, filename = os.path.split(file_path)
    filename_pre = re.findall(r"\[.+?\]", filename)[0]
    return find_files_with_prefix(file_dir, filename_pre)

def find_files_with_prefix(directory, prefix):
    """
    在给定目录中查找以指定前缀开头的所有文件。

    Args:
        directory (str): 要搜索的目录路径。
        prefix (str): 要匹配的前缀。

    Returns:
        list: 以指定前缀开头的文件路径列表。
    """
    matching_files = []
    for root, dirs, files in os.walk(directory):
        for file in files:
            if file.startswith(prefix):
                matching_files.append(os.path.join(root, file))
    return matching_files


def is_aka_exist(media_dict: dict) -> bool: #存在等价文件 chat_id message_id 一致
    file_path = media_dict.get('file_fullname')
    file_dir = os.path.dirname(file_path)
    filename_pre = f"[{media_dict.get('message_id')}]"
    if _is_exist(file_path) or (os.path.exists(file_dir) and find_file_starting_with(file_dir, filename_pre)):
        return True

def is_exist_by_forward(media_dict: dict) -> bool:
    if media_dict.get('msg_from'):
        _, file_save_url, _ = get_media_info_str(media_dict.get('msg_from_chat_username'), media_dict.get('msg_from_chat_id'),
                           media_dict.get('msg_from_message_id'), media_dict.get('msg_filename'),
                           media_dict.get('msg_caption'), media_dict.get('media_type'))
        if _is_exist(file_save_url):
            return True
    return False

def _is_exist(file_path: str) -> bool:
    """
    Check if a file exists and it is not a directory.

    Parameters
    ----------
    file_path: str
        Absolute path of the file to be checked.

    Returns
    -------
    bool
        True if the file exists else False.
    """
    return not os.path.isdir(file_path) and os.path.exists(file_path)

def find_file_starting_with(directory, prefix):
    """
    在给定目录中查找以指定前缀开头的文件。

    Args:
        directory (str): 目录路径。
        prefix (str): 期望的前缀。

    Returns:
        str or None: 如果找到文件，则返回文件名；否则返回None。
    """
    for filename in os.listdir(directory):
        if filename.startswith(prefix):
            return filename
    return None


def is_exist_in_alldb(msgdict: dict):
    #TODO 增加判断在其他为知是否存在文件大小一致 文件名类型一样 文件名相似的文件
    if db.file_similar_rate(msgdict['mime_type'], msgdict['media_size'],msgdict['filename'],msgdict['title']) >= similar_set:
        return True
    else:
        return False


# pylint: disable = R0912
def get_media_info_str(msg_chat_username: str, msg_chat_id: int, msg_message_id: int, msg_filename: str, msg_caption: str, media_type: str):

    if msg_filename and msg_filename != '':
        msg_file_onlyname, msg_file_format = os.path.splitext(msg_filename)
    else:
        msg_file_onlyname = 'No_Name'
        msg_file_format = '.unknown'

    if msg_caption and msg_caption != '' and (
            'telegram' in msg_filename.lower() or re.sub(r'[._\-\s]', '',
                                                         msg_file_onlyname).isdigit()):
        msg_filename = app.get_file_name(msg_message_id,
                                         f"{msg_caption}{msg_file_format}",
                                         msg_caption)
    else:
        msg_filename = validate_title(app.get_file_name(msg_message_id, msg_filename, msg_caption))

        # 处理存储用message衍生信息
    dirname = validate_title(f"[{str(msg_chat_id)}]{msg_chat_username}")

    file_save_path = os.path.join(app.get_file_save_path(media_type, dirname, ''),
                                  str(int(msg_message_id) // 100 * 100).zfill(6))
    temp_save_path = os.path.join(app.temp_save_path, dirname,
                                  str(int(msg_message_id) // 100 * 100).zfill(6))

    file_save_url = os.path.join(file_save_path, truncate_filename(msg_filename))
    temp_save_url = os.path.join(temp_save_path, truncate_filename(msg_filename))

    return msg_filename, file_save_url, temp_save_url

async def _get_media_meta(
    message: pyrogram.types.Message
) -> dict:
    """Extract file name and file id from media object.

    Parameters
    ----------
    message : pyrogram.types.Message

    Returns
    -------
    media_dict : dict
    """

    #待返回dict初始化
    media_dict = {}
    msg_time = ''
    msg_date = ''
    try:
        # TODO 下面有bug 当是转发信息时 如果config里没有转发源 则后面会出错 先暂时不记录原始chat_id
        msg_from = False  # 是否转发的信息
        msg_real_chat_id = 0 - message.chat.id - 1000000000000
        msg_real_chat_username = message.chat.username
        msg_real_message_id = message.id
        msg_real_chat_title = message.chat.title

        if message.forward_from_chat and message.forward_from_chat.id and message.forward_from_message_id:
            msg_from_chat_id = 0 - message.forward_from_chat.id - 1000000000000
            msg_from_chat_username = message.forward_from_chat.username
            msg_from_message_id = message.forward_from_message_id
            msg_from_chat_title = validate_title(message.forward_from_chat.title)
            msg_from = True

        if message.date:
            msg_time = message.date.strftime("%Y-%m-%d %H:%M")
            msg_date = message.date.strftime(app.date_format)

        msg_caption = getattr(message, "caption", '')
        msg_media_group_id = getattr(message, "media_group_id", None)

        if msg_caption:
            msg_caption = validate_title(msg_caption)
            app.set_caption_name(msg_real_message_id, msg_media_group_id, msg_caption)
        else:
            msg_caption = app.get_caption_name(msg_real_message_id, msg_media_group_id)

        if message.audio and message.audio != '':
            msg_type = 'audio'
            msg_filename = message.audio.file_name
            msg_duration = message.audio.duration
            msg_size = message.audio.file_size
            media_obj = message.audio
            msg_file_format = get_extension(media_obj.file_id, getattr(media_obj, "mime_type", "")).replace('.', '')
            msg_title = ''
        elif message.video and message.video != '':
            msg_type = 'video'
            msg_filename = message.video.file_name
            msg_duration = message.video.duration
            msg_size = message.video.file_size
            media_obj = message.video
            msg_file_format = get_extension(media_obj.file_id, getattr(media_obj, "mime_type", "")).replace('.', '')
            msg_title = ''
        elif message.photo and message.photo != '':
            msg_type = 'photo'
            msg_filename = f"[{msg_real_chat_username}]{msg_real_message_id}.jpg"
            msg_duration = 0
            msg_size = message.photo.file_size
            media_obj = message.photo
            msg_file_format = get_extension(media_obj.file_id, getattr(media_obj, "mime_type", "")).replace('.', '')
            msg_title = ''
        elif message.document and message.document != '':
            msg_type = 'document'
            msg_filename = message.document.file_name
            msg_duration = 0
            msg_size = message.document.file_size
            media_obj = message.document
            msg_file_format = get_extension(media_obj.file_id, getattr(media_obj, "mime_type", "")).replace('.', '')
            msg_title = ''
        else:
            logger.info(
                f"无需处理的媒体类型: ",
                exc_info=True,
            )
            return None

        msg_filename, file_save_url, temp_save_url = get_media_info_str(msg_real_chat_username, msg_real_chat_id, msg_real_message_id, msg_filename, msg_caption, msg_type)

        if not msg_title or msg_title == '':
            msg_title = validate_title_clean(process_string(os.path.splitext(msg_filename)[0]))

        if not msg_filename or 'None' in file_save_url:
            logger.error(f"[{msg_real_chat_username}]{msg_real_message_id}: ",exc_info=True,)

        if msg_from:
            #print('msg_from')
            media_dict = {
                'chat_id': msg_real_chat_id,
                'message_id': msg_real_message_id,
                'filename': msg_filename,
                'caption': msg_caption,
                'title': msg_title,
                'mime_type': msg_file_format,
                'media_size': msg_size,
                'media_duration': msg_duration,
                'media_addtime': msg_time,
                'chat_username': msg_real_chat_username,
                'chat_title': msg_real_chat_title,
                'file_fullname': file_save_url,
                'temp_file_fullname': temp_save_url,
                'file_format': msg_type,
                'msg_from': msg_from,
                'msg_from_chat_id': msg_from_chat_id,
                'msg_from_chat_username': msg_from_chat_username,
                'msg_from_message_id': msg_from_message_id,
                'msg_from_chat_title': msg_from_chat_title,
            }
        else:
            media_dict = {
                'chat_id': msg_real_chat_id,
                'message_id': msg_real_message_id,
                'filename': msg_filename,
                'caption': msg_caption,
                'title': msg_title,
                'mime_type': msg_file_format,
                'media_size': msg_size,
                'media_duration': msg_duration,
                'media_addtime': msg_time,
                'chat_username': msg_real_chat_username,
                'chat_title': msg_real_chat_title,
                'file_fullname': file_save_url,
                'temp_file_fullname': temp_save_url,
                'file_format': msg_type,
            }
    except Exception as e:
        logger.error(
            f"Message[{message.id}]: "
            f"{_t('some info is missed')}:\n[{e}].",
            exc_info=True,
        )

    return media_dict


async def add_download_task(
    message: pyrogram.types.Message,
    node: TaskNode,
):
    global total_queues
    global total_queues_finished
    """Add Download task"""
    if message.empty:
        return False
    media_dict = await _get_media_meta(message)
    file_path = media_dict.get('file_fullname')
    if not _is_exist(file_path) or os.path.getsize(file_path) <= media_dict.get('media_size'):  #简单判断 严格不同就进入队列
        node.download_status[message.id] = DownloadStatus.Downloading
        await queue.put((message, node))
        logger.info(f"加入队列[{media_dict.get('chat_username')}]{media_dict.get('filename')}   当前队列长：{queue.qsize()}")
        node.total_task += 1
        total_queues +=1
        await update_download_status(total_queues_finished, total_queues, '-1',
                                     f'当前已完成{total_queues_finished}个文件', time.time(), node, node.client)

        return True
    else:
        node.download_status[message.id] = DownloadStatus.SuccessDownload
        node.total_task += 1
        # 写入数据库 记录已完成下载
        media_dict['status'] = 1
        insert_into_db(media_dict)
        logger.info(
            f"[{media_dict.get('chat_username')}]{media_dict.get('filename')}已存在   当前已处理：{node.total_task}")
        return False

async def download_task_v2(
    client: pyrogram.Client, message: pyrogram.types.Message, node: TaskNode, media_dict: dict
):
    return

async def download_task(
    client: pyrogram.Client, message: pyrogram.types.Message, node: TaskNode
):
    """Download and Forward media"""

    download_status, file_name = await download_media(
        client, message, app.media_types, app.file_formats, node
    )

    if not node.bot:
        app.set_download_id(node, message.id, download_status)

    node.download_status[message.id] = download_status

    file_size = os.path.getsize(file_name) if file_name else 0

    await upload_telegram_chat(
        client,
        node.upload_user if node.upload_user else client,
        app,
        node,
        message,
        download_status,
        file_name,
    )

    # rclone upload
    if (
        not node.upload_telegram_chat_id
        and download_status is DownloadStatus.SuccessDownload
    ):
        if await app.upload_file(file_name):
            node.upload_success_count += 1

    await report_bot_download_status(
        node.bot,
        node,
        download_status,
        file_size,
    )


# pylint: disable = R0915,R0914
def insert_into_db(media_dict:dict):
    # TODO: 写入数据库 记录已完成下载
    try:
        db_status = db.getStatus(chat_id=media_dict['chat_id'], message_id=media_dict['message_id'])
        if db_status == 0:
            db.msg_insert_to_db(media_dict)
            # logger.info(
            #     f"Message[{media_dict.get('chat_username')}]:[{media_dict['message_id']}]{media_dict['filename']}: inserted into db"
            # )
        elif db_status == 1:
            # logger.info(
            #     f"Message[{media_dict.get('chat_username')}]:[{media_dict['message_id']}]{media_dict['filename']}: is already in db"
            # )
            1
        elif db_status == 2:
            db.msg_update_to_db(media_dict)
            # logger.info(
            #     f"Message[{media_dict.get('chat_username')}]:[{media_dict['message_id']}]{media_dict['filename']}: updated into db"
            # )
        elif db_status == 3:
            db.msg_update_to_db(media_dict)
            # logger.info(
            #     f"Message[{media_dict.get('chat_username')}]:[{media_dict['message_id']}]{media_dict['filename']}: updated into db"
            # )
    except Exception as e:
        # pylint: disable = C0301
        logger.error(
            f"[{e}].",
            exc_info=True,
        )


def save_chunk_to_file(chunk, file_path, file_name):
    if not os.path.exists(file_path):
        os.makedirs(file_path)
    file_url = os.path.join(file_path, file_name)
    chunk_size = len(chunk)
    if not _is_exist(file_url) or os.path.getsize(file_url) < chunk_size:
        with open(file_url, "wb") as f:
            f.write(chunk)
    if _is_exist(file_url) and os.path.getsize(file_url) == chunk_size:
        return True
    else:
        return False

def merge_chunkfile(folder_path, output_file, file_size, batch_size=100):
    # 获取文件夹中的所有文件
    files = os.listdir(folder_path)
    files.sort()  # 确保文件按照一致的顺序合并

    if _is_exist(output_file):
        os.remove(output_file)

    # 初始化一个空的合并数据
    merged_data = b''

    # 按批次迭代文件
    for i in range(0, len(files), batch_size):
        batch_files = files[i:i + batch_size]
        for file in batch_files:
            file_path = os.path.join(folder_path, file)
            with open(file_path, 'rb') as f:
                merged_data += f.read()

        # 将批次写入输出文件
        with open(output_file, 'ab') as out_file:
            out_file.write(merged_data)
            merged_data = b''  # 为下一批次重置 merged_data

    if _is_exist(output_file) and os.path.getsize(output_file) == file_size:
        return True
    else:
        os.remove(output_file)
        return False


@record_download_status
async def download_media(
    client: pyrogram.client.Client,
    message: pyrogram.types.Message,
    media_types: List[str],
    file_formats: dict,
    node: TaskNode,
):
    """
    Download media from Telegram.

    Each of the files to download are retried 3 times with a
    delay of 5 seconds each.

    Parameters
    ----------
    client: pyrogram.client.Client
        Client to interact with Telegram APIs.
    message: pyrogram.types.Message
        Message object retrieved from telegram.
    media_types: list
        List of strings of media types to be downloaded.
        Ex : `["audio", "photo"]`
        Supported formats:
            * audio
            * document
            * photo
            * video
            * voice
    file_formats: dict
        Dictionary containing the list of file_formats
        to be downloaded for `audio`, `document` & `video`
        media types.

    Returns
    -------
    int
        Current message id.
    """

    # pylint: disable = R0912

    global total_queues_finished

    file_name: str = ""
    ui_file_name: str = ""
    temp_file_name: str = ""
    task_start_time: float = time.time()
    media_dict = {}
    media_size = 0
    _media = None
    message = await fetch_message(client, message)
    message_id = message.id

    if not (message.audio or message.video or message.photo or message.document):
        # logger.info(f"不是媒体类型 自动跳过[{message.chat.username}]{message.id}")
        return DownloadStatus.SkipDownload, None

    try:
        media_dict = await _get_media_meta(message)
        message_id = media_dict.get('message_id')
        _media = media_dict
        file_name = media_dict.get('file_fullname')
        temp_file_name = media_dict.get('temp_file_fullname')
        file_format = media_dict.get('mime_type')
        media_size = media_dict.get('media_size')
        _type = media_dict.get('file_format')

        msg_chat_username= media_dict.get('chat_username')

        ui_file_name = file_name.split('/')[-1]
        if app.hide_file_name:
            ui_file_name = f"****{os.path.splitext(file_name.split('/')[0])}"

        if _can_download(_type, file_formats, file_format):
            # 增加广义文件存在的判断 即文件大小一致 类型相同 文件名相似度高 即认为一致
            if is_aka_exist(media_dict):
                aka_files = get_aka_file_by_path(file_name)
                for file_name_local in aka_files:
                    file_size_local = os.path.getsize(file_name_local)
                    if file_size_local and file_size_local == media_size: #本地文件存在且一样大
                        if file_name_local != file_name: #文件名不一致则按新规则命名
                            move(file_name_local, file_name)
                            logger.info(
                                f"[{msg_chat_username}]:{os.path.split(file_name_local)[1]}被重命名为[{msg_chat_username}]:{os.path.split(file_name)[1]} "
                            )

                        # 写入数据库 记录已完成下载
                        media_dict['status'] = 1
                        insert_into_db(media_dict)

                if os.path.getsize(file_name) == media_size:
                    return DownloadStatus.SuccessDownload, None
            else:
                if media_dict.get('msg_from'):#不存在相同文件 但因为是转发msg 所以有可能在其他channel里已经下载了 判断一下
                    _, from_file_name, _ = get_media_info_str(media_dict.get('msg_from_chat_username'),
                                                             media_dict.get('msg_from_chat_id'),
                                                             media_dict.get('msg_from_message_id'),
                                                             re.sub(r"^\[\d+\]", "", media_dict.get('filename')),
                                                             media_dict.get('caption'),
                                                             media_dict.get('file_format'))
                from_aka_files = get_aka_file_by_path(from_file_name)
                if from_aka_files:
                    for file_name_local in from_aka_files:
                        file_size_local = os.path.getsize(file_name_local)
                        if file_size_local and file_size_local == media_size:  # 转发源本地文件存在且一样大
                            if file_name_local != from_file_name:  # 文件名不一致则按新规则命名
                                move(file_name_local, from_file_name)
                                logger.info(
                                    f"[{media_dict.get('msg_from_chat_username')}]{os.path.split(file_name_local)[1]}被重命名为[{media_dict.get('msg_from_chat_username')}]{os.path.split(from_file_name)[1]} "
                                )
                        # 写入数据库 记录已完成下载
                        media_dict['status'] = 1
                        insert_into_db(media_dict)
                    return DownloadStatus.SuccessDownload, None
                else:
                    # 不存在相同文件 但有可能在其他channel里已经下载了 用db来判断
                    if is_exist_in_alldb(media_dict):
                        logger.info(
                            f"[{msg_chat_username}]{ui_file_name} 可能"
                            f"{_t('already download,download skipped')}.\n"
                        )
                        return DownloadStatus.SuccessDownload, None
        else:
            return DownloadStatus.SkipDownload, None

        # break
    except Exception as e:
        logger.error(
            f"Message[{message_id}]: "
            f"{_t('could not be downloaded due to following exception')}:\n[{e}].",
            exc_info=True,
        )
        return DownloadStatus.FailedDownload, None
    if _media is None:
        return DownloadStatus.SkipDownload, None

    for retry in range(3):
        try:
            down_byte = 0
            if media_size >= 1024 * 1024 * 10: #大于10M 就分快下载
                temp_file_path = os.path.dirname(temp_file_name)
                chunk_dir = f"{temp_file_path}/{message_id}_chunk"
                chunk_count = int(media_size / 1024 / 1024) + 1
                if os.path.exists(chunk_dir): # 存在文件夹说明之前下载到半途失败了 需要继续处理
                    for chunk_it in range (0, chunk_count):
                        chunk_filename = f"{str(int(chunk_it)).zfill(8)}"
                        chunk_fileURL = os.path.join(chunk_dir, chunk_filename)
                        if not _is_exist(chunk_fileURL) or os.path.getsize(chunk_fileURL) < 1024 * 1024:
                            async for chunk in client.stream_media(message, offset = chunk_it, limit=1):#从第chunk_it个开始下载，下载1个块
                                i = 0
                                while not save_chunk_to_file(chunk, chunk_dir, chunk_filename):
                                    await asyncio.sleep(0.5)
                                    i += 1
                                    if i >=3:
                                        raise pyrogram.errors.exceptions.bad_request_400.BadRequest
                        down_byte += os.path.getsize(chunk_fileURL)
                        if str(chunk_it).endswith('99'):
                            logging.info(f'[{msg_chat_username}]msg_id={message_id}  已下载{int(down_byte/media_size*100)}%:{"{:.2f}".format(down_byte/1024/1024)}M/共{"{:.2f}".format(media_size/1024/1024)}M') #100M汇报一次
                        await update_download_status(down_byte, media_size ,message_id,ui_file_name,task_start_time,node,client)

                else:
                    down_byte = 0
                    chunk_it = 0
                    async for chunk in client.stream_media(message):  # 下载全部
                        chunk_filename = f"{str(int(chunk_it)).zfill(8)}"
                        i = 0
                        while not save_chunk_to_file(chunk, chunk_dir, chunk_filename):
                            await asyncio.sleep(0.5)
                            i += 1
                            if i >= 3:
                                raise pyrogram.errors.exceptions.bad_request_400.BadRequest
                        chunk_it += 1
                        down_byte += len(chunk)
                        if str(chunk_it).endswith('99'):
                            logging.info(f'[{msg_chat_username}]msg_id={message_id}  已下载{int(down_byte/media_size*100)}%:{"{:.2f}".format(down_byte/1024/1024)}M/共{"{:.2f}".format(media_size/1024/1024)}M') #100M汇报一次
                        await update_download_status(down_byte, media_size ,message_id,ui_file_name,task_start_time,node,client)


                #logging.info(f'[{msg_chat_username}]msg_id={message_id}  已下载{"{:.2f}".format(down_byte/media_size*100)}%:{"{:.2f}".format(down_byte/1024/1024)}M/共{"{:.2f}".format(media_size/1024/1024)}M')
                if down_byte == media_size:
                    i = 0
                    while not merge_chunkfile(folder_path=chunk_dir, output_file=temp_file_name, file_size=media_size, batch_size=100):
                        await asyncio.sleep(1)
                        i += 1
                        if i >= 3:
                            raise pyrogram.errors.exceptions.bad_request_400.BadRequest
                    # logger.info(
                    #     f"[{msg_chat_username}]{message_id}: 分段下载并合并完毕！！！"
                    # )
                    if _is_exist(temp_file_name) and os.path.getsize(temp_file_name) == media_size:
                        shutil.rmtree(chunk_dir)

                    temp_download_path = temp_file_name
                else:
                    raise pyrogram.errors.exceptions.bad_request_400.BadRequest
            else:
                temp_download_path = await client.download_media(
                    message,
                    file_name=temp_file_name,
                    progress=update_download_status,
                    progress_args=(
                        message_id,
                        ui_file_name,
                        task_start_time,
                        node,
                        client,
                    ),
                )

            if temp_download_path and isinstance(temp_download_path, str):
                _check_download_finish(media_size, temp_download_path, ui_file_name)
                await asyncio.sleep(0.5)
                _move_to_download_path(temp_download_path, file_name)
                # TODO: if not exist file size or media
                media_dict['status'] = 1
                # 写入数据库 记录已完成下载
                insert_into_db(media_dict)
                total_queues_finished += 1
                await update_download_status(total_queues_finished, total_queues, '-1',
                                             f'当前已完成{total_queues_finished}个文件', time.time(), node, node.client)

                return DownloadStatus.SuccessDownload, file_name
        except pyrogram.errors.exceptions.bad_request_400.BadRequest:
            logger.warning(
                f"[{msg_chat_username}]{message_id}: {_t('file reference expired, refetching')}..."
            )
            await asyncio.sleep(RETRY_TIME_OUT)
            message = await fetch_message(client, message)
            if _check_timeout(retry, message_id):
                # pylint: disable = C0301
                logger.error(
                    f"[{msg_chat_username}]{message_id}]: "
                    f"{_t('file reference expired for 3 retries, download skipped.')}"
                )
        except pyrogram.errors.exceptions.flood_420.FloodWait as wait_err:
            await asyncio.sleep(wait_err.value)
            logger.warning("[{msg_chat_username}]{}: FlowWait {}", message_id, wait_err.value)
            _check_timeout(retry, message_id)
        except TypeError:
            # pylint: disable = C0301
            logger.warning(
                f"{_t('Timeout Error occurred when downloading Message')}[{msg_chat_username}]{message_id}, "
                f"{_t('retrying after')} {RETRY_TIME_OUT} {_t('seconds')}"
            )
            await asyncio.sleep(RETRY_TIME_OUT)
            if _check_timeout(retry, message_id):
                logger.error(
                    f"[{msg_chat_username}]{message_id}: {_t('Timing out after 3 reties, download skipped.')}"
                )
        except Exception as e:
            # pylint: disable = C0301
            logger.error(
                f"[{msg_chat_username}]{message_id}]: "
                f"{_t('could not be downloaded due to following exception')}:\n[{e}].",
                exc_info=True,
            )
            break

    return DownloadStatus.FailedDownload, None


def _load_config():
    """Load config"""
    app.load_config()


def _check_config() -> bool:
    """Check config"""
    print_meta(logger)
    try:
        _load_config()
        logger.add(
            os.path.join(app.log_file_path, "tdl.log"),
            rotation="10 MB",
            retention="10 days",
            level=app.log_level,
        )
    except Exception as e:
        logger.exception(f"load config error: {e}")
        return False

    return True


async def worker(client: pyrogram.client.Client):
    """Work for download task"""
    while app.is_running:
        #TODO 增加守护进程 当队列为空时 跟踪指定频道 有新消息则加入队列
        try:
            item = await queue.get()
            message = item[0]
            node: TaskNode = item[1]

            if node.is_stop_transmission:
                continue

            if node.client:
                await download_task(node.client, message, node)
            else:
                await download_task(client, message, node)
        except Exception as e:
            logger.exception(f"{e}")


async def download_chat_task(
    client: pyrogram.Client,
    chat_download_config: ChatDownloadConfig,
    node: TaskNode,
):
    """Download all task"""
    messages_iter = get_chat_history_v2(
        client,
        node.chat_id,
        limit=node.limit,
        max_id=node.end_offset_id,
        offset_id=chat_download_config.last_read_message_id,
        reverse=True,
    )

    chat_download_config.node = node
    logger.info(f"开始处理[{node.chat_id}]")

    if chat_download_config.ids_to_retry:
        logger.info(f"{_t('Downloading files failed during last run')}...")
        downloading_messages: list = await client.get_messages(  # type: ignore
            chat_id=node.chat_id, message_ids=chat_download_config.ids_to_retry
        )

        for message in downloading_messages:
            if not (message.audio or message.video or message.photo or message.document):
                logger.info(f"不是媒体类型 自动跳过[{message.chat.username}]{message.id}")
                continue
            await add_download_task(message, node)

    async for message in messages_iter:  # type: ignore
        meta_data = MetaData()

        # if str(queue.qsize()).endswith('00'):
        #     print(queue.qsize())

        if not(message.audio or message.video or message.photo or message.document):
            # logger.info(f"不是媒体类型 自动跳过[{message.chat.username}]{message.id}")
            continue

        caption = message.caption
        if caption:
            caption = validate_title(caption)
            app.set_caption_name(node.chat_id, message.media_group_id, caption)
        else:
            caption = app.get_caption_name(node.chat_id, message.media_group_id)

        set_meta_data(meta_data, message, caption)

        #TODO 下面的貌似没用 考虑后面增加跳过指定文件的功能
        if app.need_skip_message(chat_download_config, message.id):
            continue

        if app.exec_filter(chat_download_config, meta_data):
            await add_download_task(message, node)
        else:
            node.download_status[message.id] = DownloadStatus.SkipDownload
            await upload_telegram_chat(
                client,
                node.upload_user,
                app,
                node,
                message,
                DownloadStatus.SkipDownload,
            )

    chat_download_config.need_check = True
    chat_download_config.total_task = node.total_task
    node.is_running = True


async def download_all_chat(client: pyrogram.Client):

    """Download All chat"""
    for key, value in app.chat_download_config.items():
        value.node = TaskNode(chat_id=key)
        try:
            await download_chat_task(client, value, value.node)

        except Exception as e:
            logger.warning(f"Download {key} error: {e}")
        finally:
            value.need_check = True


async def run_until_all_task_finish():
    """Normal download"""
    while True:
        finish: bool = True
        for _, value in app.chat_download_config.items():
            if not value.need_check or value.total_task != value.finish_task:
                finish = False

        if (not app.bot_token and finish) or app.restart_program:
            break

        await asyncio.sleep(1)


def _exec_loop():
    """Exec loop"""

    app.loop.run_until_complete(run_until_all_task_finish())


async def start_server(client: pyrogram.Client):
    """
    Start the server using the provided client.
    """
    await client.start()


async def stop_server(client: pyrogram.Client):
    """
    Stop the server using the provided client.
    """
    await client.stop()


def main():
    """Main function of the downloader."""
    tasks = []
    client = HookClient(
        "media_downloader",
        api_id=app.api_id,
        api_hash=app.api_hash,
        proxy=app.proxy,
        workdir=app.session_file_path,
        start_timeout=app.start_timeout,
    )
    try:
        app.pre_run()
        init_web(app)

        set_max_concurrent_transmissions(client, app.max_concurrent_transmissions)

        app.loop.run_until_complete(start_server(client))
        logger.success(_t("Successfully started (Press Ctrl+C to stop)"))

        app.loop.create_task(download_all_chat(client))

        for _ in range(app.max_download_task):
            task = app.loop.create_task(worker(client))
            tasks.append(task)

        if app.bot_token:
            app.loop.run_until_complete(
                start_download_bot(app, client, add_download_task, download_chat_task)
            )
        _exec_loop()
    except KeyboardInterrupt:
        logger.info(_t("KeyboardInterrupt"))
    except Exception as e:
        logger.exception("{}", e)
    finally:
        app.is_running = False
        if app.bot_token:
            app.loop.run_until_complete(stop_download_bot())
        app.loop.run_until_complete(stop_server(client))
        for task in tasks:
            task.cancel()
        logger.info(_t("Stopped!"))
        check_for_updates()
        logger.info(f"{_t('update config')}......")
        app.update_config()
        logger.success(
             f"{_t('Updated last read message_id to config file')},"
             f"{_t('total download')} {app.total_download_task}, "
             f"{_t('total upload file')} "
             f"{app.cloud_drive_config.total_upload_success_file_count}"
        )


if __name__ == "__main__":
    if _check_config():
        main()
