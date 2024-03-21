"""Downloads media from telegram."""
import asyncio
import logging
import os
import shutil
import time
import re
from typing import List, Optional

import pyrogram
from loguru import logger
#from pyrogram.types import Audio, Document, Photo, Video, VideoNote, Voice
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
from utils.format import truncate_filename, validate_title
from utils.log import LogFilter
from utils.meta import print_meta
from utils.meta_data import MetaData
#from utils.updates import check_for_updates

import sqlmodel

logging.basicConfig(
    level=logging.WARNING,
    format="%(message)s",
    datefmt="[%X]",
    handlers=[RichHandler()],
)

CONFIG_NAME = "config.yaml"
DATA_FILE_NAME = "data.yaml"
APPLICATION_NAME = "media_downloader"
app = Application(CONFIG_NAME, DATA_FILE_NAME, APPLICATION_NAME)

downloadedDB = sqlmodel.Downloaded()

queue_maxsize = 0

queue: asyncio.Queue = asyncio.Queue(maxsize = queue_maxsize)
RETRY_TIME_OUT = 3

similar_set = 0.92

logging.getLogger("pyrogram.session.session").addFilter(LogFilter())
logging.getLogger("pyrogram.client").addFilter(LogFilter())

logging.getLogger("pyrogram").setLevel(logging.WARNING)





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
    if media_size == download_size:
        logger.success(f"{_t('Successfully downloaded')} - {ui_file_name} | 剩余下载队列长度:{queue.qsize()} \n")
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

def update_db(msg_dict: dict, status: int = 1):

    try:
        if (downloadedDB.is_exist_by_ids(msg_dict.get('chat_id'), msg_dict.get('message_id'), status=2)
                or downloadedDB.is_exist_by_ids(msg_dict.get('chat_id'), msg_dict.get('message_id'),
                                                status=3) or downloadedDB.is_exist_by_ids(msg_dict.get('chat_id'),
                                                                                          msg_dict.get('message_id'),
                                                                                          status=1)):
        # 数据库内记录未完成或被标记跳过。现更新为完成状态
            downloadedDB.update(status = status).where(downloadedDB.chat_id == msg_dict.get('chat_id'),
                                                downloadedDB.message_id == msg_dict.get('message_id')).execute()
            logger.info(
                f"[{msg_dict.get('chat_username')}]id={msg_dict.get('message_id')}"
                f"{_t('已在数据库中，但之前未完成或被标记跳过。现更新为完成状态')}.\n",
                exc_info=True,
            )
        else:
            downloadedDB.addto_localDB(msg_dict, status = status)
            logger.info(
                f"[{msg_dict.get('chat_username')}]id={msg_dict.get('message_id')} "
                f"{_t('already download,but not in db. insert into db')}.\n",
                exc_info=True,
            )
    except Exception as e:
        logger.error(
            f"[{e}].",
            exc_info=True,
        )


def _is_exist_in_db(msg_dict: dict) -> bool:
    if downloadedDB.is_exist_by_ids(msg_dict.get('chat_id'), msg_dict.get('message_id')):
        return True
    if downloadedDB.exist_filename_similar(msg_dict.get('mime_type'), msg_dict.get('media_size'),
                                           msg_dict.get('filename'), msg_dict.get('title')) > similar_set:
        return True
    return False


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
        #处理message原始数据
        if message.forward_from_chat and message.forward_from_chat.id and message.forward_from_message_id:
            msg_real_chat_id = 0 - message.forward_from_chat.id - 1000000000000
            msg_real_chat_username = message.forward_from_chat.username
            msg_real_message_id = message.forward_from_message_id
            msg_real_chat_title = validate_title(message.forward_from_chat.title)
        else:
            msg_real_chat_id = 0 - message.chat.id - 1000000000000
            msg_real_chat_username = message.chat.username
            msg_real_message_id = message.id
            msg_real_chat_title = validate_title(message.chat.title)

        #debug
        if msg_real_chat_id == 2146536963 and msg_real_message_id is None:
            print (message)

        if message.date:
            msg_time = message.date.strftime("%Y-%m-%d %H:%M")
            msg_date = message.date.strftime(app.date_format)

        msg_caption = getattr(message, "caption", '')
        #暂时无用 msg_link = getattr(message, "link", None)
        msg_media_group_id = getattr(message, "media_group_id", None)
        #msg_title = getattr(message, "title", '')
        if msg_caption:
            msg_caption = validate_title(msg_caption)
            app.set_caption_name(msg_real_message_id, msg_media_group_id, msg_caption)
        else:
            msg_caption = app.get_caption_name(msg_real_message_id, msg_media_group_id)

        if message.audio and message.audio != '':
            msg_type = 'audio'
            msg_title = validate_title(message.audio.title)
            if msg_caption != '' and not msg_title:
                msg_title = msg_caption
            if msg_caption != '' and ('telegram' in message.audio.file_name.lower() or re.sub(r'[._\-\s]', '',
                                                                                              os.path.splitext(
                                                                                                      message.audio.file_name)[
                                                                                                  0]).isdigit()):
                msg_filename = app.get_file_name(msg_real_message_id,
                                                 f"{msg_caption}{os.path.splitext(message.audio.file_name)[-1]}",
                                                 msg_caption)
            else:
                msg_filename = app.get_file_name(msg_real_message_id, message.audio.file_name , msg_caption)
            if msg_filename != '':
                msg_file_format = message.audio.file_name.split(".")[-1]
            else:
                msg_file_format = 'unknown'
            msg_duration = message.audio.duration
            msg_size = message.audio.file_size
            media_obj = message.audio
        elif message.video and message.video != '':
            msg_type = 'video'
            if msg_caption != '':
                msg_title = msg_caption
            if msg_caption != '' and ('telegram' in message.video.file_name.lower() or re.sub(r'[._\-\s]', '',
                                                                                              os.path.splitext(
                                                                                                      message.video.file_name)[
                                                                                                  0]).isdigit()):
                msg_filename = app.get_file_name(msg_real_message_id,
                                                 f"{msg_caption}{os.path.splitext(message.video.file_name)[-1]}",
                                                 msg_caption)
            else:
                msg_filename = app.get_file_name(msg_real_message_id, message.video.file_name, msg_caption)
            if msg_filename != '':
                msg_file_format = message.video.file_name.split(".")[-1]
            else:
                msg_file_format = 'unknown'
            msg_duration = message.video.duration
            msg_size = message.video.file_size
            #暂时无用 msg_width = message.video.width
            #暂时无用 mag_heigth = message.video.height
            media_obj = message.video
        elif message.photo and message.photo != '':
            msg_type = 'photo'
            msg_title = ''
            if msg_caption != '' and not msg_title:
                msg_title = msg_caption
            if msg_caption != '':
                msg_filename = app.get_file_name(msg_real_message_id,
                                                 f"{msg_caption}.jpg",
                                                 msg_caption)
            else:
                msg_filename = app.get_file_name(msg_real_message_id, '', msg_caption)

            msg_file_format = 'jpg'
            msg_duration = 0
            msg_size = message.photo.file_size
            #暂时无用 msg_width = message.photo.width
            #暂时无用 mag_heigth = message.photo.height
            media_obj = message.photo
        elif message.document and message.document != '':
            msg_type = 'document'
            msg_filename = app.get_file_name(msg_real_message_id, message.document.file_name , msg_caption)
            if msg_filename != '':
                msg_file_format = message.document.file_name.split(".")[-1]
                msg_title = validate_title(message.document.file_name.split(".")[-2])
            else:
                msg_file_format = 'unknown'
                msg_title =''
            msg_duration = 0
            msg_size = message.document.file_size
            media_obj = message.document
        else:
            logger.info(
                f"无需处理的媒体类型: ",
                exc_info=True,
            )
            return None

        # 处理存储用message衍生信息
        dirname = validate_title(f"[{str(msg_real_chat_id)}]{msg_real_chat_username}")

        if msg_date:
            datetime_dir_name = msg_date
        else:
            datetime_dir_name = "0000-00"

        if msg_type in ["voice", "video_note"]:
            # pylint: disable = C0209
            # voice video_note 暂不处理
            logger.error(
                f"[voice video_note 暂不处理]: ",
                exc_info=True,
            )
        else:
            if not msg_filename:
                msg_file_format = get_extension(
                    media_obj.file_id, getattr(media_obj, "mime_type", "")
                ).replace('.','')
            elif not msg_file_format:
                msg_file_format = get_extension(
                    media_obj.file_id, getattr(media_obj, "mime_type", "")
                )

            if not msg_filename:
               msg_filename = f"{app.get_file_name(msg_real_message_id, msg_filename, msg_caption)}.{msg_file_format}"

            file_save_path = os.path.join(app.get_file_save_path(msg_type, dirname, datetime_dir_name),
                                          str(int(msg_real_message_id) // 100 * 100).zfill(6))
            temp_save_path = os.path.join(app.temp_save_path, dirname,
                                          str(int(msg_real_message_id) // 100 * 100).zfill(6))

            file_save_url = os.path.join(file_save_path, truncate_filename(msg_filename))
            temp_save_url = os.path.join(temp_save_path, truncate_filename(msg_filename))

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
                'file_fullname':file_save_url,
                'temp_file_fullname':temp_save_url,
                'file_format':msg_type
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
    """Add Download task"""
    if message.empty:
        return False
    node.download_status[message.id] = DownloadStatus.Downloading
    await queue.put((message, node))
    msg_dict= await _get_media_meta(message)
    if downloadedDB.addto_localDB(msg_dict, status = 2): #status = 2 代表加入列表 但尚未完成
        logger.info(
            f"[{msg_dict.get('chat_id')}]{msg_dict.get('chat_username')}/{msg_dict.get('filename')}进入下载队列 .\n"
            f"[队列长度{queue.qsize()}]",
            exc_info=True,
        )
    else:
        logger.info(
            f"[{msg_dict.get('chat_id')}]{msg_dict.get('chat_username')}/{msg_dict.get('filename')}进入下载队列 .\n"
            f"[队列长度{queue.qsize()}]",
            exc_info=True,
        )
    node.total_task += 1
    return True


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

    file_name: str = ""
    ui_file_name: str = ""
    temp_file_name: str = ""
    task_start_time: float = time.time()
    media_dict = {}
    media_size = 0
    _media = None
    message = await fetch_message(client, message)
    message_id = message.id
    try:
        media_dict = await _get_media_meta(message)
        message_id = media_dict.get('message_id')
        _media = media_dict
        file_name = media_dict.get('file_fullname')
        temp_file_name = media_dict.get('temp_file_fullname')
        file_format = media_dict.get('mime_type')
        media_size = media_dict.get('media_size')
        _type = media_dict.get('file_format')

        ui_file_name = file_name.split('/')[-1]
        if app.hide_file_name:
            ui_file_name = f"****{os.path.splitext(file_name.split('/')[-1])[-1]}"

        if _can_download(_type, file_formats, file_format):
            if _is_exist_in_db(media_dict):
                # logger.info(
                #     f"[{media_dict.get('chat_username')}]id={message_id} {ui_file_name} "
                #     f"{_t('already download,download skipped')}.\n",
                #     exc_info=True,
                # )
                return DownloadStatus.SkipDownload, None
            if _is_exist(file_name):
                file_size = os.path.getsize(file_name)
                if file_size or file_size == media_size:
                    update_db(media_dict,status = 1)
                    return DownloadStatus.SkipDownload, None
        else:
            return DownloadStatus.SkipDownload, None

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
                # 增加将已下载文件信息写入数据库内记录功能
                if downloadedDB.is_exist_by_ids(media_dict.get('chat_id') , media_dict.get('message_id') , status = 1):
                    # 数据库已有 跳过写库
                    logger.info(
                        f"[{media_dict.get('chat_username')}]id={message_id} {ui_file_name} "
                        f"{_t('已在数据库中.')}.\n",
                        exc_info=True,
                    )
                elif (downloadedDB.is_exist_by_ids(media_dict.get('chat_id') , media_dict.get('message_id') , status = 2)
                      or downloadedDB.is_exist_by_ids(media_dict.get('chat_id') , media_dict.get('message_id') , status = 3)):
                    #update it
                    # update it
                    try:
                        downloadedDB.update(status=1).where(downloadedDB.chat_id == media_dict.get('chat_id'),
                                                            downloadedDB.message_id == media_dict.get('message_id')).execute()
                    except Exception as e:
                        logger.error(
                            f"[{e}].",
                            exc_info=True,
                        )
                    logger.info(
                        f"[{media_dict.get('chat_username')}]id={message_id} {ui_file_name} "
                        f"{_t('已在数据库中，但之前未完成或被标记跳过。现更新为完成状态')}.\n",
                        exc_info=True,
                    )
                else:
                    downloadedDB.addto_localDB(media_dict)
                    logger.info(
                        f"[{media_dict.get('chat_username')}]id={message_id} {ui_file_name} "
                        f"{_t('写入数据库.')}.\n",
                        exc_info=True,
                    )
                return DownloadStatus.SuccessDownload, file_name
        except pyrogram.errors.exceptions.bad_request_400.BadRequest:
            logger.warning(
                f"Message[{message_id}]: {_t('file reference expired, refetching')}..."
            )
            await asyncio.sleep(RETRY_TIME_OUT)
            message = await fetch_message(client, message)
            if _check_timeout(retry, message_id):
                # pylint: disable = C0301
                logger.error(
                    f"Message[{message_id}]: "
                    f"{_t('file reference expired for 3 retries, download skipped.')}"
                )
        except pyrogram.errors.exceptions.flood_420.FloodWait as wait_err:
            await asyncio.sleep(wait_err.value)
            logger.warning("Message[{}]: FlowWait {}", message_id, wait_err.value)
            _check_timeout(retry, message_id)
        except TypeError:
            # pylint: disable = C0301
            logger.warning(
                f"{_t('Timeout Error occurred when downloading Message')}[{message_id}], "
                f"{_t('retrying after')} {RETRY_TIME_OUT} {_t('seconds')}"
            )
            await asyncio.sleep(RETRY_TIME_OUT)
            if _check_timeout(retry, message_id):
                logger.error(
                    f"Message[{message_id}]: {_t('Timing out after 3 reties, download skipped.')}"
                )
        except Exception as e:
            # pylint: disable = C0301
            logger.error(
                f"Message[{message_id}]: "
                f"{_t('could not be downloaded due to following exception')}:\n[{e}].",
                exc_info=True,
            )
            break

    return DownloadStatus.FailedDownload, None


def _load_config():
    """Load config"""
    app.load_config()



def to_continue_all_msgs():
    msgs = []
    try:
        to_continue_chats = downloadedDB.search_to_continue_chats(status = 2)
        for chatit in to_continue_chats:
            chat_id = chatit.chat_id
            chat_username = chatit.chat_username
            msg_ids = list(downloadedDB.search_to_continue_mgs_by_chatid(chat_id, 2))
            if chat_username and chat_username != '':
                dictit = {
                    'chat_id': chat_username,
                    'ids_to_retry': msg_ids
                }
            else:
                dictit = {
                    'chat_id': chat_id,
                    'ids_to_retry': msg_ids
                }
            msgs.append(dictit)
    except Exception as e:
        logger.exception(f"load to continue error: {e}")
        return False

    return msgs


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

async def download_retry_chat_task(
    client: pyrogram.Client,
    chat_download_config: ChatDownloadConfig,
    node: TaskNode,
    message_id
):
    """Download retry task"""
    maxid = message_id
    messages_iter = get_chat_history_v2(
        client,
        node.chat_id,
        limit=1,
        max_id=maxid,
        offset_id=message_id,
        reverse=True,
    )

    chat_download_config.node = node

    async for message in messages_iter:  # type: ignore
        meta_data = MetaData()

        caption = message.caption
        if caption:
            caption = validate_title(caption)
            app.set_caption_name(node.chat_id, message.media_group_id, caption)
        else:
            caption = app.get_caption_name(node.chat_id, message.media_group_id)

        set_meta_data(meta_data, message, caption)

        if not app.exec_filter(chat_download_config, meta_data):
            node.download_status[message.id] = DownloadStatus.SkipDownload
            # logger.info(
            #     f"[{node.chat_id}]:{message.id}不符合过滤器要求，跳过加入队列 .\n",
            #     exc_info=True,
            # )
            await upload_telegram_chat(
                client,
                node.upload_user,
                app,
                node,
                message,
                DownloadStatus.SkipDownload,
            )
            continue

        #取消从跳过文件列表中跳过文件的方式 改为从数据库读取要跳过的id
        msg_dict = await _get_media_meta(message)
        if downloadedDB.is_exist_by_ids(msg_dict.get('chat_id'), msg_dict.get('message_id'), status = 3): #status = 3 代表无需下载
            # logger.info(
            #     f"[chat_id={message.chat.id} id={message.id} 在跳过列表中，跳过加入队列",
            #     exc_info=True,
            # )
            node.download_status[message.id] = DownloadStatus.SkipDownload
            await upload_telegram_chat(
                client,
                node.upload_user,
                app,
                node,
                message,
                DownloadStatus.SkipDownload,
            )
            continue
        #if app.need_skip_message(chat_download_config, message.id):
        #    continue

        if _is_exist_in_db(msg_dict):
            node.download_status[message.id] = DownloadStatus.SkipDownload
            # logger.info(
            #     f"[chat_id={message.chat.id} id={message.id} 在库中，跳过加入队列",
            #     exc_info=True,
            # )
            await upload_telegram_chat(
                client,
                node.upload_user,
                app,
                node,
                message,
                DownloadStatus.SkipDownload,
            )
            continue
        file_name = msg_dict.get('file_fullname')
        if _is_exist(file_name):
            file_size = os.path.getsize(file_name)
            if file_size or file_size == msg_dict.get('media_size'):
                update_db(msg_dict, status = 1)
                continue

        await add_download_task(message, node)

    chat_download_config.need_check = True
    chat_download_config.total_task = node.total_task
    node.is_running = True
    logger.info(
        f"Retry CHATID=[{node.chat_id}]message_id= {message_id}.\n",
        exc_info=True,
    )

async def download_chat_task(
    client: pyrogram.Client,
    chat_download_config: ChatDownloadConfig,
    node: TaskNode,
):
    """Download all task"""
    maxid = node.end_offset_id
    messages_iter = get_chat_history_v2(
        client,
        node.chat_id,
        limit=node.limit,
        max_id=maxid,
        offset_id=chat_download_config.last_read_message_id,
        reverse=True,
    )
    logger.info(
        f"开始将CHATID=[{node.chat_id}] 放入队列.\n",
        exc_info=True,
    )

    chat_download_config.node = node

    async for message in messages_iter:  # type: ignore
        meta_data = MetaData()

        caption = message.caption
        if caption:
            caption = validate_title(caption)
            app.set_caption_name(node.chat_id, message.media_group_id, caption)
        else:
            caption = app.get_caption_name(node.chat_id, message.media_group_id)

        set_meta_data(meta_data, message, caption)

        if not app.exec_filter(chat_download_config, meta_data):
            node.download_status[message.id] = DownloadStatus.SkipDownload
            # logger.info(
            #     f"[{node.chat_id}]:{message.id}不符合过滤器要求，跳过加入队列 .\n",
            #     exc_info=True,
            # )
            await upload_telegram_chat(
                client,
                node.upload_user,
                app,
                node,
                message,
                DownloadStatus.SkipDownload,
            )
            continue

        #取消从跳过文件列表中跳过文件的方式 改为从数据库读取要跳过的id
        msg_dict = await _get_media_meta(message)
        if downloadedDB.is_exist_by_ids(msg_dict.get('chat_id'), msg_dict.get('message_id'), status = 3): #status = 3 代表无需下载
            # logger.info(
            #     f"[chat_id={message.chat.id} id={message.id} 在跳过列表中，跳过加入队列",
            #     exc_info=True,
            # )
            node.download_status[message.id] = DownloadStatus.SkipDownload
            await upload_telegram_chat(
                client,
                node.upload_user,
                app,
                node,
                message,
                DownloadStatus.SkipDownload,
            )
            continue
        #if app.need_skip_message(chat_download_config, message.id):
        #    continue

        # if _is_exist_in_db(msg_dict, status = 1):
        #     node.download_status[message.id] = DownloadStatus.SkipDownload
        #     logger.info(
        #         f"[chat_id={message.chat.id} id={message.id} 在库中，跳过加入队列",
        #         exc_info=True,
        #     )
        #     await upload_telegram_chat(
        #         client,
        #         node.upload_user,
        #         app,
        #         node,
        #         message,
        #         DownloadStatus.SkipDownload,
        #     )
        #     continue
        file_name = msg_dict.get('file_fullname')
        if _is_exist(file_name):
            file_size = os.path.getsize(file_name)
            if file_size or file_size == msg_dict.get('media_size'):
                update_db(msg_dict, status = 1) #文件存在 更新或插入数据库
                continue
        await add_download_task(message, node)

    chat_download_config.need_check = True
    chat_download_config.total_task = node.total_task
    node.is_running = True
    if isinstance(node.chat_id, int) or node.chat_id.isnumeric():
        logger.info(
                f"将CHATID=[{0 - int(node.chat_id) - 1000000000000}] 放入队列完毕.\n",
            exc_info=True,
        )
    else:
        logger.info(
            f"将CHATID=[{node.chat_id}] 放入队列完毕.\n",
            exc_info=True,
        )


async def download_all_chat(client: pyrogram.Client):

    """Download All chat"""
    for key, value in app.chat_download_config.items():
        value.node = TaskNode(chat_id=key)
        if key == '~~continue~~':
            to_continue_msgs = downloadedDB.search_to_continue_msgs_all()
            if to_continue_msgs:
                for msg in to_continue_msgs:
                    value.node.chat_id = msg.chat_username
                    # ids_to_retry = []
                    # for ids in ids_to_retry:
                    #     value.ids_to_retry_dict[ids] = True
                    try:
                        await download_retry_chat_task(client, value, value.node, msg.message_id)
                    except Exception as e:
                        logger.warning(f"Download {key} error: {e}")
                    finally:
                        value.need_check = True
            else:
                continue
        else:
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
        #check_for_updates(app.proxy)
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
