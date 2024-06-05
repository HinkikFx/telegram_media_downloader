"""util format"""

import math
import os
import re
import shutil
import unicodedata
from dataclasses import dataclass
from datetime import datetime
from typing import Optional, Union
import string
import zhon.hanzi  # 导入中文标点符号集合
import difflib
import mimetypes
from opencc import OpenCC
import json

@dataclass
class Link:
    """Telegram Link"""

    group_id: Union[str, int, None] = None
    post_id: Optional[int] = None
    comment_id: Optional[int] = None

waste_word_json_file = os.path.join(os.path.abspath("."), "waste_word_json_file.json")


def load_waste_word_json(json_file):
    if os.path.exists(json_file):
        # Read existing data from the file
        with open(json_file, 'r', encoding='utf-8') as f:
            waste_words = json.load(f)
        return waste_words


def format_byte(size: float, dot=2):
    """format byte"""

    # pylint: disable = R0912
    if 0 <= size < 1:
        human_size = str(round(size / 0.125, dot)) + "b"
    elif 1 <= size < 1024:
        human_size = str(round(size, dot)) + "B"
    elif math.pow(1024, 1) <= size < math.pow(1024, 2):
        human_size = str(round(size / math.pow(1024, 1), dot)) + "KB"
    elif math.pow(1024, 2) <= size < math.pow(1024, 3):
        human_size = str(round(size / math.pow(1024, 2), dot)) + "MB"
    elif math.pow(1024, 3) <= size < math.pow(1024, 4):
        human_size = str(round(size / math.pow(1024, 3), dot)) + "GB"
    elif math.pow(1024, 4) <= size < math.pow(1024, 5):
        human_size = str(round(size / math.pow(1024, 4), dot)) + "TB"
    elif math.pow(1024, 5) <= size < math.pow(1024, 6):
        human_size = str(round(size / math.pow(1024, 5), dot)) + "PB"
    elif math.pow(1024, 6) <= size < math.pow(1024, 7):
        human_size = str(round(size / math.pow(1024, 6), dot)) + "EB"
    elif math.pow(1024, 7) <= size < math.pow(1024, 8):
        human_size = str(round(size / math.pow(1024, 7), dot)) + "ZB"
    elif math.pow(1024, 8) <= size < math.pow(1024, 9):
        human_size = str(round(size / math.pow(1024, 8), dot)) + "YB"
    elif math.pow(1024, 9) <= size < math.pow(1024, 10):
        human_size = str(round(size / math.pow(1024, 9), dot)) + "BB"
    elif math.pow(1024, 10) <= size < math.pow(1024, 11):
        human_size = str(round(size / math.pow(1024, 10), dot)) + "NB"
    elif math.pow(1024, 11) <= size < math.pow(1024, 12):
        human_size = str(round(size / math.pow(1024, 11), dot)) + "DB"
    elif math.pow(1024, 12) <= size:
        human_size = str(round(size / math.pow(1024, 12), dot)) + "CB"
    else:
        raise ValueError(
            f'format_byte() takes number than or equal to 0, " \
            " but less than 0 given. {size}'
        )
    return human_size


class SearchDateTimeResult:
    """search result for datetime"""

    def __init__(
        self,
        value: str = "",
        right_str: str = "",
        left_str: str = "",
        match: bool = False,
    ):
        self.value = value
        self.right_str = right_str
        self.left_str = left_str
        self.match = match


def get_date_time(text: str, fmt: str) -> SearchDateTimeResult:
    """Get first of date time,and split two part

    Parameters
    ----------
    text: str
        ready to search text

    Returns
    -------
    SearchDateTimeResult

    """
    res = SearchDateTimeResult()
    search_text = re.sub(r"\s+", " ", text)
    regex_list = [
        # 2013.8.15 22:46:21
        r"\d{4}[-/\.]{1}\d{1,2}[-/\.]{1}\d{1,2}[ ]{1,}\d{1,2}:\d{1,2}:\d{1,2}",
        # "2013.8.15 22:46"
        r"\d{4}[-/\.]{1}\d{1,2}[-/\.]{1}\d{1,2}[ ]{1,}\d{1,2}:\d{1,2}",
        # "2014.5.11"
        r"\d{4}[-/\.]{1}\d{1,2}[-/\.]{1}\d{1,2}",
        # "2014.5"
        r"\d{4}[-/\.]{1}\d{1,2}",
    ]

    format_list = [
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%Y-%m-%d",
        "%Y-%m",
    ]

    for i, value in enumerate(regex_list):
        search_res = re.search(value, search_text)
        if search_res:
            time_str = search_res.group(0)
            try:
                res.value = datetime.strptime(
                    time_str.replace("/", "-").replace(".", "-").strip(), format_list[i]
                ).strftime(fmt)
            except Exception:
                break
            if search_res.start() != 0:
                res.left_str = search_text[0 : search_res.start()]
            if search_res.end() + 1 <= len(search_text):
                res.right_str = search_text[search_res.end() :]
            res.match = True
            return res

    return res





def clean_filename(filename :str):
    # 去除以[]括起来的内容
    a = re.sub(r'\[.*?\]', '', filename)

    # 去除英文标点符号
    a = re.sub('[{}]'.format(string.punctuation), ' ', a)

    # 去除中文标点符号
    a = re.sub('[{}]'.format(zhon.hanzi.punctuation), ' ', a)

    pattern = re.compile(r'[\u4e00-\u9fff]')  # 匹配中文字符的正则表达式范围
    if bool(pattern.search(a)):
        # 去掉开头的非汉字字符
        a = re.sub(r'^[^\u4e00-\u9fa5]+', '', a)
    else:
        # 去掉开头的非英文字符
        a = re.sub(r'^[^a-zA-Z]+', '', a)
    # 去掉所有标点符号和特殊字符
    r_str = r"[\/\\\:\*\?\"\<\>#\.\|\n\s/\:*?\"<>\|_ ~，、。？！@#￥%……&*（）+：；《》]+~【】"
    b = re.sub(r_str, " ", a)
    return b

def string_similar(s1, s2):
    if s1 == '' or s2 == '':
        return 0
    s1 = clean_filename(s1)
    s2 = clean_filename(s2)
    similar = difflib.SequenceMatcher(None, s1, s2).quick_ratio()
    return similar

def replace_date_time(text: str, fmt: str = "%Y-%m-%d %H:%M:%S") -> str:
    """Replace text all datetime to the right fmt

    Parameters
    ----------
    text: str
        ready to search text

    fmt: str
        the right datetime format

    Returns
    -------
    str
        The right format datetime str

    """

    if not text:
        return text
    res_str = ""
    res = get_date_time(text, fmt)
    if not res.match:
        return text
    if res.left_str:
        res_str += replace_date_time(res.left_str)
    res_str += res.value
    if res.right_str:
        res_str += replace_date_time(res.right_str)

    return res_str


_BYTE_UNIT = ["B", "KB", "MB", "GB", "TB"]


def get_byte_from_str(byte_str: str) -> Optional[int]:
    """Get byte from str

    Parameters
    ----------
    byte_str: str
        Include byte str

    Returns
    -------
    int
        Byte
    """
    search_res = re.match(r"(\d{1,})(B|KB|MB|GB|TB)", byte_str)
    if search_res:
        unit_str = search_res.group(2)
        unit: int = 1
        for it in _BYTE_UNIT:
            if it == unit_str:
                break
            unit *= 1024

        return int(search_res.group(1)) * unit

    return None


def truncate_filename(path: str, limit: int = 230) -> str:
    """Truncate filename to the max len.

    Parameters
    ----------
    path: str
        File name path

    limit: int
        limit file name len(utf-8 byte)

    Returns
    -------
    str
        if file name len more than limit then return truncate filename or return filename

    """
    p, f = os.path.split(os.path.normpath(path))
    f, e = os.path.splitext(f)
    f_max = limit - len(e.encode("utf-8"))
    f = unicodedata.normalize("NFC", f)
    f_trunc = f.encode()[:f_max].decode("utf-8", errors="ignore")
    return os.path.join(p, f_trunc + e)


def extract_info_from_link(link) -> Link:
    """Extract info from link"""
    if link in ("me", "self"):
        return Link(group_id=link)

    channel_match = re.match(
        r"(?:https?://)?t\.me/c/(?P<channel_id>\w+)"
        r"(?:.*/(?P<post_id>\d+)|/(?P<message_id>\d+))?",
        link,
    )

    if channel_match:
        channel_id = channel_match.group("channel_id")
        post_id = channel_match.group("post_id")
        message_id = channel_match.group("message_id")

        return Link(
            group_id=int(f"-100{channel_id}"),
            post_id=int(post_id) if post_id else None,
            comment_id=int(message_id) if message_id else None,
        )

    username_match = re.match(
        r"(?:https?://)?t\.me/(?P<username>\w+)"
        r"(?:.*comment=(?P<comment_id>\d+)"
        r"|.*/(?P<post_id>\d+)"
        r"|/(?P<message_id>\d+))?",
        link,
    )

    if username_match:
        username = username_match.group("username")
        comment_id = username_match.group("comment_id")
        post_id = username_match.group("post_id")
        message_id = username_match.group("message_id")

        return Link(
            group_id=username,
            post_id=int(post_id) if post_id else None,
            comment_id=int(comment_id) if comment_id else None,
        )

    return Link()


def validate_title(title: str) -> str:
    """Fix if title validation fails

    Parameters
    ----------
    title: str
        Chat title

    """

    r_str = r"[/\\:*?\"<>|\n]"  # '/ \ : * ? " < > |'
    new_title = re.sub(r_str, " ", title)
    return new_title

def validate_title_clean(title: str) -> str:
    """Fix if title validation fails

    Parameters
    ----------
    title: str
        Chat title

    """

    r_str = r"[\/\\\:\*\?\"\<\>#\.\|\n\s/\:*?\"<>\|_ ~，、。？！@#￥%……&*（）——+：；《》]+~【】"
    b = re.sub(r_str, " ", title)

    # 去除英文标点符号
    a = re.sub('[{}]'.format(string.punctuation), ' ', b)

    # 去除中文标点符号
    b = re.sub('[{}]'.format(zhon.hanzi.punctuation), ' ', b)
    return b


def create_progress_bar(progress, total_bars=10):
    """
    example
    progress = 50
    progress_bar = create_progress_bar(progress)
    print(f'Progress: [{progress_bar}] ({progress}%)')
    """
    completed_bars = int(progress * total_bars / 100)
    remaining_bars = total_bars - completed_bars
    progress_bar = "█" * completed_bars + "░" * remaining_bars
    return progress_bar

def remove_substrings_with_regex(input_str, keyword_list):
    # 遍历 keyword_list 中的每个元素
    for element in keyword_list:
        # 使用 re.sub() 方法将找到的子字符串替换为空字符串
        input_str = re.sub(element, " ", input_str).replace('  ', ' ')
    return input_str

def process_string(string_a: str):
    a = string_a
    # 发现文件名中有时包含两次后缀，处理掉
    if string_a.lower().endswith('mp3'):
        a = re.sub("mp3", "", a, flags=re.IGNORECASE)
    elif string_a.lower().endswith('mp4'):
        a = re.sub("mp4", "", a, flags=re.IGNORECASE)

    waste_word = load_waste_word_json(waste_word_json_file)
    a = remove_substrings_with_regex(a, waste_word)
    pattern = re.compile(r'[\u4e00-\u9fff]')  # 匹配中文字符的正则表达式范围
    if bool(pattern.search(a)):
        # 去掉开头的非汉字字符
        a = re.sub(r'^[^\u4e00-\u9fa5]+', '', a)
    else:
        # 去掉开头的非英文字符
        a = re.sub(r'^[^a-zA-Z]+', '', a)

    pattern_copy = re.compile(r"([a-zA-Z0-9\u4e00-\u9fa5].+)(\([0-9]+\)+)+")
    match = pattern_copy.match(a)
    last_part = ''
    if match:
        a, last_part = match.groups()
    a = a + ' ' +  last_part

    # 去除英文标点符号
    a = re.sub('[{}]'.format(string.punctuation), ' ', a)

    # 去除中文标点符号
    a = re.sub('[{}]'.format(zhon.hanzi.punctuation), ' ', a)

    # 去除其他
    r_str = r"\/\\\:\*\?\"\<\>#\.\|\n/\:*?\"<>\|_ ~，、。？！@#￥%……&*（）—+：；《》+~【】"
    a = re.sub(r_str, " ", a).replace('  ', ' ').strip()

    return a


def t2s(string_a):
    converter = OpenCC('t2s')
    return converter.convert(string_a)


def string_similar(s1, s2):
    if s1 == '' or s2 == '':
        return 0
    s1a = process_string(s1).lower().replace(' ','')
    s2a = process_string(s2).lower().replace(' ','')
    if s1a == '' or s2a == '':
        return 0
    if s1a == s2a:
        return 1
    similara = difflib.SequenceMatcher(None, s1a, s2a).quick_ratio()

    return similara


def split_string(input_string):
    # 去除空格
    cleaned_string = input_string.replace(" ", "").replace("　", "")

    # 使用正则表达式匹配字母或中文和数字的格式
    # punctuation_all = string.punctuation + zhon.hanzi.punctuation
    pattern = re.compile(r"([a-zA-Z\u4e00-\u9fa5]+)([\d（([【《<].+)")
    match = pattern.match(cleaned_string)

    if match:
        aaa_part, num_part = match.groups()
        return aaa_part, num_part
    else:
        return None, None

def string_sequence (s1, s2):
    s1 = process_string(s1).lower()
    s2 = process_string(s2).lower()
    if s1 == '' or s2 == '' or s1 == s2:
        return False
    s1a,s1b = split_string(s1)
    s2a,s2b = split_string(s2)
    if s1a and s1b and s2a and s2b and s1a == s2a and s1b != s2b:
        return True
    diff = difflib.Differ().compare(s1, s2)
    if not diff or diff == None:
        return False
    for line in diff:
        if line.startswith('+') or line.startswith('-'):
            if not line[2:].isnumeric() and line[2:] not in ['上', '中', '下']:
                return False
            else:
                pass
    return True

def move_file(source_directory, destination_directory, filename):
    try:
        # 创建目录c（如果不存在）
        os.makedirs(destination_directory, exist_ok=True)

        # 源文件的完整路径
        source_file_path = os.path.join(source_directory, filename)

        # 目标文件的完整路径
        destination_file_path = os.path.join(destination_directory, filename)

        # 移动文件
        shutil.move(source_file_path, destination_file_path)
        # print(f"Moved {source_file_path} to {destination_file_path}")

        # print("File moved successfully!")
    except Exception as e:
        print(f"Error moving file: {e}")

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


def is_exist_files_with_prefix(directory, prefix):
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
                return True
    return False

def guess_media_type(file_name_ext: str):

    # video_names = ['avi', 'mpg', 'mpeg', 'mov', 'mp4', 'mkv', 'ts', 'wmv', 'rmvb', 'vob', 'm1v', 'm2v', 'mpv']
    # audio_names = ['avi', 'mpg', 'mpeg', 'mov', 'mp4', 'mkv', 'ts', 'wmv', 'rmvb', 'vob', 'm1v', 'm2v', 'mpv']
    #
    #
    # media_type = 'default'
    docu_names = ["mobi", "azw3", "epub", "dox", "txt", "docx", "pdf", "chm", "rar", "7z", "zip"]
    if file_name_ext.startswith('.'):
        file_name_ext = file_name_ext[1:]

    if file_name_ext in docu_names:
        return "document"

    if '.' in file_name_ext:
        mime_type, _ = mimetypes.guess_type(f"aaaa{file_name_ext}")
    else:
        mime_type, _ = mimetypes.guess_type(f"aaaa.{file_name_ext}")
    if mime_type:
        if mime_type.startswith("video"):
            media_type =  "video"
        elif mime_type.startswith("audio"):
            media_type = "audio"
        elif mime_type.startswith("text"):
            media_type = "document"
        elif mime_type.startswith("image"):
            media_type = "photo"
        else:
            return "default"
    else:
        return "default"
    return media_type

def find_missing_files(folder_path, max_number):
    missing_ranges = []
    max_file_number = max_number -1
    if not os.path.exists(folder_path):
        missing_ranges = [(0,max_file_number)]
        return missing_ranges
    # Create a set to store existing file names
    all_files = set()

    # Iterate through the range of file names
    for i in range(max_number):
        all_files.add(f"{i:08d}")

    # Read the existing file names from the folder
    if os.path.exists(folder_path):
        existing_files = set(os.listdir(folder_path))
        missing_files = all_files - existing_files
    else:
        return None

    # Group consecutive missing file names
    if missing_files is None or len(missing_files) == 0:  # 没有缺失文件
        return None
    else:
        start_range = None
        max_range = int(max(missing_files))
        for file_name in sorted(missing_files):
            cursor_idx = int(file_name)
            if start_range is None:  # 第一个元素
                start_range = cursor_idx
                if cursor_idx == max_range:  # 是最后一个元素
                    missing_ranges.append((start_range, prev_one))
                prev_one = cursor_idx
            elif cursor_idx != prev_one + 1:  # 不连续了
                missing_ranges.append((start_range, prev_one))
                start_range = cursor_idx
                if cursor_idx == max_range:  # 是最后一个元素
                    missing_ranges.append((start_range, cursor_idx))
                prev_one = cursor_idx
            else:  # 连续
                if cursor_idx == max_range:  # 是最后一个元素
                    missing_ranges.append((start_range, cursor_idx))
                prev_one = cursor_idx

        return missing_ranges

def merge_files_cat(folder_path, output_file ):
    os.system(f"cat '{folder_path}'/* > {output_file}")

def merge_files_shutil(folder_path, output_file ):
    # 获取文件夹中的所有文件
    file_list = os.listdir(folder_path)
    file_list.sort()  # 确保文件按照一致的顺序合并
    if os.path.exists(output_file):
        os.remove(output_file)
    with open(output_file, 'ab') as output_file:
        for file_name in file_list:
            file_path = os.path.join(folder_path, file_name)
            with open(file_path, 'rb') as source:
                shutil.copyfileobj(source, output_file)

def merge_files_write(folder_path, output_file, batch_size=1000):
    # 获取文件夹中的所有文件
    files = os.listdir(folder_path)
    files.sort()  # 确保文件按照一致的顺序合并

    if os.path.exists(output_file):
        os.remove(output_file)

    with open(output_file, 'ab') as output_file:
        for file_name in files:
            file_path = os.path.join(folder_path, file_name)
            with open(file_path, 'rb') as source:
                output_file.write(source.read())


def get_folder_files_size(folder_path):
    #files_size = []
    total_size = 0
    files_count = 0
    for dirpath, dirnames, filenames in os.walk(folder_path):
        for filename in filenames:
            file_path = os.path.join(dirpath, filename)
            file_size = os.path.getsize(file_path)
            #files_size.append((filename,file_size))
            total_size += file_size
            files_count += 1
    return files_count, total_size


