#!/usr/bin/env python3
"""
Windows API 知识图谱统一流水线 v4.0
═══════════════════════════════════════════════════════════════

将 extract_entities_v3.py 与 llm_refine_graph.py 整合为单一工作流：

  Phase-0  发现文件 → 双源配对（.p.txt + .txt）
  Phase-1  正则提取（双趟扫描 × 双源）
  Phase-2  质量评估 → 自动选优 → 不确定时 LLM 裁决
  Phase-3  构建初始图谱 + LLM 精炼（异步 + 进度条）
  Phase-4  输出精炼图谱 + 报告

特性：
  ✓ 140 个 OCR 文件双源配对，每对只保留质量更高的那份
  ✓ 质量评分启发式打分 + LLM 裁决双机制
  ✓ 正则提取全部 v3.0 能力（结构化子字段/OCR纠错/置信度/类型推断/断点续跑）
  ✓ LLM 校验/优化（异步/速率控制/增删改查节点边/进度条）
  ✓ 统一断点管理，全流程可续跑
  ✓ tqdm 全局进度条

Usage:
  python pipeline.py                            # 完整流水线
  python pipeline.py --phase extract            # 仅正则提取
  python pipeline.py --phase refine             # 仅 LLM 精炼（需要已有提取结果）
  python pipeline.py --max-entities 20          # LLM 最多处理 20 实体
  python pipeline.py --dry-run                  # 预览，不调 LLM
  python pipeline.py --provider ollama          # 使用本地 Ollama
  python pipeline.py --resume                   # 断点续跑
  python pipeline.py --force                    # 忽略缓存重跑
"""

import os, sys, re, json, hashlib, time, asyncio, glob, logging, argparse
from collections import OrderedDict, defaultdict
from pathlib import Path
from datetime import datetime, timezone
from typing import Dict, List, Optional, Set, Tuple
from dataclasses import dataclass

import aiohttp
from tqdm import tqdm

# ════════════════════════════════════════════════════════════════
#  全局配置
# ════════════════════════════════════════════════════════════════
WORKSPACE       = os.path.dirname(os.path.abspath(__file__))
OUTPUT_DIR      = os.path.join(WORKSPACE, "json_output_v4")
CHECKPOINT_FILE = os.path.join(OUTPUT_DIR, "_checkpoint.json")
LLM_CKPT_FILE  = os.path.join(OUTPUT_DIR, "_llm_checkpoint.json")
OPS_LOG_FILE    = os.path.join(OUTPUT_DIR, "_llm_operations.jsonl")
LLM_CONFIG_FILE = os.path.join(WORKSPACE, "llm_config.json")
MIN_DESC_LENGTH = 8
SCHEMA_VERSION  = "windows_api_kg_v4.0"

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("pipeline")

# ═══════════════════════════════════════════════════════════════
#  Part A — 正则提取引擎（源自 extract_entities_v3.py）
# ═══════════════════════════════════════════════════════════════

# ── OCR 字符级纠错 ─────────────────────────────────────────
OCR_CHAR_FIXES = [
    (re.compile(r'\bUL0NG\b'),  'ULONG'),  (re.compile(r'\bB00L\b'),  'BOOL'),
    (re.compile(r'\bDW0RD\b'),  'DWORD'),   (re.compile(r'\bHANDlE\b'),'HANDLE'),
    (re.compile(r'\bNUlL\b'),   'NULL'),    (re.compile(r'\bHRESUlT\b'),'HRESULT'),
    (re.compile(r'\bLPV0ID\b'), 'LPVOID'),  (re.compile(r'\bW0RD\b'),  'WORD'),
    (re.compile(r'\bl0CTL\b'),  'IOCTL'),
]
def ocr_fix_entity_name(name: str) -> str:
    for pat, repl in OCR_CHAR_FIXES:
        name = pat.sub(repl, name)
    if re.match(r'^[A-Z][A-Z0-9]+[-\s][A-Z][A-Z0-9]+', name):
        name = name.replace('-', '_').replace(' ', '_')
    return name

# ── 噪声 ──────────────────────────────────────────────────
NOISE_EXACT = {
    'ﾉ','展开表','告知我们有关下载 PDF 体验的信息。','反馈','此页面是否有帮助？',
    'ﾂ是','ﾄ否','在 Microsoft Q&A 获取帮助','| 在 Microsoft Q&A 获取帮助',
    '提供产品反馈','提供产品反馈 ','Tell us about your PDF experience.','ﾉ ﾉ','ﾉﾉ',
}
NOISE_RE = [re.compile(r'^≦\s*\d+\s*≧\s*$'), re.compile(r'^\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}\s*$')]
DESC_NOISE_PATTERNS = [
    (re.compile(r'ﾉ+'),''), (re.compile(r'展开表'),''),
    (re.compile(r'告知我们有关下载 PDF 体验的信息。'),''),
    (re.compile(r'Tell us about your PDF experience\.'),''),
    (re.compile(r'此页面是否有帮助？'),''),
    (re.compile(r'在 Microsoft Q&A 获取帮助'),''),
    (re.compile(r'提供产品反馈\s*'),''), (re.compile(r'ﾂ是'),''), (re.compile(r'ﾄ否'),''),
]
DESC_TRUNCATE_PATTERNS = [
    re.compile(r'要求\s+要求\s+最低支持.*$'), re.compile(r'要求\s+最低支持.*$'),
    re.compile(r'要求\s+最低受支持.*$'), re.compile(r'要求\s+标头\s+.*$'),
    re.compile(r'另请参阅\s*.*$'), re.compile(r'\s+是\s+否\s*$'),
    re.compile(r'主要代码.*$'), re.compile(r'语法\s+C\+\+.*$'),
    re.compile(r'成员\s+Signature.*$'),
    re.compile(r'[。\.]\s*注解\s*$'), re.compile(r'[。\.]\s*备注\s*$'), re.compile(r'[。\.]\s*言论\s*$'),
]
NOISE_TOKENS = ['ﾉ','展开表','告知我们有关下载','Tell us about','ﾂ是','ﾄ否']

# ── 内部段落标记 ───────────────────────────────────────────
INNER_SECTIONS = OrderedDict([
    ('语法 C++','syntax'),('语法','syntax'),('参数','parameters'),('返回值','return_value'),
    ('备注','remarks'),('注解','remarks'),('言论','remarks'),
    ('成员','members'),('要求','requirements'),('另请参阅','see_also'),
])
INNER_SECTION_TAIL_RE = re.compile(
    r'[。\.\!）\)]\s*(注解|备注|言论|语法 C\+\+|语法|参数|返回值|成员|要求|另请参阅)\s*$')

def detect_inner_section(line):
    s = line.strip()
    for marker, key in INNER_SECTIONS.items():
        if s == marker: return key, ''
        if s.startswith(marker) and len(s) > len(marker):
            rest = s[len(marker):].strip()
            if rest and (rest[0] in '[（(：:' or rest[0].isupper() or rest[0] in '类返无没'):
                return key, rest
    return None, None

def detect_tail_section(line):
    s = line.strip()
    m = INNER_SECTION_TAIL_RE.search(s)
    if m:
        for mk, key in INNER_SECTIONS.items():
            if m.group(1) == mk or m.group(1).startswith(mk):
                return s[:m.start()+1].strip(), key
    return None, None

# ── 文档级章节 ─────────────────────────────────────────────
SECTION_MAP = OrderedDict([
    ('IOCTL','ioctl'),('IOCTLs','ioctl'),('Kernel-Mode IOCTL','ioctl'),
    ('User-Mode 应用程序和服务发送的 IOCTL','ioctl'),
    ('枚举','enum'),('函数','function'),('结构','struct'),('回调','callback'),
    ('回调函数','callback'),('接口','interface'),('方法','method'),('宏','macro'),
    ('常量','constant'),('联合','union'),('属性','property'),('类','class'),
    ('委托','delegate'),('事件','event'),
])

# ── CamelCase 黑名单 ──────────────────────────────────────
CAMEL_BLACKLIST = {
    'About','Above','Across','Actually','Add','Added','Adhere','After',
    'All','Also','Although','Always','An','And','Another','Any',
    'Application','Are','Article','As','At','Available',
    'Because','Been','Before','Below','Benefits','Between','Both','But','By',
    'Call','Called','Can','Change','Changed','Check','Code','Commands',
    'Common','Componentized','Configuration','Contains','Content','Could',
    'Create','Created','Current','Custom',
    'Data','Declarative','Default','Define','Defined','Describes','Description',
    'Details','Device','Devices','Did','Different','Direct','Does','Don',
    'Driver','Drivers','During',
    'Each','Either','Else','Enable','Enabled','End','Enhanced','Enhancement',
    'Error','Event','Events','Every','Example','Existing',
    'Feedback','Field','File','Files','Filter','Find','First','Follow',
    'Following','For','Form','From','Full','Function','Functions','Future',
    'General','Get','Given','Group',
    'Handle','Handler','Has','Have','Header','Here','How','However',
    'Identifies','If','Important','In','Include','Information','Input',
    'Install','Instead','Interface','Into','Invalid','Is','It','Its',
    'Just','Kernel','Key','Kind',
    'Large','Last','Level','Library','Like','List',
    'Make','Makes','May','Member','Members','Method','Methods','Microsoft',
    'Mode','Model','Module','More','Most','Must',
    'Name','Need','New','Next','No','None','Not','Note',
    'Object','Of','Off','On','Only','Open','Optional','Or','Other',
    'Otherdrivers','Our','Out','Output','Over',
    'Pack','Page','Parameter','Parameters','Part','Path','Platform','Please',
    'Pointer','Port','Possible','Power','Practices','Prior','Process',
    'Processes','Protecteddata','Provide','Provides',
    'Query','Queue',
    'Read','Remarks','Remove','Request','Required','Requirements','Reserved',
    'Resource','Response','Result','Return','Returns','Run','Running',
    'Same','Sample','Section','See','Send','Service','Services','Set',
    'Several','Should','Show','Similar','Since','Size','So','Some',
    'Specific','Specifies','Start','State','Status','Stop','String',
    'Structure','Such','Support','Supported','Syntax','System',
    'Table','Takes','Target','Task','Tell','Test','Than','That','The',
    'Their','Then','There','These','They','This','Through','Time','To',
    'Type','Types',
    'Under','Unicode','Unless','Until','Up','Update','Upon','Use','Used',
    'User','Using',
    'Valid','Value','Values','Version','Via','View',
    'Was','Way','What','When','Where','Which','While','Will','Windows',
    'With','Within','Without','Would','Write',
    'You','Your',
    'Hardware','Software','Libraries','Internet','Bluetooth','Bidi',
    'Devmode','Midl','Wndows','Tracelog','Usbdlib','Usbdex','VidPN',
}
WIN_TYPE_WHITELIST = {
    'DWORD','HANDLE','LPVOID','ULONG','NTSTATUS','HRESULT','BOOL','BOOLEAN',
    'BYTE','CHAR','WCHAR','SHORT','USHORT','LONG','FLOAT','DOUBLE','PVOID',
    'VOID','UINT','WORD','LPSTR','LPCSTR','LPWSTR','LPCWSTR','BSTR','VARIANT',
    'LRESULT','WPARAM','LPARAM','HWND','HMODULE','HINSTANCE','HBITMAP','HBRUSH',
    'HCURSOR','HDC','HFONT','HICON','HMENU','HRGN','HPEN','HKEY','HMONITOR',
    'GUID','REFGUID','REFIID','REFCLSID','SIZE','RECT','POINT',
}

# ── 正则 ──────────────────────────────────────────────────
RE_ALLCAPS      = re.compile(r'^([A-Z][A-Z0-9]*(?:_[A-Z0-9]+)+)(?![a-z])')
MIN_ALLCAPS_LEN = 6
RE_CAMEL        = re.compile(r'^([A-Z][a-z][a-zA-Z0-9]*)(?=\s|$|[\u4e00-\u9fff])')
RE_UNDERSC      = re.compile(r'^(_[a-zA-Z][a-zA-Z0-9]+)(?=\s|$|[\u4e00-\u9fff])')
RE_FUNC_CALL    = re.compile(r'^([A-Za-z_][A-Za-z0-9_]+)\s*\(')
RE_HEADER_SECTION = re.compile(r'^(\w[\w\-]*\.h)\s+标头')
RE_HEADER_NAME  = re.compile(r'\b([\w\-]+\.h)\b')
RE_TRAILING_ENTITY = re.compile(r'[。\.]\s+([A-Z][A-Z0-9]*(?:_[A-Z0-9]+)+)\s*$')
RE_META_LINE    = re.compile(r'^(IOCTL|枚举|函数|结构|回调|接口|方法|宏)\s*（[\w\.]+）项目\d{4}/\d{2}/\d{2}')
RE_HEADER_META  = re.compile(r'^[\w\.]+\s+标头项目\d{4}/\d{2}/\d{2}')
RE_ARTICLE_META = re.compile(r'^.{0,30}项目\s*\d{4}/\d{2}/\d{2}')
RE_PARAM_DIR    = re.compile(r'^\[([^\]]+)\]\s*(\w+)')
RE_PARAM_TYPE   = re.compile(r'^类型[：:]\s*(.+)$')
RE_RETVAL_TYPE  = re.compile(r'^类型[：:]\s*(.+)$')
RE_REQ_CLIENT   = re.compile(r'(?:最低受?支持的客户端|最低支持的客户端)[：:\s]*(.+)', re.I)
RE_REQ_SERVER   = re.compile(r'(?:最低受?支持的服务器|最低支持的服务器|支持的最低服务器)[：:\s]*(.+)', re.I)
RE_REQ_TARGET   = re.compile(r'目标平台[：:\s]*(.+)', re.I)
RE_WIN_VERSION  = re.compile(
    r'(Windows\s+(?:Vista|XP|7|8|8\.1|10|11|Server\s+\d{4}(?:\s+R2)?)(?:\s*\[.+?\])?'
    r'|在\s+Windows\s+\d+\s+.*?(?:可用|中可用)'
    r'|Windows\s+\d+,?\s+版本\s+\d+)', re.I)
RE_PLATFORM_VAL = re.compile(r'(通用|Universal|Windows|Desktop)', re.I)

# ── 类型推断规则 ──────────────────────────────────────────
TYPE_INFER_RULES = [
    (re.compile(r'^IOCTL_'),'ioctl'),   (re.compile(r'^FSCTL_'),'ioctl'),
    (re.compile(r'^GUID_'),'constant'), (re.compile(r'^CLSID_'),'constant'),
    (re.compile(r'^IID_'),'constant'),  (re.compile(r'^DEVPKEY_'),'constant'),
    (re.compile(r'^PKEY_'),'constant'), (re.compile(r'^WM_'),'constant'),
    (re.compile(r'^WS_'),'constant'),   (re.compile(r'^ERROR_'),'constant'),
    (re.compile(r'^STATUS_'),'constant'),(re.compile(r'^FLAG_'),'constant'),
    (re.compile(r'^DXGI_'),'constant'), (re.compile(r'_FLAGS?$'),'enum'),
    (re.compile(r'^KSPROPERTY_'),'constant'),(re.compile(r'^SENSOR_'),'constant'),
    (re.compile(r'^EVT_'),'callback'),  (re.compile(r'^PFN_'),'callback'),
    (re.compile(r'_CALLBACK$'),'callback'),(re.compile(r'_ROUTINE$'),'callback'),
    (re.compile(r'_HANDLER$'),'callback'),(re.compile(r'Callback$'),'callback'),
    (re.compile(r'^_?[A-Z][a-z].*Ex$'),'function'),
    (re.compile(r'^_?[A-Z][a-z].*ExW$'),'function'),
    (re.compile(r'^_?[A-Z][a-z].*[AW]$'),'function'),
    (re.compile(r'^I[A-Z][a-z]'),'interface'),
]
def infer_entity_type(name, current_type):
    if current_type != 'unknown': return current_type
    for pat, inferred in TYPE_INFER_RULES:
        if pat.search(name): return inferred
    return current_type

# ── 置信度 ────────────────────────────────────────────────
def compute_confidence(entity):
    score = 0.0
    name = entity.get('name','')
    desc = entity.get('description','')
    etype = entity.get('entity_type','unknown')
    if re.match(r'^[A-Z][A-Z0-9]*(?:_[A-Z0-9]+)+$', name): score += 0.30
    elif name.startswith('_'): score += 0.20
    elif re.match(r'^[A-Z][a-z]', name): score += 0.15
    if len(desc) > 50: score += 0.20
    elif len(desc) > 20: score += 0.10
    if etype != 'unknown': score += 0.15
    if entity.get('header'): score += 0.10
    if entity.get('syntax'): score += 0.10
    if entity.get('parameters') or entity.get('members'): score += 0.10
    if entity.get('return_value'): score += 0.05
    return round(min(score, 1.0), 2)

# ── 工具函数 ──────────────────────────────────────────────
def is_noise(line):
    s = line.strip()
    if not s: return True
    if s in NOISE_EXACT: return True
    return any(p.match(s) for p in NOISE_RE)

def is_meta_line(line):
    s = line.strip()
    return bool(RE_META_LINE.match(s) or RE_HEADER_META.match(s))

def detect_section(line):
    s = line.strip().rstrip(' ')
    for marker, stype in SECTION_MAP.items():
        if s == marker: return stype
    return None

def detect_header(line):
    m = RE_HEADER_SECTION.match(line.strip())
    return m.group(1) if m else None

def is_api_reference_file(lines):
    for l in lines[:200]:
        if detect_section(l.strip()): return True
    return False

def try_entity_name(line, allow_camel):
    s = line.strip()
    m = RE_ALLCAPS.match(s)
    if m and len(m.group(1)) >= MIN_ALLCAPS_LEN:
        return ocr_fix_entity_name(m.group(1)), s[m.end():].strip()
    m = RE_UNDERSC.match(s)
    if m: return m.group(1), s[m.end():].strip()
    m = RE_FUNC_CALL.match(s)
    if m:
        n = m.group(1)
        if n not in CAMEL_BLACKLIST and len(n) >= 4 and not n[0].isdigit():
            return n, s[m.end():].strip()
    if allow_camel:
        m = RE_CAMEL.match(s)
        if m:
            n = m.group(1)
            if n not in CAMEL_BLACKLIST and len(n) >= 4:
                return n, s[m.end():].strip()
    return None

def clean_text(text):
    for pat, repl in DESC_NOISE_PATTERNS: text = pat.sub(repl, text)
    for pat in DESC_TRUNCATE_PATTERNS:    text = pat.sub('', text)
    m = RE_TRAILING_ENTITY.search(text)
    if m: text = text[:m.start()+1].strip()
    text = re.sub(r'\s+', ' ', text).strip().lstrip('- ：:')
    return re.sub(r'\s+是\s+否\s*$', '', text)

def content_hash(text): return hashlib.sha256(text.encode()).hexdigest()[:12]
def file_hash(filepath):
    h = hashlib.md5()
    with open(filepath,'rb') as f:
        for c in iter(lambda: f.read(8192), b''): h.update(c)
    return h.hexdigest()

# ── 文件元数据 ────────────────────────────────────────────
def parse_filename(filename):
    m = re.match(r'\[OCR\]_windows-(.+?)_(\d{4})(\d{2})(\d{2})_(\d{2})(\d{2})', filename)
    if not m: return {'domain':'','topic':'','ocr_time':''}
    raw_path = m.group(1)
    y,mo,d,h,mi = m.group(2),m.group(3),m.group(4),m.group(5),m.group(6)
    parts = raw_path.split('-_')
    if len(parts)==2: domain,topic = parts
    else:
        seg = raw_path.rsplit('-',1)
        domain,topic = (seg[0],seg[1]) if len(seg)==2 else (raw_path,'')
    return {'domain':domain,'topic':topic,'ocr_time':f"{y}-{mo}-{d}T{h}:{mi}:00"}

def extract_header_list(lines):
    headers, in_zone = [], False
    for l in lines[:60]:
        if '需要以下标头' in l or '以下标头' in l:
            in_zone = True; headers.extend(RE_HEADER_NAME.findall(l)); continue
        if in_zone:
            found = RE_HEADER_NAME.findall(l)
            if found: headers.extend(found)
            elif l.strip(): in_zone = False
    return sorted(set(headers))

def extract_title(lines):
    for l in lines[:20]:
        s = l.strip()
        if s and not is_noise(l) and not s.startswith('[OCR]'):
            s = re.sub(r'项目\s*\d{4}/\d{2}/\d{2}', '', s).strip()
            if s: return s
    return ''

# ── 子段落解析器 ──────────────────────────────────────────
def parse_parameters(lines):
    params, current = [], None
    for line in lines:
        s = line.strip()
        if not s or is_noise(line): continue
        m = RE_PARAM_DIR.match(s)
        if m:
            if current:
                current['description'] = clean_text(' '.join(current['_dp']))
                del current['_dp']; params.append(current)
            current = {'name':m.group(2),'direction':m.group(1).strip(),'type':None,'description':'','_dp':[]}
            rest = s[m.end():].strip()
            if rest: current['_dp'].append(rest)
            continue
        if (not current or not current['_dp']) and re.match(r'^[a-zA-Z_]\w*$',s) and len(s)<40:
            if current:
                current['description'] = clean_text(' '.join(current['_dp']))
                del current['_dp']; params.append(current)
            current = {'name':s,'direction':None,'type':None,'description':'','_dp':[]}
            continue
        if current:
            tm = RE_PARAM_TYPE.match(s)
            if tm and not current['type']: current['type']=tm.group(1).strip(); continue
            current['_dp'].append(s)
    if current:
        current['description'] = clean_text(' '.join(current['_dp']))
        del current['_dp']; params.append(current)
    for p in params:
        if not p['direction']: del p['direction']
        if not p['type']: del p['type']
    return params

def parse_members(lines):
    members, current = [], None
    for line in lines:
        s = line.strip()
        if not s or is_noise(line): continue
        if re.match(r'^[a-zA-Z_]\w*(\[\d*\])?\s*$',s) and len(s)<60:
            if current:
                current['description'] = clean_text(' '.join(current['_dp']))
                del current['_dp']; members.append(current)
            current = {'name':s.strip(),'type':None,'description':'','_dp':[]}
            continue
        if current:
            tm = RE_PARAM_TYPE.match(s)
            if tm and not current['type']: current['type']=tm.group(1).strip(); continue
            current['_dp'].append(s)
    if current:
        current['description'] = clean_text(' '.join(current['_dp']))
        del current['_dp']; members.append(current)
    for m in members:
        if not m['type']: del m['type']
    return members

def parse_requirements(lines):
    req = {}
    clean_lines = [l.strip() for l in lines if l.strip() and not is_noise(l)]
    win_versions = []
    for s in clean_lines:
        if not req.get('header'):
            hm = RE_HEADER_NAME.findall(s)
            for h in hm:
                if h != 'Header.h': req['header'] = h; break
        if not req.get('library') and '.lib' in s.lower():
            libs = re.findall(r'[\w]+\.lib', s, re.I)
            if libs: req['library'] = libs[0]
        if not req.get('dll') and '.dll' in s.lower():
            dlls = re.findall(r'[\w]+\.dll', s, re.I)
            if dlls: req['dll'] = dlls[0]
        vm = RE_WIN_VERSION.findall(s)
        for v in vm:
            v = v.strip()
            if v and len(v)>5: win_versions.append(v)
        if not req.get('target_platform'):
            pm = RE_PLATFORM_VAL.search(s)
            if pm and '最低' not in s and '受支持' not in s:
                req['target_platform'] = pm.group(1)
    if win_versions:
        for v in win_versions:
            if 'Server' in v or '服务器' in v:
                if not req.get('min_server'): req['min_server'] = v
            else:
                if not req.get('min_client'): req['min_client'] = v
    return req

_RV_SECTION_RE = re.compile(r'(?<!\u53c2\u9605|\u53c2\u89c1|\u67e5\u770b|\u8be6\u89c1)([\u3002\\.!])\s*(\u6ce8\u89e3|\u5907\u6ce8|\u8a00\u8bba|\u8981\u6c42)\s')

def _truncate_rv_at_section(text):
    text = re.sub(r'(\s没有){3,}', '', text).strip()
    m = _RV_SECTION_RE.search(text)
    if m:
        prefix = text[max(0,m.start()-6):m.start()]
        if any(kw in prefix for kw in ['参阅','参见','查看','详见','详细']): return text
        return text[:m.start()+1].strip()
    for kw in ['要求','备注','注解','言论']:
        if text.rstrip().endswith(kw): text = text.rstrip()[:-len(kw)].rstrip()
        idx = text.find(' '+kw+' ')
        if idx>0 and idx>len(text)*0.7:
            before = text[max(0,idx-6):idx]
            if not any(r in before for r in ['参阅','参见','查看','详见','"']): text = text[:idx].strip()
    text = re.sub(r'[０-９\d]\s*(备注|注解|言论)\s+', '', text).strip()
    return text

def parse_return_value(lines):
    rv, parts = {}, []
    RV_STOP = re.compile(r'^(注解|备注|言论|要求|另请参阅)')
    for line in lines:
        s = line.strip()
        if not s or is_noise(line): continue
        if RV_STOP.match(s): break
        m_t = INNER_SECTION_TAIL_RE.search(s)
        if m_t:
            before = s[:m_t.start()+1].strip()
            if before: parts.append(before)
            break
        tm = RE_RETVAL_TYPE.match(s)
        if tm and 'type' not in rv: rv['type'] = tm.group(1).strip().rstrip('。.'); continue
        parts.append(s)
    desc = clean_text(' '.join(parts))
    for sw in ['注解','备注','言论']:
        for sep in ['。','.']: 
            idx = desc.find(sep+sw)
            if idx>=0: desc = desc[:idx+1].strip()
    if desc: rv['description'] = desc
    if not rv:
        full = clean_text(' '.join(l.strip() for l in lines if l.strip()))
        if full: rv['description'] = full
    if 'description' in rv:
        d = _truncate_rv_at_section(rv['description'])
        if d: rv['description'] = d
        else: del rv['description']
    return rv if rv else None

# ── 核心：单文件实体提取 ─────────────────────────────────
def extract_entities_from_file(filepath, global_names=None):
    filename = os.path.basename(filepath)
    with open(filepath,'r',encoding='utf-8') as f: raw_text = f.read()
    lines = raw_text.split('\n')
    file_meta = parse_filename(filename)
    title = extract_title(lines)
    header_list = extract_header_list(lines)
    allow_camel = is_api_reference_file(lines)
    current_section, current_header = 'unknown', None
    entities = OrderedDict()
    current_name, current_start_line = None, 0
    inner_section = 'description'
    inner_buckets = defaultdict(list)

    def _flush():
        nonlocal current_name, inner_section, inner_buckets, current_start_line
        if not current_name: return
        desc_parts = inner_buckets.get('description',[])
        cleaned = []
        for p in desc_parts:
            p = p.strip()
            if not p or is_meta_line(p) or RE_ARTICLE_META.match(p): continue
            if p.startswith(current_name): p = p[len(current_name):].strip()
            if p: cleaned.append(p)
        desc = clean_text(' '.join(cleaned))
        if entities:
            for known in entities:
                pos = desc.find(' '+known+' ')
                if pos<0: pos = desc.find(' '+known)
                if pos>0 and pos>len(desc)*0.6: desc = desc[:pos].strip(); break
        syntax_lines = inner_buckets.get('syntax',[])
        syntax_text = '\n'.join(l for l in syntax_lines if l.strip() and not is_noise(l))
        syntax_text = re.sub(r'ﾉ+','',syntax_text).strip() or None
        param_lines = inner_buckets.get('parameters',[])
        parameters = parse_parameters(param_lines) if param_lines else None
        rv_lines = inner_buckets.get('return_value',[])
        return_value = parse_return_value(rv_lines) if rv_lines else None
        remarks_lines = inner_buckets.get('remarks',[])
        remarks_text = clean_text(' '.join(l.strip() for l in remarks_lines if l.strip() and not is_noise(l))) or None
        member_lines = inner_buckets.get('members',[])
        members = parse_members(member_lines) if member_lines else None
        req_lines = inner_buckets.get('requirements',[])
        requirements = parse_requirements(req_lines) if req_lines else None
        sa_lines = inner_buckets.get('see_also',[])
        see_also_text = ' '.join(l.strip() for l in sa_lines if l.strip() and not is_noise(l))
        see_also = re.findall(r'[A-Za-z_][A-Za-z0-9_]+',see_also_text) if see_also_text else None
        etype = infer_entity_type(current_name, current_section)
        if current_name in entities:
            ex = entities[current_name]
            if len(desc)>len(ex.get('description','')): ex['description']=desc
            if ex.get('entity_type')=='unknown' and etype!='unknown': ex['entity_type']=etype
            if current_header and not ex.get('header'): ex['header']=current_header
            if syntax_text and (not ex.get('syntax') or len(syntax_text)>len(ex.get('syntax',''))): ex['syntax']=syntax_text
            if parameters and len(parameters)>len(ex.get('parameters') or []): ex['parameters']=parameters
            if return_value and not ex.get('return_value'): ex['return_value']=return_value
            if remarks_text and (not ex.get('remarks') or len(remarks_text)>len(ex.get('remarks',''))): ex['remarks']=remarks_text
            if members and len(members)>len(ex.get('members') or []): ex['members']=members
            if requirements:
                er = ex.get('requirements') or {}; er.update(requirements); ex['requirements']=er
        else:
            entities[current_name] = {
                'name':current_name,'entity_type':etype,'description':desc,
                'header':current_header,'source_line':current_start_line+1,
                'syntax':syntax_text,'parameters':parameters,'return_value':return_value,
                'remarks':remarks_text,'members':members,'requirements':requirements,'see_also':see_also,
            }
        current_name = None; inner_section = 'description'; inner_buckets = defaultdict(list)

    for i, raw_line in enumerate(lines):
        line = raw_line.rstrip()
        if is_noise(line): continue
        stripped = line.strip()
        sect = detect_section(stripped)
        if sect: _flush(); current_section = sect; continue
        hdr = detect_header(stripped)
        if hdr: _flush(); current_header = hdr; current_section = 'unknown'; continue
        if is_meta_line(stripped): continue
        if current_name:
            before_txt, tail_sect = detect_tail_section(stripped)
            if tail_sect:
                if before_txt: inner_buckets[inner_section].append(before_txt)
                inner_section = tail_sect; continue
            isect, irest = detect_inner_section(stripped)
            if isect:
                inner_section = isect
                if irest: inner_buckets[isect].append(irest)
                continue
        result = try_entity_name(stripped, allow_camel)
        if result:
            name, rest = result
            _flush(); current_name = name; current_start_line = i
            inner_section = 'description'; inner_buckets = defaultdict(list)
            trail = RE_TRAILING_ENTITY.search(rest); trailing_next = None
            if trail: trailing_next = trail.group(1); rest = rest[:trail.start()+1].strip()
            if rest.startswith(name): rest = rest[len(name):].strip()
            if rest: inner_buckets['description'].append(rest)
            if trailing_next:
                _flush(); current_name = trailing_next; current_start_line = i
                inner_section = 'description'; inner_buckets = defaultdict(list)
        else:
            if current_name: inner_buckets[inner_section].append(stripped)
    _flush()

    local_names = set(entities.keys())
    ref_names = (global_names | local_names) if global_names else local_names
    entity_list = []
    for name, info in entities.items():
        desc = info.get('description','')
        if len(desc)<MIN_DESC_LENGTH or RE_ARTICLE_META.match(desc): continue
        xref_src = desc
        if info.get('remarks'): xref_src += ' '+info['remarks']
        xrefs = set()
        for mr in re.finditer(r'\b([A-Z][A-Z0-9]*(?:_[A-Z0-9]+)+)\b',xref_src):
            r=mr.group(1)
            if r!=name and r in ref_names: xrefs.add(r)
        for mr in re.finditer(r'\b([A-Z][a-z][a-zA-Z0-9]{2,})\b',xref_src):
            r=mr.group(1)
            if r!=name and r in ref_names and r not in CAMEL_BLACKLIST: xrefs.add(r)
        if info.get('see_also'):
            for sa in info['see_also']:
                if sa in ref_names and sa!=name: xrefs.add(sa)
        is_depr = bool(re.search(r'已弃用|过时|请勿使用|deprecated',desc,re.I))
        eid = f"windows::{name}"
        entity = OrderedDict()
        entity['id']=eid; entity['name']=name; entity['entity_type']=info['entity_type']
        entity['description']=desc
        for k in ('syntax','parameters','return_value','remarks','members','requirements'):
            if info.get(k): entity[k]=info[k]
        entity['header']=info.get('header')
        entity['deprecated']=is_depr
        entity['cross_references']=sorted(xrefs)
        entity['confidence']=compute_confidence({**info,**entity})
        entity['_source_line']=info['source_line']
        entity_list.append(entity)
    doc_meta = OrderedDict([
        ('document_id',f"{file_meta['domain']}::{file_meta['topic']}"),
        ('title',title),('domain',file_meta['domain']),('topic',file_meta['topic']),
        ('source_file',filename),('ocr_timestamp',file_meta['ocr_time']),
        ('headers_referenced',header_list),('total_entities',len(entity_list)),
    ])
    return doc_meta, entity_list

def pass1_collect_names(filepath):
    names = set()
    with open(filepath,'r',encoding='utf-8') as f: raw_text = f.read()
    lines = raw_text.split('\n')
    allow_camel = is_api_reference_file(lines)
    for raw_line in lines:
        line = raw_line.rstrip()
        if is_noise(line): continue
        s = line.strip()
        if detect_section(s) or detect_header(s) or is_meta_line(s): continue
        r = try_entity_name(s, allow_camel)
        if r: names.add(r[0])
    return names

# ── 自检 ─────────────────────────────────────────────────
REQUIRED_FIELDS = {'id','name','entity_type','description','confidence'}
def self_test(result):
    issues = []
    entities = result.get('entities',[])
    if not entities: issues.append("WARN: 没有提取到任何实体"); return issues
    seen_ids, unknown_count = set(), 0
    valid_types = set(SECTION_MAP.values()) | {'unknown'}
    for e in entities:
        eid = e.get('id','')
        if eid in seen_ids: issues.append(f"ERROR: 重复 ID: {eid}")
        seen_ids.add(eid)
        for field in REQUIRED_FIELDS:
            if field not in e: issues.append(f"ERROR: 缺失字段 {field}: {e.get('name','?')}")
        desc = e.get('description','')
        if any(noise in desc for noise in NOISE_TOKENS): issues.append(f"ERROR: 描述含噪声: {e['name']}")
        if e.get('remarks') and any(noise in e['remarks'] for noise in NOISE_TOKENS): issues.append(f"ERROR: 备注含噪声: {e['name']}")
        etype = e.get('entity_type','unknown')
        if etype not in valid_types: issues.append(f"ERROR: 无效类型 '{etype}': {e['name']}")
        if etype == 'unknown': unknown_count += 1
        conf = e.get('confidence',0)
        if not (0<=conf<=1.0): issues.append(f"ERROR: 置信度越界 {conf}: {e['name']}")
    if entities and unknown_count/len(entities)>0.3:
        issues.append(f"WARN: unknown 类型占比 {unknown_count}/{len(entities)} ({unknown_count*100//len(entities)}%)")
    if unknown_count>0: issues.append(f"INFO: {unknown_count} 个实体类型为 unknown")
    return issues

def build_output(doc_meta, entity_list):
    content_text = json.dumps(entity_list, ensure_ascii=False)
    return OrderedDict([
        ('_schema',SCHEMA_VERSION),
        ('_generated_at',datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')),
        ('_content_hash',content_hash(content_text)),
        ('document',doc_meta), ('entities',entity_list),
    ])


# ═══════════════════════════════════════════════════════════════
#  Part B — 双源质量对比 + LLM 裁决
# ═══════════════════════════════════════════════════════════════

def score_entity_list(entity_list: list) -> dict:
    """对一份实体列表做质量打分（高精度版）"""
    if not entity_list:
        return {'total':0,'avg_conf':0,'typed_ratio':0,'field_coverage':0,
                'avg_desc_len':0,'noise_ratio':0,'syntax_ratio':0,'score':0}
    total = len(entity_list)
    avg_conf = sum(e.get('confidence',0) for e in entity_list) / total
    typed = sum(1 for e in entity_list if e.get('entity_type','unknown')!='unknown')
    typed_ratio = typed / total

    # 字段覆盖（加权）
    fields_weighted = 0
    for e in entity_list:
        if e.get('syntax'): fields_weighted += 2.0
        if e.get('parameters'): fields_weighted += 1.5
        if e.get('return_value'): fields_weighted += 1.0
        if e.get('remarks'): fields_weighted += 1.0
        if e.get('members'): fields_weighted += 1.5
        if e.get('requirements'): fields_weighted += 1.0
        if e.get('header'): fields_weighted += 0.5
    max_field_w = total * 8.5 if total else 1
    field_coverage = fields_weighted / max_field_w

    # 描述质量
    desc_lens = [len(e.get('description','')) for e in entity_list]
    avg_desc_len = sum(desc_lens) / total
    desc_quality = min(avg_desc_len / 100.0, 1.0)   # 平均100字满分

    # 噪声残留
    noise_count = 0
    for e in entity_list:
        d = e.get('description','')
        if any(n in d for n in NOISE_TOKENS): noise_count += 1
        if 'ﾉ' in d or '展开表' in d: noise_count += 1
    noise_ratio = noise_count / (total * 2) if total else 0
    noise_penalty = noise_ratio   # 越高越差

    # syntax / parameters 覆盖
    syntax_count = sum(1 for e in entity_list if e.get('syntax'))
    syntax_ratio = syntax_count / total

    # 综合评分（更细粒度）
    score = (
        0.10 * min(total / 20, 1.0)        # 实体数量
        + 0.20 * avg_conf                   # 平均置信度
        + 0.15 * typed_ratio                # 类型覆盖
        + 0.20 * field_coverage             # 字段覆盖（加权）
        + 0.15 * desc_quality               # 描述质量
        + 0.10 * syntax_ratio               # syntax 覆盖
        - 0.10 * noise_penalty              # 噪声惩罚
    )
    return {'total':total,'avg_conf':round(avg_conf,4),'typed_ratio':round(typed_ratio,4),
            'field_coverage':round(field_coverage,4),'avg_desc_len':round(avg_desc_len,1),
            'noise_ratio':round(noise_ratio,4),'syntax_ratio':round(syntax_ratio,4),
            'score':round(score,6)}

def build_file_pairs(workspace):
    """构建 .p.txt / .txt 文件对"""
    all_files = sorted(f for f in os.listdir(workspace) if f.startswith('[OCR]') and f.endswith('.txt'))
    p_files = {f[:-6]:f for f in all_files if f.endswith('.p.txt')}   # base→fname
    t_files = {f[:-4]:f for f in all_files if not f.endswith('.p.txt')}  # base→fname
    bases = sorted(set(p_files.keys()) | set(t_files.keys()))
    pairs = []
    for b in bases:
        pairs.append({
            'base': b,
            'p_file': p_files.get(b),
            't_file': t_files.get(b),
        })
    return pairs


# ═══════════════════════════════════════════════════════════════
#  Part C — LLM 客户端 + 精炼引擎（源自 llm_refine_graph.py）
# ═══════════════════════════════════════════════════════════════

@dataclass
class LLMProviderConfig:
    api_base_url: str; api_key: str; model_name: str
    batch_size: int = 10; max_workers: int = 4
    requests_per_min: int = 60; max_retries: int = 3
    retry_backoff_base: float = 0.5

def load_llm_config(provider='deepseek'):
    if not os.path.exists(LLM_CONFIG_FILE):
        log.warning(f"LLM 配置文件不存在: {LLM_CONFIG_FILE}，LLM 功能不可用")
        return None
    with open(LLM_CONFIG_FILE,'r',encoding='utf-8') as f: raw = json.load(f)
    if provider == 'ollama':
        o = raw.get('ollama',{})
        return LLMProviderConfig(
            api_base_url=o.get('api_base_url','http://localhost:11434')+'/v1',
            api_key='ollama', model_name=o.get('model_name','qwen3:1.7b'),
            batch_size=raw.get('batch_size',10),
            max_workers=min(raw.get('max_workers',4),2),
            requests_per_min=min(raw.get('requests_per_min',60),30),
            max_retries=raw.get('max_retries',3),
            retry_backoff_base=raw.get('retry_backoff_base',0.5))
    return LLMProviderConfig(
        api_base_url=raw['api_base_url'], api_key=raw['api_key'], model_name=raw['model_name'],
        batch_size=raw.get('batch_size',10), max_workers=raw.get('max_workers',4),
        requests_per_min=raw.get('requests_per_min',60), max_retries=raw.get('max_retries',3),
        retry_backoff_base=raw.get('retry_backoff_base',0.5))

class AsyncRateLimiter:
    def __init__(self, rpm):
        self.interval = 60.0/rpm; self._lock = asyncio.Lock(); self._last = 0.0
    async def acquire(self):
        async with self._lock:
            wait = self.interval - (time.monotonic()-self._last)
            if wait>0: await asyncio.sleep(wait)
            self._last = time.monotonic()

class LLMClient:
    def __init__(self, config, rate_limiter):
        self.config = config; self.rl = rate_limiter; self._session = None
        self.total_calls=0; self.tokens_in=0; self.tokens_out=0; self.errors=0
    async def __aenter__(self):
        self._session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=120,connect=30))
        return self
    async def __aexit__(self,*a):
        if self._session: await self._session.close()
    async def chat(self, messages, temperature=0.3):
        url = f"{self.config.api_base_url}/chat/completions"
        headers = {"Content-Type":"application/json","Authorization":f"Bearer {self.config.api_key}"}
        payload = {"model":self.config.model_name,"messages":messages,"temperature":temperature,"max_tokens":2048}
        for attempt in range(1, self.config.max_retries+1):
            await self.rl.acquire()
            try:
                async with self._session.post(url,headers=headers,json=payload) as resp:
                    if resp.status==200:
                        data = await resp.json(); self.total_calls += 1
                        u = data.get('usage',{}); self.tokens_in+=u.get('prompt_tokens',0); self.tokens_out+=u.get('completion_tokens',0)
                        choices = data.get('choices',[])
                        return choices[0].get('message',{}).get('content','') if choices else None
                    elif resp.status==429:
                        w=self.config.retry_backoff_base*(2**attempt)+1
                        log.warning(f"Rate limited, retry {attempt}, wait {w:.1f}s"); await asyncio.sleep(w)
                    elif resp.status>=500:
                        w=self.config.retry_backoff_base*(2**attempt)
                        log.warning(f"Server {resp.status}, retry {attempt}"); await asyncio.sleep(w)
                    else:
                        body=await resp.text(); log.error(f"API {resp.status}: {body[:200]}"); self.errors+=1; return None
            except (asyncio.TimeoutError, aiohttp.ClientError) as e:
                w=self.config.retry_backoff_base*(2**attempt)+2
                log.warning(f"{type(e).__name__}, retry {attempt}"); await asyncio.sleep(w)
        self.errors += 1; return None
    def usage_report(self):
        return f"LLM调用:{self.total_calls} | 输入token:{self.tokens_in:,} | 输出token:{self.tokens_out:,} | 错误:{self.errors}"

# ── 知识图谱数据结构 ─────────────────────────────────────
MAX_NEIGHBOR_DISPLAY = 15
MAX_DESC_IN_NEIGHBOR = 150
MAX_DESC_IN_ENTITY   = 800
MAX_SYNTAX_IN_ENTITY = 500

def _truncate(text, max_len):
    if not text: return ""
    text = text.replace("\n"," ").strip()
    return text if len(text)<=max_len else text[:max_len]+"..."

class KnowledgeGraph:
    def __init__(self):
        self.entities = {}          # entity_id → dict
        self.entity_names = {}      # entity_name → entity_id
        self.edges = []
        self.adj = defaultdict(set)
        self.edge_index = defaultdict(list)
        self.source_files = {}      # entity_id → file basename
    def _rebuild_adjacency(self):
        self.adj.clear(); self.edge_index.clear()
        for e in self.edges:
            si = self.entity_names.get(e.get('source',''),f"windows::{e.get('source','')}")
            ti = self.entity_names.get(e.get('target',''),f"windows::{e.get('target','')}")
            self.adj[si].add(ti); self.adj[ti].add(si)
            self.edge_index[si].append(e); self.edge_index[ti].append(e)
    def get_neighborhood(self, eid, max_n=MAX_NEIGHBOR_DISPLAY):
        nids = list(self.adj.get(eid,set()))
        existing = [n for n in nids if n in self.entities]
        external = [n for n in nids if n not in self.entities]
        selected = (existing+external)[:max_n]
        neighbors = []
        for nid in selected:
            if nid in self.entities:
                e = self.entities[nid]
                neighbors.append({'id':nid,'name':e.get('name',''),'entity_type':e.get('entity_type','unknown'),
                                  'description':_truncate(e.get('description',''),MAX_DESC_IN_NEIGHBOR)})
            else:
                nm = nid.replace('windows::','') if nid.startswith('windows::') else nid
                neighbors.append({'id':nid,'name':nm,'entity_type':'external_ref','description':'(未在图谱中)'})
        return neighbors, self.edge_index.get(eid,[])
    def stats(self):
        tc = defaultdict(int)
        for e in self.entities.values(): tc[e.get('entity_type','unknown')]+=1
        return {'total_entities':len(self.entities),'total_edges':len(self.edges),
                'entity_types':dict(tc),'avg_degree':round(2*len(self.edges)/max(len(self.entities),1),2)}
    def load_from_extraction(self, all_entity_map, all_edges, file_entity_lists):
        """直接从提取阶段的内存数据构建图谱（无需中间文件 I/O）"""
        for fname, entity_list in file_entity_lists.items():
            for ent in entity_list:
                eid = ent.get('id','')
                if not eid: continue
                self.entities[eid] = ent
                self.entity_names[ent.get('name','')] = eid
                self.source_files[eid] = fname
        self.edges = all_edges
        self._rebuild_adjacency()
    def load_from_dir(self, input_dir):
        entity_files = sorted(glob.glob(os.path.join(input_dir,"*.json")))
        entity_files = [f for f in entity_files if not os.path.basename(f).startswith("_") and "global" not in os.path.basename(f)]
        for fp in entity_files:
            fn = os.path.basename(fp)
            with open(fp,'r',encoding='utf-8') as f: doc = json.load(f)
            for ent in doc.get('entities',[]):
                eid = ent.get('id','')
                if not eid: continue
                self.entities[eid]=ent; self.entity_names[ent.get('name','')]=eid; self.source_files[eid]=fn
        ep = os.path.join(input_dir,'global_edges.json')
        if os.path.exists(ep):
            with open(ep,'r',encoding='utf-8') as f: self.edges = json.load(f).get('edges',[])
        self._rebuild_adjacency()

# ── LLM Prompt 定义 ──────────────────────────────────────
SYSTEM_PROMPT = """\
你是一位 Windows API 文档与知识图谱专家。
你将收到一个从 OCR 文档中自动提取的实体（节点），及其在知识图谱中的 1-hop 邻域。

你的任务：
1. **校验**：判断该实体是否有效（是否为 OCR 噪声、重复节点、或无意义数据）
2. **优化**：改善 description / entity_type / syntax / parameters / return_value / remarks 等字段质量
3. **审查边**：检查连接的边是否合理
4. **图操作**：给出需要执行的图修改操作

## 可用操作
| 操作 | 格式 | 说明 |
|------|------|------|
| update_field | `{"op":"update_field","field":"<字段名>","value":"<新值>"}` | 更新字段 |
| add_edge | `{"op":"add_edge","target":"<目标实体名>","edge_type":"<类型>"}` | 新增边 |
| delete_edge | `{"op":"delete_edge","target":"<目标实体名>","edge_type":"<类型>"}` | 删除边 |
| add_node | `{"op":"add_node","name":"<名称>","entity_type":"<类型>","description":"<描述>"}` | 新增节点 |
| delete_node | `{"op":"delete_node","reason":"<原因>"}` | 删除当前实体 |
| merge_into | `{"op":"merge_into","target_id":"<目标实体ID>","reason":"<原因>"}` | 合并到另一实体 |

## entity_type 合法值
function, structure, enum, callback, macro, constant, interface, ioctl, method, property, event, typedef, union, unknown

## 注意事项
- description 中可能含有 OCR 噪声（ﾉ、展开表、乱码等），请清理
- entity_type 为 "unknown" 的可根据名称模式推断
- 空字段不需填充虚构内容
- 实体基本正确则返回空 operations 数组
- 只做有把握的修改，不要臆测

**只返回一个 JSON 对象，不要 markdown 标记或其他文字：**
{"verdict":"keep|delete|merge","confidence":0.0~1.0,"summary":"一句话评估","operations":[...]}"""

COMPARE_PROMPT = """\
你是 Windows API 文档质量评估专家。下面是同一份 OCR 文档的两种 OCR 结果提取出的实体样本。
请判断哪份质量更高（A 还是 B），基于：实体名准确性、描述清晰度、字段完整度、噪声水平。

## A 版本（{a_source}）
实体数：{a_count}，平均置信度：{a_conf}
样本：
{a_samples}

## B 版本（{b_source}）
实体数：{b_count}，平均置信度：{b_conf}
样本：
{b_samples}

**只返回一个 JSON 对象：**
{{"winner":"A"|"B","confidence":0.0~1.0,"reason":"一句话理由"}}"""

def build_user_prompt(entity, neighbors, edges):
    parts = ["## 当前实体\n"]
    ev = {'id':entity.get('id',''),'name':entity.get('name',''),
          'entity_type':entity.get('entity_type','unknown'),
          'description':_truncate(entity.get('description',''),MAX_DESC_IN_ENTITY),
          'confidence':entity.get('confidence',0)}
    for k in ('syntax','parameters','return_value','remarks','members','requirements','header','cross_references'):
        v = entity.get(k)
        if v:
            if k=='syntax': ev[k]=_truncate(str(v),MAX_SYNTAX_IN_ENTITY)
            elif k=='parameters' and isinstance(v,list): ev[k]=v[:8]
            elif k in ('members','cross_references') and isinstance(v,list): ev[k]=v[:10]
            else: ev[k]=v
    parts.append("```json\n"+json.dumps(ev,ensure_ascii=False,indent=2)+"\n```\n")
    if edges:
        parts.append(f"\n## 连接的边（共 {len(edges)} 条）\n")
        seen = set()
        for e in edges[:25]:
            key=(e.get('source',''),e.get('target',''),e.get('type',''))
            if key in seen: continue
            seen.add(key); parts.append(f"- {e.get('source','')} --[{e.get('type','')}]--> {e.get('target','')}")
        parts.append("")
    if neighbors:
        parts.append(f"\n## 邻居实体（{len(neighbors)} 个）\n")
        for nb in neighbors:
            parts.append(f"- **{nb['name']}** ({nb['entity_type']}): {nb.get('description','') or '(无描述)'}")
        parts.append("")
    return "\n".join(parts)

def _extract_json_candidates(text: str):
    """从 LLM 文本中提取多个 JSON 候选（完整块、代码块、首个对象到末尾）。"""
    cands = []
    if not text:
        return cands
    t = text.strip().replace("\ufeff", "")
    cands.append(t)

    # markdown code blocks
    for m in re.finditer(r"```(?:json)?\s*\n?(.*?)\n?```", t, re.DOTALL | re.IGNORECASE):
        block = m.group(1).strip()
        if block:
            cands.append(block)

    # 从首个 { 到末尾，处理“前后有说明文本”的情况
    fb = t.find("{")
    if fb != -1:
        cands.append(t[fb:].strip())

    # 去重并保序
    out = []
    seen = set()
    for c in cands:
        if c and c not in seen:
            seen.add(c)
            out.append(c)
    return out

def _append_missing_closers(text: str) -> str:
    """为截断 JSON 自动补齐缺失的 ]/}，忽略字符串内部符号。"""
    stack = []
    in_str = False
    esc = False
    for ch in text:
        if in_str:
            if esc:
                esc = False
            elif ch == '\\':
                esc = True
            elif ch == '"':
                in_str = False
            continue
        if ch == '"':
            in_str = True
        elif ch in '{[':
            stack.append('}' if ch == '{' else ']')
        elif ch in '}]':
            if stack and ch == stack[-1]:
                stack.pop()
    if not stack:
        return text
    return text + ''.join(reversed(stack))

def _repair_json_text(text: str) -> str:
    """轻量修复常见 LLM JSON 漂移：尾逗号、未加引号 key、截断闭合。"""
    s = text.strip().replace("\ufeff", "")
    s = re.sub(r"[\x00-\x08\x0B\x0C\x0E-\x1F]", "", s)
    # 清掉 markdown 栅栏（即便候选里带着）
    s = re.sub(r"^```(?:json)?", "", s, flags=re.IGNORECASE).strip()
    s = re.sub(r"```$", "", s).strip()

    # 去掉结尾尾逗号: {"a":1,} / [1,2,]
    s = re.sub(r",\s*([}\]])", r"\1", s)

    # 把裸 key 修复为 "key":
    s = re.sub(r"([\{\[,])\s*([A-Za-z_][A-Za-z0-9_]*)\s*:", r"\1 \"\2\":", s)
    # 把缺失开引号的 key（如 field":）修复为 "field":
    s = re.sub(r"([\{\[,])\s*([A-Za-z_][A-Za-z0-9_]*)\"\s*:", r"\1 \"\2\":", s)

    # 补闭合
    s = _append_missing_closers(s)
    return s

def _robust_json_load(text: str):
    """多候选 + 修复式 JSON 解析，成功返回 dict/obj，否则 None。"""
    for cand in _extract_json_candidates(text):
        # 原样尝试
        try:
            return json.loads(cand)
        except Exception:
            pass
        # 修复后尝试
        fixed = _repair_json_text(cand)
        try:
            return json.loads(fixed)
        except Exception:
            pass
    return None

def _fallback_parse_response_from_text(text: str):
    """当 JSON 彻底损坏时，正则兜底抽取核心字段，尽量不丢结果。"""
    body = re.sub(r"```(?:json)?|```", "", text, flags=re.IGNORECASE).strip()
    verdict_m = re.search(r'"verdict"\s*:\s*"(keep|delete|merge)"', body, re.I)
    conf_m = re.search(r'"confidence"\s*:\s*([0-9]*\.?[0-9]+)', body, re.I)
    summary_m = re.search(r'"summary"\s*:\s*"(.*?)"\s*(?:,\s*"operations"\s*:|$)', body, re.S | re.I)
    if not verdict_m and not summary_m and not conf_m:
        return None

    obj = {
        'verdict': (verdict_m.group(1).lower() if verdict_m else 'keep'),
        'confidence': float(conf_m.group(1)) if conf_m else 0.5,
        'summary': summary_m.group(1).strip() if summary_m else '',
        'operations': [],
    }

    # 粗粒度抽取 operations（即使数组括号损坏也可工作）
    for op_m in re.finditer(r'"op"\s*:\s*"(update_field|add_edge|delete_edge|add_node|delete_node|merge_into)"', body, re.I):
        op_name = op_m.group(1)
        win = body[op_m.start():op_m.start()+420]
        op = {'op': op_name}
        if op_name == 'update_field':
            fm = re.search(r'"field"\s*:\s*"([A-Za-z_][A-Za-z0-9_]*)"', win)
            if not fm:
                fm = re.search(r'\bfield\b\s*:\s*"([A-Za-z_][A-Za-z0-9_]*)"', win)
            vm = re.search(r'"value"\s*:\s*"(.*?)"\s*(?:[,}]|$)', win, re.S)
            if fm and vm:
                op['field'] = fm.group(1)
                op['value'] = vm.group(1)
                obj['operations'].append(op)
        elif op_name in ('add_edge', 'delete_edge'):
            tm = re.search(r'"target"\s*:\s*"(.*?)"', win, re.S)
            em = re.search(r'"edge_type"\s*:\s*"(.*?)"', win, re.S)
            if tm:
                op['target'] = tm.group(1)
                if em:
                    op['edge_type'] = em.group(1)
                obj['operations'].append(op)
        elif op_name == 'add_node':
            nm = re.search(r'"name"\s*:\s*"(.*?)"', win, re.S)
            tm = re.search(r'"entity_type"\s*:\s*"(.*?)"', win, re.S)
            dm = re.search(r'"description"\s*:\s*"(.*?)"\s*(?:[,}]|$)', win, re.S)
            if nm:
                op['name'] = nm.group(1)
                if tm:
                    op['entity_type'] = tm.group(1)
                if dm:
                    op['description'] = dm.group(1)
                obj['operations'].append(op)
        elif op_name == 'delete_node':
            rm = re.search(r'"reason"\s*:\s*"(.*?)"\s*(?:[,}]|$)', win, re.S)
            if rm:
                op['reason'] = rm.group(1)
            obj['operations'].append(op)
        elif op_name == 'merge_into':
            tm = re.search(r'"target_id"\s*:\s*"(.*?)"', win, re.S)
            rm = re.search(r'"reason"\s*:\s*"(.*?)"\s*(?:[,}]|$)', win, re.S)
            if tm:
                op['target_id'] = tm.group(1)
                if rm:
                    op['reason'] = rm.group(1)
                obj['operations'].append(op)
    return obj

def _fallback_parse_compare_from_text(text: str):
    body = re.sub(r"```(?:json)?|```", "", text, flags=re.IGNORECASE).strip()
    wm = re.search(r'"winner"\s*:\s*"([ABab])"', body)
    if not wm:
        return None
    cm = re.search(r'"confidence"\s*:\s*([0-9]*\.?[0-9]+)', body, re.I)
    rm = re.search(r'"reason"\s*:\s*"(.*?)"\s*(?:[,}]|$)', body, re.S | re.I)
    return {
        'winner': wm.group(1).upper(),
        'confidence': float(cm.group(1)) if cm else 0.5,
        'reason': rm.group(1).strip() if rm else '',
    }

def parse_llm_response(raw_text):
    if not raw_text: return None
    text = re.sub(r"<think>.*?</think>","",raw_text.strip(),flags=re.DOTALL).strip()
    obj = _robust_json_load(text)
    if isinstance(obj, dict):
        return _validate_response(obj)
    fb_obj = _fallback_parse_response_from_text(text)
    if isinstance(fb_obj, dict):
        return _validate_response(fb_obj)
    log.warning(f"无法解析 LLM 响应: {text[:200]}...")
    return None

def _validate_response(obj):
    conf_raw = obj.get('confidence', 0.5)
    try:
        conf_val = float(conf_raw)
    except (TypeError, ValueError):
        conf_val = 0.5
    r = {'verdict':obj.get('verdict','keep'),'confidence':conf_val,
         'summary':str(obj.get('summary','')),'operations':[]}
    if r['verdict'] not in ('keep','delete','merge'): r['verdict']='keep'
    r['confidence'] = max(0.0,min(1.0,r['confidence']))
    for op in (obj.get('operations',[]) or []):
        if isinstance(op,dict) and op.get('op') in ('update_field','add_edge','delete_edge','add_node','delete_node','merge_into'):
            r['operations'].append(op)
    return r

def parse_llm_compare(raw_text):
    """解析 LLM 质量对比返回"""
    if not raw_text: return None
    text = re.sub(r"<think>.*?</think>","",raw_text.strip(),flags=re.DOTALL).strip()
    obj = _robust_json_load(text)
    if isinstance(obj, dict) and 'winner' in obj:
        winner = str(obj.get('winner', '')).strip().upper()
        if winner in ('A', 'B'):
            obj['winner'] = winner
            return obj
    fb = _fallback_parse_compare_from_text(text)
    if fb:
        return fb
    return None

# ── 图操作执行器 ─────────────────────────────────────────
class OperationExecutor:
    def __init__(self, graph):
        self.graph = graph; self.all_ops = []
        self.stats = {'update_field':0,'add_edge':0,'delete_edge':0,'add_node':0,'delete_node':0,'merge_into':0,'skipped':0,'errors':0}
    def collect(self, eid, response):
        ops = response.get('operations',[])
        for op in ops:
            self.all_ops.append({'entity_id':eid,'op':op,'verdict':response.get('verdict','keep'),'summary':response.get('summary','')})
        if response.get('verdict')=='delete' and not any(o.get('op')=='delete_node' for o in ops):
            self.all_ops.append({'entity_id':eid,'op':{'op':'delete_node','reason':response.get('summary','verdict=delete')},
                                 'verdict':'delete','summary':response.get('summary','')})
    def execute_all(self):
        if not self.all_ops: return self.stats
        log.info(f"开始执行 {len(self.all_ops)} 个操作...")
        deleted_ids, merged_ids = set(), {}
        for r in self.all_ops:
            op=r['op']; eid=r['entity_id']
            if op.get('op')=='delete_node': deleted_ids.add(eid)
            elif op.get('op')=='merge_into':
                tid=op.get('target_id','')
                if tid and tid in self.graph.entities: merged_ids[eid]=tid
        for r in tqdm(self.all_ops,desc="执行图操作",unit="op"):
            op=r['op']; eid=r['entity_id']; ot=op.get('op','')
            if eid in deleted_ids and ot!='delete_node': self.stats['skipped']+=1; continue
            if eid in merged_ids and ot!='merge_into': self.stats['skipped']+=1; continue
            try:
                if ot=='update_field': self._uf(eid,op)
                elif ot=='add_edge': self._ae(eid,op)
                elif ot=='delete_edge': self._de(eid,op)
                elif ot=='add_node': self._an(op)
                elif ot=='delete_node': self._dn(eid,deleted_ids)
                elif ot=='merge_into': self._mi(eid,op,deleted_ids)
                else: self.stats['skipped']+=1
            except Exception as e: log.warning(f"操作失败 [{eid}] {ot}: {e}"); self.stats['errors']+=1
        orig = len(self.graph.edges)
        self.graph.edges = [e for e in self.graph.edges
            if self.graph.entity_names.get(e.get('source','')) not in deleted_ids
            and self.graph.entity_names.get(e.get('target','')) not in deleted_ids]
        orphan = orig-len(self.graph.edges)
        if orphan: log.info(f"清理 {orphan} 条孤立边")
        self.graph._rebuild_adjacency()
        return self.stats

    def _uf(self, eid, op):
        fn=op.get('field',''); v=op.get('value')
        if not fn or eid not in self.graph.entities: self.stats['skipped']+=1; return
        if fn not in ('description','entity_type','syntax','return_value','remarks','header','deprecated'):
            self.stats['skipped']+=1; return
        self.graph.entities[eid][fn]=v; self.stats['update_field']+=1
    def _ae(self, eid, op):
        tn=op.get('target',''); et=op.get('edge_type','references')
        sn=self.graph.entities.get(eid,{}).get('name','')
        if not tn or not sn: self.stats['skipped']+=1; return
        self.graph.edges.append({'source':sn,'target':tn,'type':et,'source_file':self.graph.source_files.get(eid,'llm')})
        self.stats['add_edge']+=1
    def _de(self, eid, op):
        tn=op.get('target',''); et=op.get('edge_type',''); sn=self.graph.entities.get(eid,{}).get('name','')
        if not sn or not tn: self.stats['skipped']+=1; return
        for i,e in enumerate(self.graph.edges):
            match = (e.get('source')==sn and e.get('target')==tn) or (e.get('source')==tn and e.get('target')==sn)
            if match and (not et or e.get('type')==et):
                self.graph.edges.pop(i); self.stats['delete_edge']+=1; return
        self.stats['skipped']+=1
    def _an(self, op):
        nm=op.get('name','')
        if not nm: self.stats['skipped']+=1; return
        nid=f"windows::{nm}"
        if nid in self.graph.entities: self.stats['skipped']+=1; return
        self.graph.entities[nid]={'id':nid,'name':nm,'entity_type':op.get('entity_type','unknown'),
                                   'description':op.get('description',''),'confidence':0.7,'_source':'llm'}
        self.graph.entity_names[nm]=nid; self.graph.source_files[nid]='llm_added'
        self.stats['add_node']+=1
    def _dn(self, eid, deleted_ids):
        if eid not in self.graph.entities: self.stats['skipped']+=1; return
        ent=self.graph.entities.pop(eid); nm=ent.get('name','')
        if nm in self.graph.entity_names: del self.graph.entity_names[nm]
        deleted_ids.add(eid); self.stats['delete_node']+=1
    def _mi(self, eid, op, deleted_ids):
        tid=op.get('target_id','')
        if tid not in self.graph.entities or eid not in self.graph.entities: self.stats['skipped']+=1; return
        se=self.graph.entities[eid]; te=self.graph.entities[tid]; sn=se.get('name','')
        for k in ('description','syntax','return_value','remarks','header'):
            if not te.get(k) and se.get(k): te[k]=se[k]
        for k in ('parameters','members','cross_references'):
            if se.get(k,[]) and not te.get(k,[]): te[k]=se[k]
        tn=te.get('name','')
        for edge in self.graph.edges:
            if edge.get('source')==sn: edge['source']=tn
            if edge.get('target')==sn: edge['target']=tn
        self.graph.entities.pop(eid,None)
        if sn in self.graph.entity_names: del self.graph.entity_names[sn]
        deleted_ids.add(eid); self.stats['merge_into']+=1

# ── 断点管理器 ───────────────────────────────────────────
class CheckpointManager:
    def __init__(self, ckpt_path, ops_path):
        self.ckpt_path=ckpt_path; self.ops_path=ops_path
        self.processed_ids=set(); self.responses={}
        self.start_time=datetime.now(timezone.utc).isoformat()
    def load(self):
        if not os.path.exists(self.ckpt_path): return False
        try:
            with open(self.ckpt_path,'r',encoding='utf-8') as f: data=json.load(f)
            self.processed_ids=set(data.get('processed_ids',[])); self.start_time=data.get('start_time',self.start_time)
            if os.path.exists(self.ops_path):
                with open(self.ops_path,'r',encoding='utf-8') as f:
                    for line in f:
                        line=line.strip()
                        if not line: continue
                        try:
                            rec=json.loads(line); eid=rec.get('entity_id','')
                            if eid: self.responses[eid]=rec.get('response',{})
                        except: pass
            log.info(f"断点恢复: {len(self.processed_ids)} 实体, {len(self.responses)} 响应")
            return True
        except Exception as e: log.warning(f"断点加载失败: {e}"); return False
    def save(self):
        data={'version':'1.0','start_time':self.start_time,
              'last_save':datetime.now(timezone.utc).isoformat(),
              'processed_count':len(self.processed_ids),'processed_ids':sorted(self.processed_ids)}
        tmp=self.ckpt_path+'.tmp'
        with open(tmp,'w',encoding='utf-8') as f: json.dump(data,f,ensure_ascii=False)
        os.replace(tmp, self.ckpt_path)
    def log_response(self, eid, response):
        self.processed_ids.add(eid); self.responses[eid]=response
        with open(self.ops_path,'a',encoding='utf-8') as f:
            f.write(json.dumps({'entity_id':eid,'timestamp':datetime.now(timezone.utc).isoformat(),'response':response},ensure_ascii=False)+'\n')
    def is_processed(self, eid): return eid in self.processed_ids


# ═══════════════════════════════════════════════════════════════
#  Part D — 统一流水线主流程
# ═══════════════════════════════════════════════════════════════

def load_extract_checkpoint():
    if os.path.exists(CHECKPOINT_FILE):
        try:
            with open(CHECKPOINT_FILE,'r',encoding='utf-8') as f: return json.load(f)
        except: pass
    return {}

def save_extract_checkpoint(data):
    with open(CHECKPOINT_FILE,'w',encoding='utf-8') as f: json.dump(data,f,ensure_ascii=False,indent=2)


async def phase_extract(args, llm_config=None):
    """Phase 1+2: 双源提取 + 质量选优"""
    t0 = time.time()
    force = args.force
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # ── 发现文件对 ──
    pairs = build_file_pairs(WORKSPACE)
    log.info(f"发现 {len(pairs)} 个文档对（{sum(1 for p in pairs if p['p_file'])} 有 .p.txt, {sum(1 for p in pairs if p['t_file'])} 有 .txt）")

    ckpt = load_extract_checkpoint() if not force else {}
    ckpt_hashes = ckpt.get('file_hashes', {})

    # ── Pass-1: 构建全局词汇表（扫描所有源文件） ──
    all_source_files = []
    for p in pairs:
        if p['p_file']: all_source_files.append(p['p_file'])
        if p['t_file']: all_source_files.append(p['t_file'])

    log.info(f"═══ Pass-1: 构建全局实体词汇表 ({len(all_source_files)} 文件) ═══")
    global_names = set()
    for idx, fname in enumerate(tqdm(all_source_files, desc="Pass-1 词汇扫描", unit="file"), 1):
        fp = os.path.join(WORKSPACE, fname)
        try:
            global_names |= pass1_collect_names(fp)
        except Exception as ex:
            log.warning(f"Pass-1 跳过: {fname} → {ex}")
    log.info(f"  Pass-1 完成: {len(global_names)} 个全局实体名 ({time.time()-t0:.1f}s)")

    # ── Pass-2: 双源提取 + 质量对比 ──
    log.info(f"═══ Pass-2: 双源提取 + 质量选优 ({len(pairs)} 文档) ═══")

    # 需要 LLM 裁决的文档对
    llm_compare_queue = []
    all_entity_map = {}
    all_edges = []
    file_entity_lists = {}   # out_name → entity_list
    new_ckpt_hashes = {}
    total_entities = 0
    total_errors = 0
    skipped = 0
    summary = []
    quality_decisions = []  # 记录每对的选优决策

    for pair in tqdm(pairs, desc="Pass-2 双源提取", unit="doc"):
        base = pair['base']
        candidates = {}  # 'p' or 't' → (doc_meta, entity_list, score_info)

        for src_key, src_file in [('p', pair['p_file']), ('t', pair['t_file'])]:
            if not src_file:
                continue
            fp = os.path.join(WORKSPACE, src_file)
            fh = file_hash(fp)
            new_ckpt_hashes[src_file] = fh

            # 输出名
            out_name = src_file.replace('[OCR]_windows-', '')
            if out_name.endswith('.p.txt'): out_name = out_name[:-6] + '.json'
            elif out_name.endswith('.txt'): out_name = out_name[:-4] + '.json'

            # 断点续跑: 尝试从缓存加载
            cached_out = os.path.join(OUTPUT_DIR, f"_{src_key}_{out_name}")
            if not force and ckpt_hashes.get(src_file) == fh and os.path.exists(cached_out):
                try:
                    with open(cached_out, 'r', encoding='utf-8') as f:
                        cached = json.load(f)
                    el = cached.get('entities', [])
                    si = score_entity_list(el)
                    dm = cached.get('document', {})
                    candidates[src_key] = (dm, el, si, out_name)
                    continue
                except:
                    pass

            try:
                dm, el = extract_entities_from_file(fp, global_names)
                result = build_output(dm, el)
                issues = self_test(result)
                errors = [i for i in issues if i.startswith('ERROR')]
                if errors:
                    total_errors += len(errors)
                si = score_entity_list(el)
                candidates[src_key] = (dm, el, si, out_name)
                # 缓存中间结果
                with open(os.path.join(OUTPUT_DIR, f"_{src_key}_{out_name}"), 'w', encoding='utf-8') as f:
                    json.dump(result, f, ensure_ascii=False, indent=2)
            except Exception as ex:
                log.warning(f"提取失败: {src_file} → {ex}")

        if not candidates:
            summary.append({'base': base, 'decision': 'none', 'entities': 0})
            continue

        # ── 质量选优 ──
        if len(candidates) == 1:
            # 只有一个源
            winner_key = list(candidates.keys())[0]
            dm, el, si, out_name = candidates[winner_key]
            decision = f"only_{winner_key}"
        else:
            # 两个源都有，对比分数
            p_dm, p_el, p_si, p_out = candidates['p']
            t_dm, t_el, t_si, t_out = candidates['t']
            score_diff = abs(p_si['score'] - t_si['score'])

            if score_diff == 0:
                # 提取结果完全一致，默认选 .p.txt，无需 LLM
                winner_key = 'p'
                decision = f"identical (score={p_si['score']:.4f})"
            elif score_diff >= 0.005:
                # 启发式能区分
                if p_si['score'] >= t_si['score']:
                    winner_key = 'p'
                    decision = f"heuristic_p (p={p_si['score']:.3f} t={t_si['score']:.3f})"
                else:
                    winner_key = 't'
                    decision = f"heuristic_t (p={p_si['score']:.3f} t={t_si['score']:.3f})"
            else:
                # 差距太小，加入 LLM 裁决队列
                llm_compare_queue.append((pair, candidates))
                decision = f"llm_pending (p={p_si['score']:.3f} t={t_si['score']:.3f})"
                winner_key = None
                # 暂时选 p 作为默认
                dm, el, si, out_name = candidates['p']
                decision_info = {'base':base,'decision':decision,'p_score':p_si['score'],
                                 't_score':t_si['score'],'entities':len(el)}
                quality_decisions.append(decision_info)
                summary.append(decision_info)

                # 先放入图（后面 LLM 可能会替换）
                out_path = os.path.join(OUTPUT_DIR, out_name)
                result = build_output(dm, el)
                with open(out_path, 'w', encoding='utf-8') as f:
                    json.dump(result, f, ensure_ascii=False, indent=2)
                for e in el:
                    ename = e.get('name', '')
                    all_entity_map[ename] = {'id': e.get('id'), 'file': out_name, 'type': e.get('entity_type')}
                file_entity_lists[out_name] = el
                total_entities += len(el)
                continue

            dm, el, si, out_name = candidates[winner_key]

        # 保存选中版本
        out_path = os.path.join(OUTPUT_DIR, out_name)
        result = build_output(dm, el)
        with open(out_path, 'w', encoding='utf-8') as f:
            json.dump(result, f, ensure_ascii=False, indent=2)

        for e in el:
            ename = e.get('name', '')
            all_entity_map[ename] = {'id': e.get('id'), 'file': out_name, 'type': e.get('entity_type')}
        file_entity_lists[out_name] = el
        total_entities += len(el)

        decision_info = {'base':base,'decision':decision,'entities':len(el)}
        quality_decisions.append(decision_info)
        summary.append(decision_info)

    # ── LLM 裁决（差距太小的文档对） ──
    if llm_compare_queue and llm_config and not args.dry_run:
        log.info(f"═══ LLM 质量裁决: {len(llm_compare_queue)} 个近似文档对 ═══")
        rl = AsyncRateLimiter(llm_config.requests_per_min)
        async with LLMClient(llm_config, rl) as client:
            for pair, cands in tqdm(llm_compare_queue, desc="LLM质量裁决", unit="doc"):
                p_dm, p_el, p_si, p_out = cands['p']
                t_dm, t_el, t_si, t_out = cands['t']
                # 构建对比 prompt
                p_samples = json.dumps([{'name':e['name'],'type':e.get('entity_type',''),'desc':_truncate(e.get('description',''),100)}
                                         for e in p_el[:5]], ensure_ascii=False, indent=1)
                t_samples = json.dumps([{'name':e['name'],'type':e.get('entity_type',''),'desc':_truncate(e.get('description',''),100)}
                                         for e in t_el[:5]], ensure_ascii=False, indent=1)
                prompt = COMPARE_PROMPT.format(
                    a_source='.p.txt', a_count=len(p_el), a_conf=f"{p_si['avg_conf']:.3f}", a_samples=p_samples,
                    b_source='.txt', b_count=len(t_el), b_conf=f"{t_si['avg_conf']:.3f}", b_samples=t_samples)
                resp = await client.chat([{'role':'system','content':'你是 Windows API 文档质量评估专家。'},
                                          {'role':'user','content':prompt}])
                result = parse_llm_compare(resp)
                if result and result.get('winner') in ('A','B'):
                    winner_key = 'p' if result['winner']=='A' else 't'
                    log.info(f"  LLM 裁决 {pair['base']}: 选 {'p.txt' if winner_key=='p' else '.txt'} ({result.get('reason','')})")
                else:
                    winner_key = 'p'  # 默认回退
                    log.info(f"  LLM 裁决失败 {pair['base']}: 默认选 .p.txt")

                # 替换之前临时选的
                dm, el, si, out_name = cands[winner_key]
                out_path = os.path.join(OUTPUT_DIR, out_name)
                res = build_output(dm, el)
                with open(out_path, 'w', encoding='utf-8') as f:
                    json.dump(res, f, ensure_ascii=False, indent=2)
                # 更新实体映射
                for e in el:
                    ename = e.get('name', '')
                    all_entity_map[ename] = {'id': e.get('id'), 'file': out_name, 'type': e.get('entity_type')}
                file_entity_lists[out_name] = el

    elif llm_compare_queue:
        log.info(f"跳过 LLM 裁决（{len(llm_compare_queue)} 对，{'dry-run' if args.dry_run else '无LLM配置'}），默认选 .p.txt")

    # ── 构建全局边 ──
    log.info("构建全局关系边...")
    for out_name, el in file_entity_lists.items():
        for e in el:
            src = e.get('name', '')
            for ref in e.get('cross_references', []):
                all_edges.append({'source': src, 'target': ref, 'type': 'references', 'source_file': out_name})

    # ── 保存全局索引 ──
    idx_doc = OrderedDict([
        ('_schema', 'global_entity_index_v4.0'),
        ('_generated_at', datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')),
        ('total_unique_entities', len(all_entity_map)),
        ('entities', OrderedDict(sorted(all_entity_map.items()))),
    ])
    with open(os.path.join(OUTPUT_DIR, 'global_entity_index.json'), 'w', encoding='utf-8') as f:
        json.dump(idx_doc, f, ensure_ascii=False, indent=2)

    edges_doc = OrderedDict([
        ('_schema', 'api_edges_v4.0'),
        ('_generated_at', datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')),
        ('total_edges', len(all_edges)),
        ('edges', all_edges),
    ])
    with open(os.path.join(OUTPUT_DIR, 'global_edges.json'), 'w', encoding='utf-8') as f:
        json.dump(edges_doc, f, ensure_ascii=False, indent=2)

    # ── 保存断点 ──
    save_extract_checkpoint({
        'schema_version': SCHEMA_VERSION,
        'last_run': datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ'),
        'file_hashes': new_ckpt_hashes,
    })

    elapsed = time.time() - t0
    log.info(f"{'='*60}")
    log.info(f"提取完成 ({elapsed:.1f}s)")
    log.info(f"  文档对: {len(pairs)} | 总实体: {total_entities} | 全局索引: {len(all_entity_map)} | 边: {len(all_edges)}")
    log.info(f"  质量决策: {len(quality_decisions)} (LLM裁决: {len(llm_compare_queue)})")
    log.info(f"{'='*60}")

    # 保存提取报告
    report = {'_schema':'extraction_report_v4.0',
              '_generated_at':datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ'),
              'total_pairs':len(pairs),'total_entities':total_entities,'total_errors':total_errors,
              'global_entities':len(all_entity_map),'global_edges':len(all_edges),
              'llm_compare_count':len(llm_compare_queue),'quality_decisions':quality_decisions}
    with open(os.path.join(OUTPUT_DIR,'_extraction_report.json'),'w',encoding='utf-8') as f:
        json.dump(report,f,ensure_ascii=False,indent=2)

    return all_entity_map, all_edges, file_entity_lists


async def phase_refine(args, llm_config, all_entity_map=None, all_edges=None, file_entity_lists=None):
    """Phase 3: LLM 精炼"""
    t0 = time.time()

    # ── 加载图谱 ──
    graph = KnowledgeGraph()
    if all_entity_map and all_edges and file_entity_lists:
        graph.load_from_extraction(all_entity_map, all_edges, file_entity_lists)
    else:
        graph.load_from_dir(OUTPUT_DIR)
    initial_stats = graph.stats()
    log.info(f"初始图谱: {json.dumps(initial_stats, ensure_ascii=False)}")

    if not llm_config:
        log.error("LLM 配置不可用，无法执行精炼阶段"); return

    log.info(f"LLM: {llm_config.model_name} @ {llm_config.api_base_url}")

    # ── 断点续跑 ──
    ckpt = CheckpointManager(LLM_CKPT_FILE, OPS_LOG_FILE)
    if args.resume: ckpt.load()

    # ── 筛选待处理实体 ──
    all_eids = list(graph.entities.keys())
    pending = [eid for eid in all_eids if not ckpt.is_processed(eid)]
    if args.min_confidence < 1.0:
        pending = [eid for eid in pending if graph.entities[eid].get('confidence',1.0)<=args.min_confidence]
        log.info(f"置信度筛选 (≤{args.min_confidence}): {len(pending)} 实体")
    if args.entity_type:
        pending = [eid for eid in pending if graph.entities[eid].get('entity_type','unknown')==args.entity_type]
    if args.max_entities > 0:
        pending = pending[:args.max_entities]
    already = len(ckpt.processed_ids)
    log.info(f"待精炼: {len(pending)} 实体 (已跳过 {already})")

    if not pending:
        log.info("所有实体已处理完毕"); return

    if args.dry_run:
        est = len(pending)/llm_config.requests_per_min
        log.info(f"[DRY-RUN] 预计 LLM 调用: {len(pending)} | 耗时: {est:.1f}分钟 ({est/60:.1f}小时)")
        for eid in pending[:3]:
            ent = graph.entities[eid]; nb, ed = graph.get_neighborhood(eid)
            prompt = build_user_prompt(ent, nb, ed)
            log.info(f"\n{'='*60}\n[SAMPLE] {eid}\n{prompt[:500]}...\n{'='*60}")
        return

    # ── 异步处理 ──
    rl = AsyncRateLimiter(llm_config.requests_per_min)
    sem = asyncio.Semaphore(llm_config.max_workers)
    executor = OperationExecutor(graph)

    for eid, resp in ckpt.responses.items(): executor.collect(eid, resp)
    if ckpt.responses: log.info(f"从断点恢复 {len(ckpt.responses)} 条操作")

    async def _process_one(eid):
        async with sem:
            ent = graph.entities.get(eid)
            if not ent: return None
            nb, ed = graph.get_neighborhood(eid)
            msg = build_user_prompt(ent, nb, ed)
            raw = await client.chat([{'role':'system','content':SYSTEM_PROMPT},{'role':'user','content':msg}])
            parsed = parse_llm_response(raw) or {'verdict':'keep','confidence':0.0,'summary':'解析失败','operations':[]}
            ckpt.log_response(eid, parsed)
            executor.collect(eid, parsed)
            return parsed

    async with LLMClient(llm_config, rl) as client:
        bs = llm_config.batch_size
        batches = [pending[i:i+bs] for i in range(0,len(pending),bs)]
        pbar = tqdm(total=len(pending),desc="LLM 精炼实体",unit="entity",
                    bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}]")
        for bi, batch in enumerate(batches):
            results = await asyncio.gather(*[_process_one(eid) for eid in batch], return_exceptions=True)
            errs = sum(1 for r in results if isinstance(r, Exception))
            if errs:
                for r in results:
                    if isinstance(r, Exception): log.warning(f"异常: {r}")
            pbar.update(len(batch)); ckpt.save()
            if (bi+1)%10==0: pbar.set_postfix_str(f"batch={bi+1}/{len(batches)} | {client.usage_report()}")
        pbar.close()
        log.info(f"LLM 完成: {client.usage_report()}")

    # ── 执行图操作 ──
    if args.review:
        oc = defaultdict(int)
        for r in executor.all_ops: oc[r['op'].get('op','?')]+=1
        print(f"\n操作摘要 ({len(executor.all_ops)} 个): {dict(oc)}")
        for r in executor.all_ops[:10]: print(f"  [{r['entity_id']}] {r['op']}")
        if input("\n执行？[y/N]: ").strip().lower()!='y': log.info("用户取消"); return

    exec_stats = executor.execute_all()
    log.info(f"操作执行: {json.dumps(exec_stats, ensure_ascii=False)}")

    # ── 保存精炼图谱 ──
    save_refined_graph(graph, OUTPUT_DIR, exec_stats)

    final = graph.stats()
    elapsed = time.time()-t0
    log.info(f"\n{'='*60}")
    log.info(f"精炼完成 ({elapsed:.1f}s)")
    log.info(f"  原始: {initial_stats['total_entities']} 实体, {initial_stats['total_edges']} 边")
    log.info(f"  精炼: {final['total_entities']} 实体, {final['total_edges']} 边")
    log.info(f"  Δ实体={final['total_entities']-initial_stats['total_entities']}, Δ边={final['total_edges']-initial_stats['total_edges']}")
    log.info(f"{'='*60}")


def save_refined_graph(graph, output_dir, exec_stats):
    """保存精炼后的图谱（覆盖写入同一目录）"""
    file_groups = defaultdict(list)
    for eid, ent in graph.entities.items():
        sf = graph.source_files.get(eid, 'llm_added.json')
        file_groups[sf].append(ent)
    saved = 0
    for fname, ents in file_groups.items():
        with open(os.path.join(output_dir, fname), 'w', encoding='utf-8') as f:
            json.dump({'_schema':SCHEMA_VERSION+'_refined','_generated_at':datetime.now(timezone.utc).isoformat(),
                       'entities':ents}, f, ensure_ascii=False, indent=2)
        saved += 1
    # 全局索引
    idx = {}
    for eid, ent in graph.entities.items():
        idx[ent.get('name','')] = {'id':eid,'file':graph.source_files.get(eid,''),'type':ent.get('entity_type','unknown')}
    with open(os.path.join(output_dir,'global_entity_index.json'),'w',encoding='utf-8') as f:
        json.dump({'_schema':'global_entity_index_v4.0_refined',
                   '_generated_at':datetime.now(timezone.utc).isoformat(),
                   'total_unique_entities':len(idx),'entities':idx},f,ensure_ascii=False,indent=2)
    # 全局边
    with open(os.path.join(output_dir,'global_edges.json'),'w',encoding='utf-8') as f:
        json.dump({'_schema':'api_edges_v4.0_refined',
                   '_generated_at':datetime.now(timezone.utc).isoformat(),
                   'total_edges':len(graph.edges),'edges':graph.edges},f,ensure_ascii=False,indent=2)
    # 报告
    with open(os.path.join(output_dir,'_refinement_report.json'),'w',encoding='utf-8') as f:
        json.dump({'_schema':'refinement_report_v4.0','_generated_at':datetime.now(timezone.utc).isoformat(),
                   'graph_stats':graph.stats(),'operation_stats':exec_stats,
                   'output_files':saved,'total_entities':sum(len(v) for v in file_groups.values()),
                   'total_edges':len(graph.edges)},f,ensure_ascii=False,indent=2)
    log.info(f"精炼图谱已保存: {saved} 文件, {len(graph.entities)} 实体, {len(graph.edges)} 边")


# ═══════════════════════════════════════════════════════════════
#  CLI 入口
# ═══════════════════════════════════════════════════════════════

async def run_pipeline(args):
    llm_config = load_llm_config(args.provider)

    if args.phase in ('all', 'extract'):
        result = await phase_extract(args, llm_config)
    else:
        result = None

    if args.phase in ('all', 'refine'):
        if result:
            em, ae, fe = result
            await phase_refine(args, llm_config, em, ae, fe)
        else:
            await phase_refine(args, llm_config)


def main():
    parser = argparse.ArgumentParser(
        description="Windows API 知识图谱统一流水线 v4.0",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  python pipeline.py                          # 完整流水线（提取 + 精炼）
  python pipeline.py --phase extract          # 仅正则提取
  python pipeline.py --phase refine           # 仅 LLM 精炼
  python pipeline.py --max-entities 20        # LLM 最多处理 20 个实体
  python pipeline.py --min-confidence 0.5     # 仅精炼低置信度实体
  python pipeline.py --provider ollama        # 使用本地 Ollama
  python pipeline.py --resume                 # 断点续跑
  python pipeline.py --dry-run                # 预览不执行 LLM
  python pipeline.py --force                  # 强制重新提取
""",
    )
    parser.add_argument("--phase", choices=["all","extract","refine"], default="all", help="运行阶段 (默认: all)")
    parser.add_argument("--provider", choices=["deepseek","ollama"], default="deepseek", help="LLM 提供商")
    parser.add_argument("--max-entities", type=int, default=0, help="LLM 最多处理 N 个实体 (0=全部)")
    parser.add_argument("--min-confidence", type=float, default=1.0, help="仅精炼置信度 ≤ 此值的实体")
    parser.add_argument("--entity-type", type=str, default="", help="仅精炼指定类型")
    parser.add_argument("--resume", action="store_true", help="断点续跑")
    parser.add_argument("--dry-run", action="store_true", help="不调用 LLM")
    parser.add_argument("--review", action="store_true", help="执行操作前人工审查")
    parser.add_argument("--force", action="store_true", help="忽略缓存")
    args = parser.parse_args()
    asyncio.run(run_pipeline(args))


if __name__ == "__main__":
    main()
