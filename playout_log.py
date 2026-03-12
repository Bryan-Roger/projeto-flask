"""
playout_log.py – Sistema de log centralizado para TV Verde Vale Playout
Grava em arquivo rotativo + console com timestamps precisos.
"""

import os, sys, threading
from datetime import datetime
from pathlib import Path

_lock  = threading.Lock()
_fh    = None
_level = 'DEBUG'

LEVELS = {'DEBUG': 0, 'INFO': 1, 'WARN': 2, 'ERROR': 3}

LOG_DIR = Path(r'C:\Users\tv\Documents\claude\logs')


def init(level='DEBUG'):
    global _fh, _level
    _level = level
    LOG_DIR.mkdir(exist_ok=True)
    fname = LOG_DIR / ('playout_%s.log' % datetime.now().strftime('%Y-%m-%d_%H-%M-%S'))
    _fh = open(fname, 'w', encoding='utf-8', buffering=1)
    _write('INFO', 'LOG', '=' * 60)
    _write('INFO', 'LOG', 'TV Verde Vale Playout — log iniciado')
    _write('INFO', 'LOG', 'Arquivo: ' + str(fname))
    _write('INFO', 'LOG', '=' * 60)
    return fname


def _write(level, module, msg):
    if LEVELS.get(level, 0) < LEVELS.get(_level, 0):
        return
    ts  = datetime.now().strftime('%H:%M:%S.%f')[:-3]
    line = '[%s] %s %-12s %s' % (ts, level[0], module, msg)
    with _lock:
        print(line)
        if _fh:
            _fh.write(line + '\n')


def debug(module, msg): _write('DEBUG', module, msg)
def info (module, msg): _write('INFO',  module, msg)
def warn (module, msg): _write('WARN',  module, msg)
def error(module, msg): _write('ERROR', module, msg)


def section(title):
    _write('INFO', 'LOG', '')
    _write('INFO', 'LOG', '─── ' + title + ' ' + '─' * max(0, 50 - len(title)))


def vt_info(filepath):
    """Tenta extrair info do VT via ffprobe e loga."""
    import subprocess, json
    ffprobe = None
    for p in [r'C:\ffmpeg\bin\ffprobe.exe',
              r'C:\Users\tv\Documents\claude\ffprobe.exe',
              r'C:\Users\tv\Documents\claude\bin\ffprobe.exe']:
        if os.path.isfile(p):
            ffprobe = p
            break
    if not ffprobe:
        ffprobe = 'ffprobe'

    try:
        r = subprocess.run(
            [ffprobe, '-v', 'quiet', '-print_format', 'json',
             '-show_streams', '-show_format', filepath],
            capture_output=True, text=True, timeout=10)
        d = json.loads(r.stdout)
        for s in d.get('streams', []):
            ct = s.get('codec_type', '?')
            if ct == 'video':
                _write('DEBUG', 'VT', '  video: %s profile=%s level=%s %sx%s fps=%s pix=%s scan=%s' % (
                    s.get('codec_name','?'), s.get('profile','-'), s.get('level','-'),
                    s.get('width','-'), s.get('height','-'),
                    s.get('r_frame_rate','-'), s.get('pix_fmt','-'),
                    s.get('field_order','-')))
            elif ct == 'audio':
                _write('DEBUG', 'VT', '  audio: %s %sHz ch=%s' % (
                    s.get('codec_name','?'), s.get('sample_rate','-'), s.get('channels','-')))
        fmt = d.get('format', {})
        _write('DEBUG', 'VT', '  dur=%.2fs size=%sKB fmt=%s' % (
            float(fmt.get('duration', 0)),
            int(fmt.get('size', 0)) // 1024,
            fmt.get('format_name', '?')))
    except Exception as e:
        _write('DEBUG', 'VT', '  ffprobe indisponivel: ' + str(e))
