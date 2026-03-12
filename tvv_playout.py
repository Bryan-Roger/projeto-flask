#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
TVV Playout — TV Verde Vale
============================
Duas janelas:
  PlayerWindow   — preview, transporte, volume, tempos
  PlaylistWindow — lista de VTs, toolbar, botões

Motor DeckLink: lazy-init (só abre quando reproduz, libera ao parar)
"""

import sys, os, json, subprocess
from pathlib import Path
from datetime import datetime
from enum import Enum, auto
import copy as _copy

from PyQt5.QtWidgets import (
    QApplication, QMainWindow, QWidget, QLabel, QPushButton,
    QVBoxLayout, QHBoxLayout, QSplitter, QTableWidget, QTableWidgetItem,
    QHeaderView, QAbstractItemView, QFrame, QSlider, QMenu, QAction,
    QFileDialog, QMessageBox, QDialog, QDialogButtonBox, QFormLayout,
    QLineEdit, QComboBox, QToolBar, QStatusBar, QInputDialog, QGroupBox,
    QCheckBox, QProgressBar, QTabWidget, QSpinBox, QDoubleSpinBox,
    QRadioButton, QButtonGroup, QShortcut, QDial,
)
from PyQt5.QtCore import Qt, QTimer, QThread, pyqtSignal, QObject, QPoint, QSize
from PyQt5.QtGui import QColor, QFont, QKeySequence, QPainter

# ── Caminhos ───────────────────────────────────────────────────────────────────
APP_DIR       = Path(__file__).parent
CONFIG_FILE   = APP_DIR / 'config.json'
LAST_PL_FILE  = APP_DIR / 'last_playlist.json'
PLAYLISTS_DIR = APP_DIR / 'playlists'
PLAYLISTS_DIR.mkdir(exist_ok=True)

# ── Motor DeckLink (opcional) ──────────────────────────────────────────────────
# Usa a copia local (tvv_playout/) que contem o fix do sync de audio.
# O original em claude/ NAO e modificado.
DECKLINK_ENGINE_OK = False
try:
    sys.path.insert(0, str(APP_DIR))                          # pasta local primeiro
    sys.path.insert(1, str(Path(r'C:\Users\tv\Documents\claude')))  # fallback
    from decklink_out import DeckLinkGraph
    DECKLINK_ENGINE_OK = True
except Exception as _e:
    print(f'[WARN] decklink_out: {_e}')

try:
    import psutil
    PSUTIL_OK = True
except ImportError:
    PSUTIL_OK = False

try:
    import vlc
    VLC_OK = True
except ImportError:
    VLC_OK = False

# ── ffprobe ────────────────────────────────────────────────────────────────────
_FFPROBE_PATHS = [
    r'C:\ffmpeg\bin\ffprobe.exe',
    r'C:\Users\tv\Documents\claude\ffmpeg\bin\ffprobe.exe',
    'ffprobe',
]
def _find_ffprobe():
    for p in _FFPROBE_PATHS:
        if p == 'ffprobe' or Path(p).is_file(): return p
    return 'ffprobe'

def _fmt_time(s: float) -> str:
    s = max(0, int(s))
    return f'{s//3600:02d}:{(s%3600)//60:02d}:{s%60:02d}'


# ══════════════════════════════════════════════════════════════════════════════
# MODELO DE DADOS
# ══════════════════════════════════════════════════════════════════════════════

class ItemType(Enum):
    FILE    = auto()
    SRT_IN  = auto()
    RTMP_IN = auto()
    YT_IN   = auto()
    PAUSE   = auto()   # item de pausa temporizada entre VTs

class PlaylistItem:
    def __init__(self):
        self.type        : ItemType = ItemType.FILE
        self.filepath    : str   = ''
        self.url         : str   = ''
        self.title       : str   = ''
        self.duration    : float = 0.0
        self.pause_after : bool  = False
        self.in_point    : float = 0.0
        self.out_point   : float = 0.0
        # metadados preenchidos pelo ProbeWorker
        self.fps         : float = 0.0
        self.fps_str     : str   = ''
        self.width       : int   = 0
        self.height      : int   = 0
        self.codec_v     : str   = ''
        self.codec_a     : str   = ''
        self.fmt         : str   = ''
        self.file_size   : int   = 0   # bytes
        # para ItemType.PAUSE
        self.pause_dur   : float = 5.0  # segundos de pausa

    @staticmethod
    def from_file(fp: str) -> 'PlaylistItem':
        i = PlaylistItem(); i.filepath = fp; i.title = Path(fp).name; return i

    @staticmethod
    def from_stream(url: str, t: ItemType, title: str = '') -> 'PlaylistItem':
        i = PlaylistItem(); i.type = t; i.url = url
        i.title = title or url; return i

    @staticmethod
    def from_pause(dur: float = 5.0) -> 'PlaylistItem':
        i = PlaylistItem(); i.type = ItemType.PAUSE
        i.pause_dur = dur; i.duration = dur
        i.title = f'⏸ Pausa  {dur:.0f}s'
        return i

    def to_dict(self):
        return dict(type=self.type.name, filepath=self.filepath, url=self.url,
                    title=self.title, duration=self.duration,
                    pause_after=self.pause_after,
                    in_point=self.in_point, out_point=self.out_point,
                    fps=self.fps, fps_str=self.fps_str,
                    width=self.width, height=self.height,
                    codec_v=self.codec_v, codec_a=self.codec_a,
                    fmt=self.fmt, file_size=self.file_size,
                    pause_dur=self.pause_dur)

    @staticmethod
    def from_dict(d: dict) -> 'PlaylistItem':
        i = PlaylistItem()
        i.type        = ItemType[d.get('type','FILE')]
        i.filepath    = d.get('filepath','')
        i.url         = d.get('url','')
        i.title       = d.get('title','')
        i.duration    = d.get('duration', 0.0)
        i.pause_after = d.get('pause_after', False)
        i.in_point    = d.get('in_point', 0.0)
        i.out_point   = d.get('out_point', 0.0)
        i.fps         = d.get('fps', 0.0)
        i.fps_str     = d.get('fps_str', '')
        i.width       = d.get('width', 0)
        i.height      = d.get('height', 0)
        i.codec_v     = d.get('codec_v', '')
        i.codec_a     = d.get('codec_a', '')
        i.fmt         = d.get('fmt', '')
        i.file_size   = d.get('file_size', 0)
        i.pause_dur   = d.get('pause_dur', 5.0)
        return i

    def display_dur(self) -> str:
        if self.type == ItemType.PAUSE:
            return _fmt_time(self.pause_dur)
        if self.type != ItemType.FILE or self.duration <= 0:
            return '∞' if self.type != ItemType.FILE else '--:--'
        return _fmt_time(self.duration)

    def type_icon(self) -> str:
        return {ItemType.FILE:'▶', ItemType.SRT_IN:'📡',
                ItemType.RTMP_IN:'🎥', ItemType.YT_IN:'▶️',
                ItemType.PAUSE:'⏸'}.get(self.type,'?')


# ══════════════════════════════════════════════════════════════════════════════
# PLAYLIST MODEL
# ══════════════════════════════════════════════════════════════════════════════

class PlaylistModel(QObject):
    changed = pyqtSignal()
    def __init__(self): super().__init__(); self._items: list[PlaylistItem] = []
    def __len__(self):    return len(self._items)
    def __getitem__(self, i): return self._items[i]
    def items(self):      return list(self._items)
    def append(self, it): self._items.append(it); self.changed.emit()
    def insert(self, i, it): self._items.insert(i, it); self.changed.emit()
    def remove(self, i):
        if 0 <= i < len(self._items): self._items.pop(i); self.changed.emit()
    def move(self, f, t):
        if f == t: return
        it = self._items.pop(f); self._items.insert(t, it); self.changed.emit()
    def clear(self): self._items.clear(); self.changed.emit()
    def update_dur(self, i, d):
        if 0 <= i < len(self._items): self._items[i].duration = d; self.changed.emit()
    def update_meta(self, i, meta: dict):
        """Atualiza metadados completos do item (resultado do ProbeWorker)."""
        if not (0 <= i < len(self._items)): return
        it = self._items[i]
        it.duration  = meta.get('duration',  it.duration)
        it.fps       = meta.get('fps',       it.fps)
        it.fps_str   = meta.get('fps_str',   it.fps_str)
        it.width     = meta.get('width',     it.width)
        it.height    = meta.get('height',    it.height)
        it.codec_v   = meta.get('codec_v',   it.codec_v)
        it.codec_a   = meta.get('codec_a',   it.codec_a)
        it.fmt       = meta.get('fmt',       it.fmt)
        it.file_size = meta.get('file_size', it.file_size)
        self.changed.emit()
    def total_dur(self): return sum(i.duration for i in self._items if i.duration > 0)
    def to_json(self): return json.dumps([i.to_dict() for i in self._items],
                                          ensure_ascii=False, indent=2)
    def load_json(self, txt: str):
        self._items = [PlaylistItem.from_dict(d) for d in json.loads(txt)]
        self.changed.emit()


# ══════════════════════════════════════════════════════════════════════════════
# PROBE ASSÍNCRONO
# ══════════════════════════════════════════════════════════════════════════════

class ProbeWorker(QThread):
    """Probe completo: retorna dict com duration, fps, resolução, codecs, etc."""
    result = pyqtSignal(int, dict)
    def __init__(self, idx, fp): super().__init__(); self.idx=idx; self.fp=fp
    def run(self):
        meta = {'duration':0.0,'fps':0.0,'fps_str':'','width':0,'height':0,
                'codec_v':'','codec_a':'','fmt':'','file_size':0}
        try:
            fl = subprocess.CREATE_NO_WINDOW if os.name=='nt' else 0
            out = subprocess.check_output(
                [_find_ffprobe(),'-v','quiet','-print_format','json',
                 '-show_streams','-show_format', self.fp],
                stderr=subprocess.DEVNULL, timeout=15, creationflags=fl)
            d = json.loads(out)
            fmt = d.get('format', {})
            meta['duration']  = float(fmt.get('duration', 0) or 0)
            meta['file_size'] = int(fmt.get('size', 0) or 0)
            # Formato: usar extensão real do arquivo (mp4, mov, mkv…)
            # format_name do ffprobe retorna "mov,mp4,…" para .mp4 — confuso
            ext = Path(self.fp).suffix.lower().lstrip('.')
            meta['fmt'] = ext if ext else fmt.get('format_name','').split(',')[0]
            for s in d.get('streams', []):
                ct = s.get('codec_type','')
                if ct == 'video' and not meta['codec_v']:
                    fps_str = s.get('r_frame_rate','')
                    meta['fps_str'] = fps_str
                    try:
                        n, den = fps_str.split('/')
                        meta['fps'] = round(float(n)/float(den), 3)
                    except Exception: pass
                    meta['width']   = int(s.get('width',  0) or 0)
                    meta['height']  = int(s.get('height', 0) or 0)
                    meta['codec_v'] = s.get('codec_name','')
                elif ct == 'audio' and not meta['codec_a']:
                    ar = int(s.get('sample_rate', 0) or 0)
                    ch = int(s.get('channels', 0) or 0)
                    cn = s.get('codec_name','')
                    meta['codec_a'] = f'{cn} {ar//1000}k' if ar else cn
        except Exception: pass
        self.result.emit(self.idx, meta)


# ══════════════════════════════════════════════════════════════════════════════
# MONITOR DE CPU / GPU
# ══════════════════════════════════════════════════════════════════════════════

class SysMonWorker(QThread):
    """Coleta CPU (psutil) e GPU (nvidia-smi) a cada 2s."""
    stats = pyqtSignal(float, float)   # (cpu_pct, gpu_pct) — gpu=-1 se indisponível

    _nvidia_ok : bool | None = None   # None = não testado ainda

    def run(self):
        while not self.isInterruptionRequested():
            cpu = psutil.cpu_percent(interval=None) if PSUTIL_OK else -1.0
            gpu = self._get_gpu()
            self.stats.emit(cpu, gpu)
            self.msleep(2000)

    def _get_gpu(self) -> float:
        if SysMonWorker._nvidia_ok is False: return -1.0
        try:
            out = subprocess.check_output(
                ['nvidia-smi', '--query-gpu=utilization.gpu',
                 '--format=csv,noheader,nounits'],
                stderr=subprocess.DEVNULL, timeout=3,
                creationflags=subprocess.CREATE_NO_WINDOW if os.name=='nt' else 0)
            SysMonWorker._nvidia_ok = True
            return float(out.strip().splitlines()[0])
        except Exception:
            SysMonWorker._nvidia_ok = False
            return -1.0


# ══════════════════════════════════════════════════════════════════════════════
# ENGINE BRIDGE + PLAYER ENGINE  (lazy DeckLink init)
# ══════════════════════════════════════════════════════════════════════════════

class _Bridge(QObject):
    ended = pyqtSignal()
    error = pyqtSignal(str)

class PlayerEngine(QObject):
    sig_ended    = pyqtSignal()
    sig_error    = pyqtSignal(str)
    sig_position = pyqtSignal(float)

    def __init__(self, preview_widget=None):
        super().__init__()
        self._br = _Bridge()
        self._br.ended.connect(self.sig_ended, Qt.QueuedConnection)
        self._br.error.connect(self.sig_error, Qt.QueuedConnection)
        self._dk           = None
        self._dk_open      = False
        self._vlc_inst     = None
        self._vlc_player   = None
        self._preview      = preview_widget
        self._volume       = 80
        self._pos_timer    = QTimer()
        self._pos_timer.setInterval(250)
        self._pos_timer.timeout.connect(lambda: self.sig_position.emit(self.get_position()))
        # Slate (tela idle)
        self._slate_active = False
        self._slate_cfg    = {}
        self._slate_loop   = False   # True = loop de slate ativo no DeckLink
        # PAUSE item
        self._pause_timer  = QTimer()
        self._pause_timer.setSingleShot(True)
        self._pause_timer.timeout.connect(lambda: self._br.ended.emit())
        self._init_vlc()

    def set_preview(self, widget):
        self._preview = widget
        if self._vlc_player and widget:
            try: self._vlc_player.set_hwnd(int(widget.winId()))
            except Exception: pass

    def _init_vlc(self):
        if not VLC_OK: return
        try:
            self._vlc_inst   = vlc.Instance('--no-xlib')
            self._vlc_player = self._vlc_inst.media_player_new()
            self._vlc_player.audio_set_mute(True)
            if self._preview:
                self._vlc_player.set_hwnd(int(self._preview.winId()))
        except Exception as e: print(f'[VLC] {e}')

    def _ensure_dk(self):
        """Lazy-init: abre DeckLink só quando for reproduzir."""
        if self._dk_open: return True
        if not DECKLINK_ENGINE_OK: return False
        try:
            self._dk = DeckLinkGraph(
                on_ended=lambda: self._br.ended.emit(),
                on_error=lambda m: self._br.error.emit(m))
            self._dk.open()
            self._dk_open = True
            return True
        except Exception as e:
            self._br.error.emit(f'DeckLink: {e}')
            self._dk = None; return False

    def play(self, item: 'PlaylistItem'):
        self.stop_slate()
        if item.type == ItemType.PAUSE:
            self._play_pause_item(item)
        elif item.type == ItemType.FILE:
            self._play_file(item.filepath)
        elif item.type in (ItemType.SRT_IN, ItemType.RTMP_IN):
            self._play_stream(item.url)
        else:
            self._br.error.emit(f'Stream não implementado: {item.url}')

    def _play_stream(self, url: str):
        """Reproduz stream SRT ou RTMP via FFmpeg/DeckLink + VLC preview."""
        if self._ensure_dk():
            try:
                self._dk.load(url)
                self._dk.play()
                self._dk.set_volume(self._volume)
            except Exception as e:
                self._br.error.emit(str(e)); return
        if self._vlc_player:
            try:
                m = self._vlc_inst.media_new(url)
                # desativa cache de rede para menor latência no preview
                m.add_option(':network-caching=400')
                self._vlc_player.set_media(m)
                self._vlc_player.audio_set_mute(True)
                self._vlc_player.play()
            except Exception: pass
        self._pos_timer.start()

    def _play_pause_item(self, item: 'PlaylistItem'):
        """Item de pausa: aguarda N segundos, exibe slate e emite sig_ended."""
        ms = max(100, int(item.pause_dur * 1000))
        self._pause_timer.start(ms)
        self._pos_timer.start()
        # mostra slate durante a pausa (VLC + DeckLink)
        if self._slate_cfg:
            self._show_slate_dk(self._slate_cfg)

    def _play_file(self, fp: str):
        if self._ensure_dk():
            try:
                self._dk.load(fp); self._dk.play()
                self._dk.set_volume(self._volume)
            except Exception as e:
                self._br.error.emit(str(e)); return
        if self._vlc_player:
            try:
                m = self._vlc_inst.media_new(fp)
                self._vlc_player.set_media(m)
                self._vlc_player.audio_set_mute(True)
                self._vlc_player.play()
            except Exception: pass
        self._pos_timer.start()

    # ── Slate ─────────────────────────────────────────────────────────────────

    def start_slate(self, cfg: dict):
        """Inicia a tela idle (slate) no VLC + DeckLink."""
        if not cfg.get('slate_enabled', False): return
        path = cfg.get('slate_path', '')
        if not path or not Path(path).exists(): return
        self._slate_cfg    = cfg
        self._slate_active = True
        # VLC preview
        self._show_slate_vlc(cfg)
        # DeckLink (apenas para vídeo — imagem estática não tem loop natural)
        self._show_slate_dk(cfg)

    def _show_slate_vlc(self, cfg: dict):
        """Envia slate ao VLC com loop."""
        if not self._vlc_player: return
        path  = cfg.get('slate_path', '')
        stype = cfg.get('slate_type', 'image')
        try:
            if stype == 'image':
                m = self._vlc_inst.media_new(path, 'image-duration=-1')
            else:
                m = self._vlc_inst.media_new(path)
                m.add_option('input-repeat=65535')
            self._vlc_player.set_media(m)
            self._vlc_player.audio_set_mute(True)
            self._vlc_player.play()
        except Exception as e:
            print(f'[Slate VLC] {e}')

    def _show_slate_dk(self, cfg: dict):
        """Envia slate ao DeckLink. Para vídeo: faz loop manual via on_ended."""
        path  = cfg.get('slate_path', '')
        stype = cfg.get('slate_type', 'image')
        # Imagem estática: DeckLink não exibe imagem nativa; usa VLC apenas
        if stype == 'image': return
        if not self._ensure_dk(): return
        if not path or not Path(path).exists(): return
        self._slate_loop = True
        # Substitui temporariamente on_ended para fazer loop
        def _slate_ended():
            if self._slate_loop and self._slate_active:
                try:
                    self._dk.load(path)
                    self._dk.play()
                except Exception: pass
            else:
                # Saiu do slate — emite ended normal apenas se não for loop
                pass
        try:
            # Reconecta callbacks para o loop de slate
            self._dk._on_ended  = _slate_ended
            self._dk._on_error  = lambda m: None
            self._dk.load(path)
            self._dk.play()
        except Exception as e:
            print(f'[Slate DK] {e}')
            self._slate_loop = False

    def stop_slate(self):
        if not self._slate_active: return
        self._slate_active = False
        self._slate_loop   = False
        # Restaura callbacks normais no DeckLink
        if self._dk:
            try:
                self._dk._on_ended = lambda: self._br.ended.emit()
                self._dk._on_error = lambda m: self._br.error.emit(m)
                self._dk._kill_ffmpeg()
            except Exception: pass
        if self._vlc_player:
            try: self._vlc_player.stop()
            except Exception: pass

    def stop(self):
        self._pos_timer.stop()
        self._pause_timer.stop()
        self._slate_loop = False
        if self._dk:
            try: self._dk._kill_ffmpeg()
            except Exception: pass
        if self._vlc_player:
            try: self._vlc_player.stop()
            except Exception: pass
        self._slate_active = False

    def set_volume(self, v: int):
        self._volume = v
        if self._dk:
            try: self._dk.set_volume(v)
            except Exception: pass

    def get_position(self) -> float:
        if self._dk:
            try: return self._dk.get_position()
            except Exception: pass
        return 0.0

    def destroy(self):
        self._pos_timer.stop()
        self._pause_timer.stop()
        if self._dk:
            try: self._dk.destroy()
            except Exception: pass
        if self._vlc_player:
            try: self._vlc_player.stop()
            except Exception: pass


# ══════════════════════════════════════════════════════════════════════════════
# TABELA DA PLAYLIST  — 14 colunas com metadados completos
# ══════════════════════════════════════════════════════════════════════════════

# índices das colunas
(COL_NUM, COL_TYPE, COL_SCHED_START, COL_SCHED_END, COL_TITLE,
 COL_DUR, COL_FPS, COL_RES, COL_CODEC_V, COL_CODEC_A,
 COL_FMT, COL_SIZE, COL_PATH, COL_PAUSE) = range(14)
NCOLS = 14

_WHITE    = QColor('#d0d0d8')
_GREEN    = QColor('#a0ffb0')
_AMBER    = QColor('#f0c060')   # cor para itens PAUSE
_BGPLAY   = QColor('#1a5c2a')
_BGPAUSE  = QColor('#3a3010')   # fundo linha PAUSE


def _fmt_size(b: int) -> str:
    """Formata tamanho de arquivo em KB/MB."""
    if b <= 0: return ''
    if b < 1024*1024: return f'{b/1024:.0f} KB'
    return f'{b/1024/1024:.1f} MB'


class PlaylistTable(QTableWidget):
    row_moved  = pyqtSignal(int, int)
    ctx_action = pyqtSignal(str, int)

    _HEADERS = ['#', '', 'Início', 'Fim', 'Título',
                'Duração', 'FPS', 'Resolução', 'Codec V', 'Áudio',
                'Fmt', 'Tamanho', 'Caminho', '⏸']
    _WIDTHS  = [36, 24, 64, 64, None, 62, 48, 78, 58, 72, 44, 62, None, 24]

    def __init__(self, parent=None):
        super().__init__(0, NCOLS, parent)
        self._drag_row = -1
        self.setHorizontalHeaderLabels(self._HEADERS)
        hh = self.horizontalHeader()
        for col, w in enumerate(self._WIDTHS):
            if w is None:
                if col == COL_TITLE:
                    hh.setSectionResizeMode(col, QHeaderView.Stretch)
                else:
                    hh.setSectionResizeMode(col, QHeaderView.Interactive)
                    self.setColumnWidth(col, 160)
            else:
                hh.setSectionResizeMode(col, QHeaderView.Fixed)
                self.setColumnWidth(col, w)
        self.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.setSelectionMode(QAbstractItemView.ExtendedSelection)
        self.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.setDragDropMode(QAbstractItemView.InternalMove)
        self.setAcceptDrops(True)
        self.setShowGrid(False)
        self.verticalHeader().setVisible(False)
        self.setAlternatingRowColors(False)
        self.setContextMenuPolicy(Qt.CustomContextMenu)
        self.customContextMenuRequested.connect(self._ctx)
        self.horizontalHeader().setMinimumSectionSize(24)

    def _cell(self, text: str, align=Qt.AlignLeft, fg=None) -> QTableWidgetItem:
        it = QTableWidgetItem(text)
        it.setTextAlignment(align | Qt.AlignVCenter)
        it.setForeground(fg or _WHITE)
        it.setFlags(it.flags() & ~Qt.ItemIsEditable)
        return it

    def populate(self, model, cur: int = -1, sched_ref=None):
        from datetime import timedelta
        self.setRowCount(0)
        cum = 0.0
        for i, item in enumerate(model.items()):
            self.insertRow(i)
            is_pause = item.type == ItemType.PAUSE
            is_cur   = (i == cur)
            if is_cur:
                bg = _BGPLAY;  fg = _GREEN
            elif is_pause:
                bg = _BGPAUSE; fg = _AMBER
            else:
                bg = QColor('#18181e') if i%2==0 else QColor('#141419')
                fg = _WHITE

            dur = item.pause_dur if is_pause else item.duration
            start_s = cum
            end_s   = cum + dur
            cum     = end_s

            # horários de relógio
            if sched_ref is not None:
                t_start = sched_ref + timedelta(seconds=start_s)
                t_end   = sched_ref + timedelta(seconds=end_s)
                sched_start_str = t_start.strftime('%H:%M:%S')
                sched_end_str   = t_end.strftime('%H:%M:%S')
            else:
                sched_start_str = '--:--:--'
                sched_end_str   = '--:--:--'

            fps_txt  = f'{item.fps:.2f}' if item.fps > 0 else ''
            res_txt  = f'{item.width}×{item.height}' if item.width else ''
            path_txt = item.filepath or item.url

            cells = [
                self._cell(str(i+1),           Qt.AlignCenter, fg),
                self._cell(item.type_icon(),   Qt.AlignCenter, fg),
                self._cell(sched_start_str,    Qt.AlignCenter, fg),
                self._cell(sched_end_str,      Qt.AlignCenter, fg),
                self._cell(item.title,         Qt.AlignLeft,   fg),
                self._cell(item.display_dur(), Qt.AlignCenter, fg),
                self._cell(fps_txt,            Qt.AlignCenter, fg),
                self._cell(res_txt,            Qt.AlignCenter, fg),
                self._cell(item.codec_v,       Qt.AlignCenter, fg),
                self._cell(item.codec_a,       Qt.AlignCenter, fg),
                self._cell(item.fmt,           Qt.AlignCenter, fg),
                self._cell(_fmt_size(item.file_size), Qt.AlignRight, fg),
                self._cell(Path(path_txt).name if path_txt else '', Qt.AlignLeft, fg),
                self._cell('⏸' if item.pause_after else '', Qt.AlignCenter, fg),
            ]
            for col, cell in enumerate(cells):
                cell.setBackground(bg)
                # tooltip completo no título e caminho
                if col in (COL_TITLE, COL_PATH):
                    cell.setToolTip(path_txt)
                self.setItem(i, col, cell)
            self.setRowHeight(i, 26)

    # drag-and-drop
    def mousePressEvent(self, e):
        if e.button() == Qt.LeftButton: self._drag_row = self.rowAt(e.pos().y())
        super().mousePressEvent(e)
    def dropEvent(self, e):
        to = self.rowAt(e.pos().y())
        if to < 0: to = self.rowCount()-1
        if self._drag_row >= 0 and self._drag_row != to:
            self.row_moved.emit(self._drag_row, to)
        self._drag_row = -1

    def mouseDoubleClickEvent(self, e):
        row = self.rowAt(e.pos().y())
        if row < 0:
            self.ctx_action.emit('add_files_end', -1)
            return
        super().mouseDoubleClickEvent(e)

    def _ctx(self, pos: QPoint):
        row = self.rowAt(pos.y())
        has_sel = bool(self.selectedIndexes())
        m = QMenu(self)

        def a(lbl, name, parent_menu=None):
            pm = parent_menu or m
            act = pm.addAction(lbl)
            act.triggered.connect(lambda: self.ctx_action.emit(name, row))
            return act

        if row >= 0:
            a('▶  Reproduzir agora',    'play_now')
            a('⏭  Reproduzir a seguir', 'play_next')
            m.addSeparator()

        if has_sel:
            a('📋  Copiar', 'copy')
            m.addSeparator()

        if row >= 0:
            sub_before = m.addMenu('➕  Inserir antes')
            a('📋  Colar',           'paste_before',    sub_before)
            a('⏸  Pausa…',          'pause_before',    sub_before)
            a('📡  Stream…',         'stream_before',   sub_before)
            a('▶️  YouTube…',         'yt_before',       sub_before)
            a('📂  Do arquivo…',     'add_files',       sub_before)

            sub_after = m.addMenu('➕  Inserir depois')
            a('📋  Colar',           'paste_after',     sub_after)
            a('⏸  Pausa…',          'pause_after_ins', sub_after)
            a('📡  Stream…',         'stream_after',    sub_after)
            a('▶️  YouTube…',         'yt_after',        sub_after)
            a('📂  Do arquivo…',     'add_files_after', sub_after)

        sub_end = m.addMenu('➕  Adicionar ao fim')
        a('📋  Colar',               'paste_end',       sub_end)
        a('⏸  Pausa…',              'pause_end',       sub_end)
        a('📡  Stream…',             'stream_end',      sub_end)
        a('▶️  YouTube…',             'yt_end',          sub_end)
        a('📂  Do arquivo…',         'add_files_end',   sub_end)

        m.addSeparator()
        if row >= 0:
            a('⬆  Mover para cima',   'move_up')
            a('⬇  Mover para baixo',  'move_down')
            m.addSeparator()
            a('✏  Renomear',          'rename')
            a('⏸  Pausar após este',  'toggle_pause')
            m.addSeparator()
            a('🗑  Remover',           'remove')
        a('✖  Limpar lista', 'clear')
        m.exec_(self.viewport().mapToGlobal(pos))


# ══════════════════════════════════════════════════════════════════════════════
# DIÁLOGOS
# ══════════════════════════════════════════════════════════════════════════════

class AddStreamDialog(QDialog):
    """Diálogo para adicionar entrada SRT ou RTMP à playlist."""
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle('Adicionar stream de entrada')
        self.setMinimumWidth(480)
        layout = QVBoxLayout(self)

        form = QFormLayout()

        self.cb_type = QComboBox()
        self.cb_type.addItems(['SRT', 'RTMP'])
        self.cb_type.currentTextChanged.connect(self._on_type_change)
        form.addRow('Protocolo:', self.cb_type)

        self.le_host = QLineEdit()
        self.le_host.setPlaceholderText('192.168.1.100  ou  meu.servidor.com')
        form.addRow('Host:', self.le_host)

        self.le_port = QLineEdit('9000')
        self.le_port.setPlaceholderText('9000')
        self.le_port.setFixedWidth(80)
        form.addRow('Porta:', self.le_port)

        # campo de caminho — visível para RTMP (ex: /live/stream_key)
        self.le_path = QLineEdit()
        self.le_path.setPlaceholderText('/live/chave_stream')
        self._lbl_path = QLabel('Caminho:')
        form.addRow(self._lbl_path, self.le_path)

        # latência SRT (ms)
        self.spin_latency = QSpinBox()
        self.spin_latency.setRange(20, 10000); self.spin_latency.setValue(200)
        self.spin_latency.setSuffix(' ms')
        self.spin_latency.setFixedWidth(90)
        self._lbl_latency = QLabel('Latência SRT:')
        form.addRow(self._lbl_latency, self.spin_latency)

        self.le_title = QLineEdit()
        self.le_title.setPlaceholderText('Nome exibido na playlist (opcional)')
        form.addRow('Título:', self.le_title)

        layout.addLayout(form)

        # URL preview
        self._lbl_url = QLabel()
        self._lbl_url.setStyleSheet('color:#8888a0;font-size:9pt;padding:4px;'
                                    'background:#111118;border-radius:3px;')
        self._lbl_url.setWordWrap(True)
        layout.addWidget(self._lbl_url)

        # conecta campos para atualizar preview
        for w in (self.le_host, self.le_port, self.le_path):
            w.textChanged.connect(self._update_url_preview)
        self.spin_latency.valueChanged.connect(self._update_url_preview)
        self._on_type_change(self.cb_type.currentText())  # estado inicial

        bb = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        bb.accepted.connect(self._validate_and_accept)
        bb.rejected.connect(self.reject)
        layout.addWidget(bb)

    def _on_type_change(self, t: str):
        is_rtmp = (t == 'RTMP')
        self._lbl_path.setVisible(is_rtmp)
        self.le_path.setVisible(is_rtmp)
        self._lbl_latency.setVisible(not is_rtmp)
        self.spin_latency.setVisible(not is_rtmp)
        if not is_rtmp:
            self.le_port.setText(self.le_port.text() or '9000')
        else:
            self.le_port.setText(self.le_port.text() or '1935')
        self._update_url_preview()

    def _build_url(self) -> str:
        host = self.le_host.text().strip()
        port = self.le_port.text().strip()
        t    = self.cb_type.currentText()
        if not host: return ''
        if t == 'SRT':
            lat  = self.spin_latency.value()
            return f'srt://{host}:{port}?latency={lat}'
        else:
            path = self.le_path.text().strip().lstrip('/')
            return f'rtmp://{host}:{port}/{path}'

    def _update_url_preview(self):
        url = self._build_url()
        self._lbl_url.setText(f'URL: {url}' if url else 'URL: (preencha host e porta)')

    def _validate_and_accept(self):
        if not self.le_host.text().strip():
            QMessageBox.warning(self, 'Campo obrigatório', 'Informe o host/IP do stream.'); return
        if not self.le_port.text().strip().isdigit():
            QMessageBox.warning(self, 'Porta inválida', 'A porta deve ser um número.'); return
        self.accept()

    def get_item(self) -> 'PlaylistItem | None':
        url = self._build_url()
        if not url: return None
        t    = self.cb_type.currentText()
        itype = ItemType.SRT_IN if t == 'SRT' else ItemType.RTMP_IN
        title = self.le_title.text().strip() or url
        return PlaylistItem.from_stream(url, itype, title)


class OutputStreamDialog(QDialog):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle('Enviar saída para stream')
        self.setMinimumWidth(460)
        layout = QVBoxLayout(self)
        form = QFormLayout()
        self.cb_type = QComboBox()
        self.cb_type.addItems(['SRT', 'YouTube / RTMP'])
        self.le_url = QLineEdit()
        self.le_url.setPlaceholderText('srt://destino:porta  |  rtmp://a.rtmp.youtube.com/live2/CHAVE')
        form.addRow('Tipo:', self.cb_type)
        form.addRow('Destino:', self.le_url)
        layout.addLayout(form)
        warn = QLabel('⚠  Envio de stream ainda não implementado.')
        warn.setStyleSheet('color:#e0a040; font-size:11px;')
        layout.addWidget(warn)
        bb = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        bb.accepted.connect(self.accept); bb.rejected.connect(self.reject)
        layout.addWidget(bb)

    def get_config(self):
        return {'type': self.cb_type.currentText(), 'url': self.le_url.text().strip()}


# ══════════════════════════════════════════════════════════════════════════════
# DIÁLOGO DE CONFIGURAÇÃO DE SAÍDA
# ══════════════════════════════════════════════════════════════════════════════

def _enum_audio_devices() -> list[tuple[str,str]]:
    """Retorna [(id, nome)] dos dispositivos de áudio WASAPI via PowerShell."""
    devices = [('', 'Padrão do sistema')]
    try:
        cmd = (
            'Get-WmiObject Win32_PnPEntity | '
            'Where-Object { $_.DeviceID -like "SWD\\\\MMDEVAPI\\\\{0.0.0*" } | '
            'Select-Object Name, DeviceID | ConvertTo-Json'
        )
        out = subprocess.check_output(
            ['powershell', '-NoProfile', '-Command', cmd],
            stderr=subprocess.DEVNULL, timeout=5)
        items = json.loads(out)
        if isinstance(items, dict): items = [items]
        for it in items:
            name = it.get('Name','')
            did  = it.get('DeviceID','')
            if name and did:
                # extrair GUID do DeviceID
                guid_part = did.split('\\')[-1] if '\\' in did else did
                devices.append(('{' + guid_part + '}', name))
    except Exception:
        pass
    return devices

def _enum_screens() -> list[tuple[int,str]]:
    """Retorna [(índice, nome)] das telas Qt disponíveis."""
    screens = []
    for i, s in enumerate(QApplication.screens()):
        geo = s.geometry()
        screens.append((i, f'Monitor {i+1} — {s.name()} {geo.width()}×{geo.height()} @ {geo.x()},{geo.y()}'))
    return screens


class OutputConfigDialog(QDialog):
    """Configuração de saída: DeckLink / PC / Tela externa."""
    def __init__(self, cfg: dict, parent=None):
        super().__init__(parent)
        self.setWindowTitle('Configuração de saída')
        self.setMinimumWidth(520)
        layout = QVBoxLayout(self)

        # ── modo de vídeo ──────────────────────────────────────────────────────
        grp_vid = QGroupBox('Saída de vídeo')
        fv = QFormLayout(grp_vid)
        self.cb_mode = QComboBox()
        self.cb_mode.addItems(['DeckLink (SDI)', 'Monitor externo (janela fullscreen)', 'Monitor principal (preview)'])
        mode_map = {'decklink': 0, 'pc_ext': 1, 'pc': 2}
        self.cb_mode.setCurrentIndex(mode_map.get(cfg.get('output_mode','pc'), 2))
        fv.addRow('Modo:', self.cb_mode)

        self.cb_screen = QComboBox()
        self._screens = _enum_screens()
        for _, name in self._screens:
            self.cb_screen.addItem(name)
        cur_scr = cfg.get('video_screen', 0)
        if cur_scr < self.cb_screen.count():
            self.cb_screen.setCurrentIndex(cur_scr)
        fv.addRow('Tela:', self.cb_screen)
        layout.addWidget(grp_vid)

        # ── modo de áudio ──────────────────────────────────────────────────────
        grp_aud = QGroupBox('Saída de áudio')
        fa = QFormLayout(grp_aud)
        self.cb_audio = QComboBox()
        self._audio_devices = _enum_audio_devices()
        for _, name in self._audio_devices:
            self.cb_audio.addItem(name)
        cur_aud = cfg.get('audio_device', '')
        for i, (did, _) in enumerate(self._audio_devices):
            if did == cur_aud:
                self.cb_audio.setCurrentIndex(i); break
        fa.addRow('Dispositivo:', self.cb_audio)
        layout.addWidget(grp_aud)

        # ── info DeckLink ──────────────────────────────────────────────────────
        dk_txt = '✅ Motor DeckLink disponível' if DECKLINK_ENGINE_OK else '❌ Motor DeckLink não disponível (decklink_out.py não encontrado)'
        lbl_dk = QLabel(dk_txt)
        lbl_dk.setStyleSheet('font-size:11px; color:#aaa;')
        layout.addWidget(lbl_dk)

        lbl_note = QLabel('Nota: o DeckLink só pode ser usado por um programa por vez.\nSe o Unimedia estiver aberto, feche-o antes de reproduzir.')
        lbl_note.setStyleSheet('font-size:11px; color:#e0a040;')
        lbl_note.setWordWrap(True)
        layout.addWidget(lbl_note)

        bb = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        bb.accepted.connect(self.accept); bb.rejected.connect(self.reject)
        layout.addWidget(bb)

    def get_config(self) -> dict:
        mode_map = {0: 'decklink', 1: 'pc_ext', 2: 'pc'}
        scr_idx  = self.cb_screen.currentIndex()
        aud_idx  = self.cb_audio.currentIndex()
        return {
            'output_mode':       mode_map[self.cb_mode.currentIndex()],
            'video_screen':      self._screens[scr_idx][0] if scr_idx < len(self._screens) else 0,
            'video_screen_name': self._screens[scr_idx][1] if scr_idx < len(self._screens) else '',
            'audio_device':      self._audio_devices[aud_idx][0] if aud_idx < len(self._audio_devices) else '',
            'audio_name':        self._audio_devices[aud_idx][1] if aud_idx < len(self._audio_devices) else '',
        }


# ══════════════════════════════════════════════════════════════════════════════
# DIÁLOGO DE CONFIGURAÇÕES GERAIS  (Slate / Idle screen)
# ══════════════════════════════════════════════════════════════════════════════

class SettingsDialog(QDialog):
    """Configurações gerais do TVV Playout: tela idle, comportamento, etc."""
    def __init__(self, cfg: dict, parent=None):
        super().__init__(parent)
        self.setWindowTitle('Configurações')
        self.setMinimumWidth(520)
        layout = QVBoxLayout(self)
        tabs = QTabWidget()
        layout.addWidget(tabs)

        # ── Aba: Tela Idle (Slate) ──────────────────────────────────────────
        tab_slate = QWidget()
        vb = QVBoxLayout(tab_slate)

        grp_en = QGroupBox('Tela idle (quando nenhum VT está tocando)')
        fl = QFormLayout(grp_en)

        self.chk_slate = QCheckBox('Ativar tela idle')
        self.chk_slate.setChecked(cfg.get('slate_enabled', False))
        fl.addRow('', self.chk_slate)

        self.grp_type = QButtonGroup(self)
        self.rb_img = QRadioButton('Imagem estática')
        self.rb_vid = QRadioButton('Vídeo em loop')
        self.grp_type.addButton(self.rb_img, 0)
        self.grp_type.addButton(self.rb_vid, 1)
        is_vid = cfg.get('slate_type','image') == 'video'
        self.rb_vid.setChecked(is_vid)
        self.rb_img.setChecked(not is_vid)
        row_type = QHBoxLayout()
        row_type.addWidget(self.rb_img); row_type.addWidget(self.rb_vid); row_type.addStretch()
        fl.addRow('Tipo:', row_type)

        self.le_slate_path = QLineEdit(cfg.get('slate_path',''))
        self.le_slate_path.setPlaceholderText('Caminho da imagem ou vídeo…')
        btn_browse = QPushButton('…'); btn_browse.setFixedWidth(32)
        btn_browse.clicked.connect(self._browse_slate)
        row_path = QHBoxLayout()
        row_path.addWidget(self.le_slate_path); row_path.addWidget(btn_browse)
        fl.addRow('Arquivo:', row_path)

        note = QLabel('A tela idle é exibida automaticamente no preview (VLC) quando\n'
                      'o playout para. Para DeckLink, um vídeo em loop é recomendado.')
        note.setStyleSheet('color:#8888a0; font-size:9pt;')
        note.setWordWrap(True)
        vb.addWidget(grp_en)
        vb.addWidget(note)
        vb.addStretch()
        tabs.addTab(tab_slate, '🎬 Tela Idle')

        # ── Aba: Playlist ────────────────────────────────────────────────────
        tab_pl = QWidget()
        vb2 = QVBoxLayout(tab_pl)
        grp_pl = QGroupBox('Comportamento da playlist')
        fl2 = QFormLayout(grp_pl)

        self.chk_loop = QCheckBox('Repetir lista ao terminar')
        self.chk_loop.setChecked(cfg.get('playlist_loop', False))
        fl2.addRow('', self.chk_loop)

        self.chk_autoplay = QCheckBox('Iniciar reprodução automática ao abrir')
        self.chk_autoplay.setChecked(cfg.get('autoplay', False))
        fl2.addRow('', self.chk_autoplay)

        vb2.addWidget(grp_pl); vb2.addStretch()
        tabs.addTab(tab_pl, '📋 Playlist')

        bb = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        bb.accepted.connect(self.accept); bb.rejected.connect(self.reject)
        layout.addWidget(bb)

    def _browse_slate(self):
        if self.rb_vid.isChecked():
            fp, _ = QFileDialog.getOpenFileName(self, 'Selecionar vídeo slate', '',
                'Vídeo (*.mp4 *.mov *.mxf *.avi *.mkv *.ts);;Todos (*.*)')
        else:
            fp, _ = QFileDialog.getOpenFileName(self, 'Selecionar imagem slate', '',
                'Imagem (*.jpg *.jpeg *.png *.bmp *.tiff);;Todos (*.*)')
        if fp: self.le_slate_path.setText(fp)

    def get_config(self) -> dict:
        return {
            'slate_enabled': self.chk_slate.isChecked(),
            'slate_type':    'video' if self.rb_vid.isChecked() else 'image',
            'slate_path':    self.le_slate_path.text().strip(),
            'playlist_loop': self.chk_loop.isChecked(),
            'autoplay':      self.chk_autoplay.isChecked(),
        }


# ══════════════════════════════════════════════════════════════════════════════
# JANELA DO PLAYER  (preview + transporte + tempos)
# ══════════════════════════════════════════════════════════════════════════════

THEME = """
QWidget          { background:#16161c; color:#d0d0d8; font-family:'Segoe UI'; font-size:9pt; }
QMainWindow      { background:#16161c; }
QGroupBox        { border:1px solid #2a2a38; border-radius:4px; margin-top:8px; padding-top:6px; }
QGroupBox::title { subcontrol-origin:margin; left:8px; color:#8888a0; font-size:8pt; }
QPushButton      { background:#22222e; color:#c8c8d8; border:1px solid #333342;
                   border-radius:3px; padding:4px 12px; }
QPushButton:hover   { background:#2e2e3e; border-color:#4a4a60; }
QPushButton:pressed { background:#1a1a26; }
QPushButton:disabled{ color:#444455; border-color:#222230; }
QSlider::groove:horizontal { background:#252532; height:5px; border-radius:2px; }
QSlider::handle:horizontal { background:#3a7a50; width:13px; height:13px;
                              margin:-4px 0; border-radius:6px; }
QSlider::sub-page:horizontal { background:#2a5a3c; border-radius:2px; }
QLabel           { color:#c8c8d0; }
QStatusBar       { background:#111118; color:#606070; font-size:8pt; }
QMenuBar         { background:#111118; color:#c0c0d0; }
QMenuBar::item:selected { background:#22222e; }
QMenu            { background:#1c1c26; color:#c8c8d0; border:1px solid #2a2a3a; }
QMenu::item:selected { background:#1a4a2a; }
QToolBar         { background:#111118; border-bottom:1px solid #222230; spacing:4px; }
QComboBox        { background:#1e1e2a; color:#c0c0d0; border:1px solid #333342;
                   border-radius:3px; padding:2px 6px; }
QComboBox QAbstractItemView { background:#1c1c28; color:#c0c0d0;
                               selection-background-color:#1a4a2a; }
QLineEdit        { background:#1e1e2a; color:#c0c0d0; border:1px solid #333342;
                   border-radius:3px; padding:3px 6px; }
QTableWidget     { background:#141420; color:#d0d0dc; gridline-color:#1e1e2c;
                   selection-background-color:#1a4a2a; selection-color:#b0ffb8;
                   border:none; alternate-background-color:#17171e; }
QTableWidget QHeaderView::section { background:#111118; color:#787890;
                   border:none; border-bottom:1px solid #222232;
                   padding:3px 4px; font-size:8pt; }
QScrollBar:vertical { background:#111118; width:10px; }
QScrollBar::handle:vertical { background:#2a2a3a; border-radius:4px; min-height:20px; }
QSplitter::handle { background:#222230; }
"""

#---------------------------VU METER-----------------------------------------------------
class VuMeter(QWidget):
    def __init__(self):
        super().__init__()
        self.level = 0
        self.setMinimumHeight(18)

    def setLevel(self, v):
        self.level = max(0, min(100, v))
        self.update()

    def paintEvent(self, event):
        p = QPainter(self)
        r = self.rect()

        # fundo
        p.fillRect(r, QColor("#101018"))

        # largura proporcional
        w = int(r.width() * (self.level / 100))
        bar = r.adjusted(0, 0, -(r.width() - w), 0)

        # cor por nível
        if self.level < 60:
            color = QColor("#00d060")
        elif self.level < 85:
            color = QColor("#e0c020")
        else:
            color = QColor("#e03030")

        p.fillRect(bar, color)
#----------------------------------------------------------------------------------------------------

class PlayerWindow(QMainWindow):
    """Janela do player: preview + controles de transporte."""

    request_output_config = pyqtSignal()
    request_output_stream = pyqtSignal()
    def _update_vu(self):
        try:
            if not hasattr(self._engine, "_player"):
                return

            p = self._engine._player

            if p is None:
                return

            # volume atual
            vol = p.audio_get_volume()

            if vol >= 0:
                self._vu.setLevel(vol)

        except:
            pass

    def __init__(self, engine: PlayerEngine, model: PlaylistModel):
        super().__init__()
        self.setWindowTitle('TVV Playout — Player')
        self.resize(700, 560)
        self.setMinimumWidth(580)
        self._engine = engine
        self._model  = model
        self._cur_idx = -1
        self._running = False
        self._dur     = 0.0
        self._elapsed_list = 0.0

        self._build_ui()
        self._vu_timer = QTimer()
        self._vu_timer.setInterval(50)
        self._vu_timer.timeout.connect(self._update_vu)
        self._vu_timer.start()
        self._engine.sig_position.connect(self._on_position)
        self._engine.sig_ended.connect(self._on_ended)
        self._engine.sig_error.connect(self._on_error)

    def _build_ui(self):
        from PyQt5.QtWidgets import QSizePolicy as _SP
        cw = QWidget(); self.setCentralWidget(cw)
        vb = QVBoxLayout(cw); vb.setContentsMargins(8,8,8,6); vb.setSpacing(6)

        # ══ Faixa superior: preview (esq, expansível) + painel info (dir) ════
        top = QHBoxLayout(); top.setContentsMargins(0,0,0,0); top.setSpacing(8)

        # ── Preview + knob de volume ──────────────────────────────────────────
        prev_col = QVBoxLayout(); prev_col.setContentsMargins(0,0,0,0); prev_col.setSpacing(4)

        self._preview = QFrame()
        self._preview.setMinimumSize(300, 170)
        self._preview.setSizePolicy(_SP.Expanding, _SP.Expanding)
        self._preview.setStyleSheet('background:#000; border:1px solid #2a2a40;')
        prev_col.addWidget(self._preview, stretch=1)
        self._engine.set_preview(self._preview)

        # ── Knob de volume (QDial) ────────────────────────────────────────────
        box_vol = QGroupBox('Volume')
        box_vol.setStyleSheet(
            'QGroupBox{border:1px solid #2a2a3e;border-radius:5px;'
            'margin-top:10px;padding:4px 6px 4px 6px;}'
            'QGroupBox::title{subcontrol-origin:margin;left:8px;'
            'color:#50506a;font-size:8pt;}')

        vol_row = QHBoxLayout(box_vol)
        vol_row.setContentsMargins(4,4,4,4)
        vol_row.setSpacing(8)

        # VU meter
        self._vu = VuMeter()
        self._vu.setFixedHeight(16)

        self._lbl_vol_dial = QLabel('80%')
        self._lbl_vol_dial.setFixedWidth(32)
        self._lbl_vol_dial.setAlignment(Qt.AlignCenter)
        self._lbl_vol_dial.setStyleSheet(
            'font-size:9pt;color:#8090a8;font-weight:bold;'
        )

        vol_row.addStretch()
        vol_row.addWidget(self._vu)
        vol_row.addWidget(self._lbl_vol_dial)
        vol_row.addStretch()

        prev_col.addWidget(box_vol)
        top.addLayout(prev_col, stretch=3)

        # ── Painel direito: 3 QGroupBoxes ────────────────────────────────────
        right = QVBoxLayout(); right.setContentsMargins(0,0,0,0); right.setSpacing(6)

        # ── Box Hardware (CPU/GPU) ─────────────────────────────────────────────
        _BOX = ('QGroupBox{border:1px solid #2a2a3e;border-radius:5px;'
                'margin-top:10px;padding:6px 8px 4px 8px;}'
                'QGroupBox::title{subcontrol-origin:margin;left:8px;'
                'color:#50506a;font-size:8pt;}')
        box_hw = QGroupBox('Hardware'); box_hw.setFixedWidth(200)
        box_hw.setStyleSheet(_BOX)
        vb_hw = QVBoxLayout(box_hw); vb_hw.setContentsMargins(4,4,4,4); vb_hw.setSpacing(5)
        def _hw_row(label, bar_color, lbl_color):
            r = QHBoxLayout(); r.setSpacing(5)
            lh = QLabel(label); lh.setFixedWidth(26)
            lh.setStyleSheet(f'font-size:8pt;color:{lbl_color};')
            bar = QProgressBar(); bar.setRange(0,100); bar.setValue(0)
            bar.setFixedHeight(10); bar.setTextVisible(False)
            bar.setStyleSheet(
                f'QProgressBar{{background:#1a1a24;border:none;border-radius:4px;}}'
                f'QProgressBar::chunk{{background:{bar_color};border-radius:4px;}}')
            lv = QLabel('--%'); lv.setFixedWidth(32)
            lv.setStyleSheet(f'font-size:8pt;color:{lbl_color};')
            r.addWidget(lh); r.addWidget(bar,1); r.addWidget(lv)
            return r, bar, lv
        row_cpu, self._bar_cpu, self._lbl_cpu = _hw_row('CPU','#2a5a7a','#6080a0')
        row_gpu, self._bar_gpu, self._lbl_gpu = _hw_row('GPU','#2a6a3a','#608060')
        vb_hw.addLayout(row_cpu); vb_hw.addLayout(row_gpu)
        right.addWidget(box_hw)

        # ── Box Relógio ────────────────────────────────────────────────────────
        box_clk = QGroupBox('Horário'); box_clk.setStyleSheet(_BOX)
        vb_clk = QVBoxLayout(box_clk); vb_clk.setContentsMargins(4,2,4,4); vb_clk.setSpacing(1)
        self._lbl_clock = QLabel('00:00:00')
        self._lbl_clock.setAlignment(Qt.AlignCenter)
        self._lbl_clock.setStyleSheet(
            'font-size:26pt;font-weight:bold;color:#c0e0ff;font-family:"Segoe UI Semibold","Segoe UI";')
        self._lbl_date = QLabel('')
        self._lbl_date.setAlignment(Qt.AlignCenter)
        self._lbl_date.setStyleSheet('font-size:8pt;color:#505062;')
        vb_clk.addWidget(self._lbl_clock); vb_clk.addWidget(self._lbl_date)
        right.addWidget(box_clk)

        # ── Box Próximo VT ─────────────────────────────────────────────────────
        box_nxt = QGroupBox('Próximo'); box_nxt.setStyleSheet(_BOX)
        vb_nxt = QVBoxLayout(box_nxt); vb_nxt.setContentsMargins(4,2,4,4); vb_nxt.setSpacing(2)
        self._lbl_next_title = QLabel('—')
        self._lbl_next_title.setWordWrap(True)
        self._lbl_next_title.setStyleSheet('font-size:9pt;color:#9090b0;')
        self._lbl_next_meta = QLabel('')
        self._lbl_next_meta.setWordWrap(True)
        self._lbl_next_meta.setStyleSheet('font-size:8pt;color:#505062;')
        vb_nxt.addWidget(self._lbl_next_title); vb_nxt.addWidget(self._lbl_next_meta)
        right.addWidget(box_nxt)

        right.addStretch()
        top.addLayout(right, stretch=0)
        vb.addLayout(top, stretch=1)

        # ══ Agora reproduzindo ════════════════════════════════════════════════
        self._lbl_now = QLabel('— Parado —')
        self._lbl_now.setAlignment(Qt.AlignCenter)
        self._lbl_now.setStyleSheet('font-weight:bold; font-size:10pt; color:#e8e8f0;')
        self._lbl_now.setWordWrap(True)
        self._lbl_now.setFixedHeight(32)
        vb.addWidget(self._lbl_now)

        # ── timeline VT ───────────────────────────────────────────────────────
        self._prog_vt = QSlider(Qt.Horizontal)
        self._prog_vt.setRange(0, 1000)
        self._prog_vt.setEnabled(False)
        self._prog_vt.setFixedHeight(12)
        vb.addWidget(self._prog_vt)

        # ── tempos VT ─────────────────────────────────────────────────────────
        row_t = QHBoxLayout(); row_t.setContentsMargins(0,0,0,0)
        self._lbl_pos = QLabel('00:00:00')
        self._lbl_pos.setStyleSheet('font-size:12pt; font-weight:bold; color:#80e090;')
        self._lbl_dur = QLabel('/ 00:00:00')
        self._lbl_dur.setStyleSheet('font-size:10pt; color:#606070;')
        self._lbl_rem = QLabel('resta 00:00:00')
        self._lbl_rem.setStyleSheet('font-size:9pt; color:#505060;')
        row_t.addWidget(self._lbl_pos); row_t.addWidget(self._lbl_dur)
        row_t.addStretch(); row_t.addWidget(self._lbl_rem)
        vb.addLayout(row_t)

        # ── tempos da lista ────────────────────────────────────────────────────
        row_l = QHBoxLayout(); row_l.setContentsMargins(0,0,0,0)
        self._lbl_list_elapsed = QLabel('Lista: 00:00:00 decorrido')
        self._lbl_list_elapsed.setStyleSheet('font-size:8pt; color:#506060;')
        self._lbl_list_total = QLabel('total 00:00:00')
        self._lbl_list_total.setStyleSheet('font-size:8pt; color:#404050;')
        row_l.addWidget(self._lbl_list_elapsed); row_l.addStretch(); row_l.addWidget(self._lbl_list_total)
        vb.addLayout(row_l)

        # ── volume ─────────────────────────────────────────────────────────────
        row_v = QHBoxLayout(); row_v.setContentsMargins(0,0,0,0)
        lbl_v = QLabel('Vol:'); lbl_v.setFixedWidth(28)
        self._sld_vol = QSlider(Qt.Horizontal)
        self._sld_vol.setRange(0,100); self._sld_vol.setValue(80)
        self._sld_vol.setFixedHeight(12)
        self._lbl_vol_pct = QLabel('80%')
        self._lbl_vol_pct.setFixedWidth(34)
        self._lbl_vol_pct.setStyleSheet('color:#808090; font-size:9pt;')
        self._sld_vol.valueChanged.connect(self._on_volume)
        row_v.addWidget(lbl_v); row_v.addWidget(self._sld_vol); row_v.addWidget(self._lbl_vol_pct)
        vb.addLayout(row_v)

        # ── transporte ─────────────────────────────────────────────────────────
        row_tr = QHBoxLayout(); row_tr.setContentsMargins(0,0,0,0); row_tr.setSpacing(4)
        def tbtn(label, tip, fn, w=None):
            b = QPushButton(label); b.setToolTip(tip); b.setFixedHeight(32)
            if w: b.setFixedWidth(w)
            b.clicked.connect(fn); row_tr.addWidget(b); return b
        self._btn_play  = tbtn('▶  Play',    'Iniciar sequência',       self.cmd_play,    80)
        self._btn_stop  = tbtn('⏹  Stop',    'Parar reprodução',        self.cmd_stop,    80)
        self._btn_pause = tbtn('⏸  Pausa',   'Pausar/continuar',        self.cmd_pause,   80)
        self._btn_next  = tbtn('⏭  Avançar', 'Avançar para próximo VT', self.cmd_advance, 90)
        vb.addLayout(row_tr)

        # ── barra de saída ──────────────────────────────────────────────────────
        row_out = QHBoxLayout(); row_out.setContentsMargins(0,0,0,0)
        self._lbl_out = QLabel('Saída: —')
        self._lbl_out.setStyleSheet('font-size:8pt; color:#606070;')
        btn_cfg = QPushButton('⚙ Saída'); btn_cfg.setFixedHeight(22)
        btn_cfg.clicked.connect(self.request_output_config)
        btn_stream = QPushButton('📤 Stream'); btn_stream.setFixedHeight(22)
        btn_stream.clicked.connect(self.request_output_stream)
        row_out.addWidget(self._lbl_out); row_out.addStretch()
        row_out.addWidget(btn_cfg); row_out.addWidget(btn_stream)
        vb.addLayout(row_out)

        # ── iniciar monitor de HW + relógio ────────────────────────────────────
        self._sysmon = SysMonWorker()
        self._sysmon.stats.connect(self._on_sysmon)
        self._sysmon.start()

        self._clock_timer = QTimer()
        self._clock_timer.setInterval(1000)
        self._clock_timer.timeout.connect(self._tick_clock)
        self._clock_timer.start()
        self._tick_clock()   # primeira atualização imediata

        self._status = QStatusBar(); self.setStatusBar(self._status)
        dk = '🟢 DeckLink' if DECKLINK_ENGINE_OK else '🔴 DeckLink indisponível'
        self._status.showMessage(dk + ('  |  🟢 VLC' if VLC_OK else '  |  🔴 VLC indisponível'))


    # ── lógica do player ───────────────────────────────────────────────────────

    def _tick_clock(self):
        now = datetime.now()
        self._lbl_clock.setText(now.strftime('%H:%M:%S'))
        # Dia da semana em português
        dias = ['Segunda','Terça','Quarta','Quinta','Sexta','Sábado','Domingo']
        self._lbl_date.setText(f'{dias[now.weekday()]}, {now.day:02d}/{now.month:02d}/{now.year}')

    def set_current(self, idx: int):
        """Chamado pela PlaylistWindow ao mudar o item atual."""
        self._cur_idx = idx
        self._elapsed_list = self._calc_elapsed_before(idx)
        if 0 <= idx < len(self._model):
            item = self._model[idx]
            self._dur = item.duration
            self._lbl_now.setText(item.title)
            self._lbl_dur.setText('/ ' + item.display_dur())
            self._prog_vt.setRange(0, max(1, int(self._dur * 10)))
            self._prog_vt.setValue(0)
            self._update_list_labels(0.0)
        else:
            self._lbl_now.setText('— Parado —')
            self._dur = 0.0
        # Próximo VT
        nxt_idx = idx + 1 if idx >= 0 else -1
        if 0 <= nxt_idx < len(self._model):
            nxt = self._model[nxt_idx]
            icon = nxt.type_icon()
            self._lbl_next_title.setText(f'{icon}  {nxt.title}')
            parts = []
            if nxt.type == ItemType.PAUSE:
                parts.append(f'pausa de {nxt.pause_dur:.0f}s')
            elif nxt.type == ItemType.FILE:
                if nxt.duration > 0: parts.append(nxt.display_dur())
                if nxt.fps > 0:      parts.append(f'{nxt.fps:.2f}fps')
                if nxt.width:        parts.append(f'{nxt.width}×{nxt.height}')
                if nxt.codec_v:      parts.append(nxt.codec_v)
            elif nxt.type in (ItemType.SRT_IN, ItemType.RTMP_IN):
                parts.append(nxt.url or nxt.type.name)
            self._lbl_next_meta.setText('  '.join(parts))
        else:
            self._lbl_next_title.setText('— fim da lista —')
            self._lbl_next_meta.setText('')

    def _calc_elapsed_before(self, idx: int) -> float:
        return sum(self._model[i].duration for i in range(min(idx, len(self._model)))
                   if self._model[i].duration > 0)

    def _on_position(self, pos: float):
        self._lbl_pos.setText(_fmt_time(pos))
        rem = max(0.0, self._dur - pos)
        self._lbl_rem.setText(f'resta {_fmt_time(rem)}')
        if self._dur > 0:
            self._prog_vt.setValue(int(pos / self._dur * self._prog_vt.maximum()))
        self._update_list_labels(pos)

    def _update_list_labels(self, vt_pos: float):
        elapsed = self._elapsed_list + vt_pos
        total   = self._model.total_dur()
        self._lbl_list_elapsed.setText(f'Lista: {_fmt_time(elapsed)} decorrido')
        self._lbl_list_total.setText(f'total {_fmt_time(total)}')

    def _on_volume(self, v: int):
        self._lbl_vol_pct.setText(f'{v}%')
        self._lbl_vol_dial.setText(f'{v}%')

        # atualiza VU meter
        self._vu.setLevel(v)

        # envia volume para engine
        self._engine.set_volume(v)

  

    def _on_ended(self):
        # sinaliza para a PlaylistWindow avançar
        pass   # PlaylistWindow conecta direto ao engine.sig_ended

    def _on_error(self, msg: str):
        self._status.showMessage(f'⚠ {msg}', 8000)

    def _on_sysmon(self, cpu: float, gpu: float):
        if cpu >= 0:
            self._lbl_cpu.setText(f'{cpu:.0f}%')
            self._bar_cpu.setValue(int(cpu))
            color = '#7a2a2a' if cpu > 80 else '#4a6a8a' if cpu > 50 else '#2a5a7a'
            self._bar_cpu.setStyleSheet(
                f'QProgressBar{{background:#1a1a24;border:none;border-radius:4px;}}'
                f'QProgressBar::chunk{{background:{color};border-radius:4px;}}')
        else:
            self._lbl_cpu.setText('n/d')
        if gpu >= 0:
            self._lbl_gpu.setText(f'{gpu:.0f}%')
            self._bar_gpu.setValue(int(gpu))
            color = '#7a4a2a' if gpu > 80 else '#4a7a4a' if gpu > 50 else '#2a6a3a'
            self._bar_gpu.setStyleSheet(
                f'QProgressBar{{background:#1a1a24;border:none;border-radius:4px;}}'
                f'QProgressBar::chunk{{background:{color};border-radius:4px;}}')
        else:
            self._lbl_gpu.setText('n/d')

    def update_output_label(self, cfg: dict):
        mode = {'decklink':'DeckLink SDI', 'pc_ext':'Monitor externo', 'pc':'Monitor principal'}
        self._lbl_out.setText(f"Saída: {mode.get(cfg.get('output_mode','pc'),'?')}  |  "
                              f"Áudio: {cfg.get('audio_name','padrão')}")

    def cmd_play(self):
        # delegado para PlaylistWindow via sinal
        self._btn_play.setEnabled(False)
        self._btn_play.setEnabled(True)

    def cmd_stop(self):   pass   # conectado externamente
    def cmd_pause(self):  pass
    def cmd_advance(self): pass

    def set_running(self, v: bool):
        self._running = v
        self._btn_play.setEnabled(not v)
        self._btn_stop.setEnabled(v)
        self._btn_pause.setEnabled(v)
        self._btn_next.setEnabled(v)


# ══════════════════════════════════════════════════════════════════════════════
# JANELA DA PLAYLIST
# ══════════════════════════════════════════════════════════════════════════════

class PlaylistWindow(QMainWindow):
    """Janela da lista de VTs com toolbar e botões de operação."""

    def __init__(self, engine: PlayerEngine, model: PlaylistModel,
                 player_win: PlayerWindow):
        super().__init__()
        self.setWindowTitle('TVV Playout — Lista')
        self.resize(1100, 580)
        self._engine     = engine
        self._model      = model
        self._player_win = player_win
        self._cur_idx    = -1
        self._nxt_idx    = -1
        self._running    = False
        self._ignore_ended = False   # guard contra sig_ended fantasma do stop()
        self._sched_ref  = None      # datetime de referência para horários da lista
        self._probes     : list[ProbeWorker] = []
        self._cfg        = self._load_cfg()

        self._model.changed.connect(self._refresh)
        self._engine.sig_ended.connect(self._on_ended)
        self._engine.sig_error.connect(self._on_error)
        self._clipboard : list[PlaylistItem] = []   # clipboard interno de VTs

        self._build_ui()
        
        self._apply_cfg_to_engine()
        self._load_last()

        # conectar botões do player
        player_win.cmd_play    = self._cmd_play_seq
        player_win.cmd_stop    = self._cmd_stop
        player_win.cmd_pause   = self._cmd_pause
        player_win.cmd_advance = self._cmd_advance
        player_win._btn_play.clicked.connect(self._cmd_play_seq)
        player_win._btn_stop.clicked.connect(self._cmd_stop)
        player_win._btn_pause.clicked.connect(self._cmd_pause)
        player_win._btn_next.clicked.connect(self._cmd_advance)
        player_win.request_output_config.connect(self._output_config)
        player_win.request_output_stream.connect(self._output_stream)

    # ── config ────────────────────────────────────────────────────────────────

    def _load_cfg(self) -> dict:
        defaults = {'output_mode':'pc', 'video_screen':0, 'audio_device':'',
                    'audio_name':'Padrão do sistema', 'video_screen_name':'Monitor principal',
                    'slate_enabled':False, 'slate_type':'image', 'slate_path':'',
                    'playlist_loop':False, 'autoplay':False}
        if CONFIG_FILE.exists():
            try:
                saved = json.loads(CONFIG_FILE.read_text('utf-8'))
                defaults.update(saved)
                return defaults
            except Exception: pass
        return defaults

    def _save_cfg(self):
        CONFIG_FILE.write_text(json.dumps(self._cfg, indent=2), 'utf-8')

    def _apply_cfg_to_engine(self):
        self._player_win.update_output_label(self._cfg)
        # Passa cfg do slate para o engine para que PAUSE items possam usá-la
        self._engine._slate_cfg = self._cfg

    # ── UI ────────────────────────────────────────────────────────────────────

    def _build_ui(self):
        tb = QToolBar(); tb.setMovable(False)
        self.addToolBar(tb)
        def ta(lbl, tip, fn, key=None):
            a = QAction(lbl, self); a.setToolTip(tip); a.triggered.connect(fn)
            if key: a.setShortcut(key)
            tb.addAction(a)
        ta('📂 Abrir',   'Abrir playlist',         self._open,  'Ctrl+O')
        ta('💾 Salvar',  'Salvar playlist',         self._save,  'Ctrl+S')
        tb.addSeparator()
        ta('➕ Arquivo', 'Adicionar arquivo(s)',     self._add_files_end)
        ta('📡 Stream',  'Adicionar stream',         self._add_stream_end)
        tb.addSeparator()
        ta('⚙ Saída',   'Configurar saída',         self._output_config)
        ta('📤 Stream',  'Enviar saída para stream', self._output_stream)
        tb.addSeparator()
        ta('🔧 Config',  'Configurações gerais',     self._settings)

        # atalhos de teclado
        from PyQt5.QtWidgets import QShortcut
        from PyQt5.QtGui import QKeySequence
        QShortcut(QKeySequence('Ctrl+C'), self, self._cmd_copy)
        QShortcut(QKeySequence('Ctrl+V'), self, lambda: self._cmd_paste(None))
        QShortcut(QKeySequence('Delete'), self, self._cmd_remove)

        cw = QWidget(); self.setCentralWidget(cw)
        vb = QVBoxLayout(cw); vb.setContentsMargins(6,4,6,4); vb.setSpacing(4)

        self._table = PlaylistTable()
        self._table.row_moved.connect(lambda f,t: self._model.move(f,t))
        self._table.ctx_action.connect(self._ctx)
        self._table.doubleClicked.connect(lambda idx: self._play_item(idx.row()))
        vb.addWidget(self._table)

        # ── botões ──────────────────────────────────────────────────────────
        row_b = QHBoxLayout(); row_b.setContentsMargins(0,0,0,0); row_b.setSpacing(3)
        def bb(lbl, tip, fn):
            b = QPushButton(lbl); b.setToolTip(tip); b.setFixedHeight(26)
            b.clicked.connect(fn); row_b.addWidget(b); return b
        bb('▶ Agora',     'Reproduzir selecionado agora',   self._cmd_play_now)
        bb('⏭ A seguir',  'Após VT atual',                  self._cmd_play_next)
        row_b.addStretch()
        bb('⬆', 'Mover para cima',  self._cmd_up)
        bb('⬇', 'Mover para baixo', self._cmd_down)
        row_b.addStretch()
        bb('➕ Arquivo', 'Adicionar arquivo(s)',         self._add_files_end)
        bb('⏸ Pausa',   'Inserir pausa temporizada',    lambda: self._add_pause(None))
        bb('🗑 Remover',  'Remover selecionado',          self._cmd_remove)
        bb('✖ Limpar',   'Limpar lista',                 self._cmd_clear)
        vb.addLayout(row_b)

        # ── rodapé ─────────────────────────────────────────────────────────
        row_f = QHBoxLayout(); row_f.setContentsMargins(0,0,0,0)
        self._lbl_total = QLabel('Total: --:--:--')
        self._lbl_total.setStyleSheet('color:#606070; font-size:9pt;')
        self._lbl_count = QLabel('')
        self._lbl_count.setStyleSheet('color:#606070; font-size:9pt;')
        row_f.addWidget(self._lbl_total); row_f.addStretch(); row_f.addWidget(self._lbl_count)
        vb.addLayout(row_f)

        self._status = QStatusBar(); self.setStatusBar(self._status)


    # ── operações playlist ─────────────────────────────────────────────────────

    def _sel_row(self): 
        rows = sorted({i.row() for i in self._table.selectedIndexes()})
        return rows[0] if rows else -1

    def _sel_rows(self):
        return sorted({i.row() for i in self._table.selectedIndexes()})

    def _add_files(self, at=None):
        paths, _ = QFileDialog.getOpenFileNames(
            self, 'Adicionar vídeos', '',
            'Vídeo (*.mp4 *.mov *.mxf *.avi *.mkv *.ts *.mts *.mpg *.wmv *.m2t);;Todos (*.*)')
        if not paths: return
        for i, fp in enumerate(paths):
            item = PlaylistItem.from_file(fp)
            if at is None:
                idx = len(self._model); self._model.append(item)
            else:
                idx = at + i; self._model.insert(idx, item)
            self._probe(idx, fp)

    def _probe(self, idx: int, fp: str):
        w = ProbeWorker(idx, fp)
        w.result.connect(lambda i, meta: self._model.update_meta(i, meta))
        w.finished.connect(lambda: self._probes.remove(w) if w in self._probes else None)
        self._probes.append(w); w.start()

    def _add_files_end(self): self._add_files()
    def _add_stream_end(self):
        dlg = AddStreamDialog(self)
        if dlg.exec_() == QDialog.Accepted:
            item = dlg.get_item()
            if item: self._model.append(item)

    def _cmd_play_now(self):
        r = self._sel_row()
        if r >= 0:
            self._nxt_idx = -1   # cancela "a seguir" pendente
            self._play_item(r)

    def _cmd_play_next(self):
        r = self._sel_row()
        if r >= 0: self._nxt_idx = r; self._status.showMessage(f'A seguir: {self._model[r].title}')

    def _cmd_up(self):
        r = self._sel_row()
        if r > 0: self._model.move(r, r-1); self._table.selectRow(r-1)

    def _cmd_down(self):
        r = self._sel_row()
        if r >= 0 and r < len(self._model)-1:
            self._model.move(r, r+1); self._table.selectRow(r+1)

    def _cmd_remove(self):
        for r in reversed(self._sel_rows()): self._model.remove(r)

    def _cmd_clear(self):
        if QMessageBox.question(self,'Limpar','Limpar toda a lista?',
                                QMessageBox.Yes|QMessageBox.No) == QMessageBox.Yes:
            self._model.clear()

    def _ctx(self, action: str, row: int):
        m = {
            'play_now':        lambda: (setattr(self, '_nxt_idx', -1), self._play_item(row)),
            'play_next':       self._cmd_play_next,
            'copy':            self._cmd_copy,
            'paste_before':    lambda: self._cmd_paste(row),
            'paste_after':     lambda: self._cmd_paste(row + 1),
            'paste_end':       lambda: self._cmd_paste(None),
            'pause_before':    lambda: self._add_pause(row),
            'pause_after_ins': lambda: self._add_pause(row + 1),
            'pause_end':       lambda: self._add_pause(None),
            'stream_before':   lambda: self._add_stream_at(row),
            'stream_after':    lambda: self._add_stream_at(row + 1),
            'stream_end':      self._add_stream_end,
            'yt_before':       lambda: self._add_stream_at(row),
            'yt_after':        lambda: self._add_stream_at(row + 1),
            'yt_end':          self._add_stream_end,
            'add_files':       lambda: self._add_files(row),
            'add_files_after': lambda: self._add_files(row + 1),
            'add_files_end':   self._add_files_end,
            'move_up':         self._cmd_up,
            'move_down':       self._cmd_down,
            'rename':          lambda: self._rename(row),
            'toggle_pause':    lambda: self._toggle_pause(row),
            'remove':          lambda: self._model.remove(row),
            'clear':           self._cmd_clear,
        }
        fn = m.get(action)
        if fn: fn()

    def _add_stream_at(self, row):
        dlg = AddStreamDialog(self)
        if dlg.exec_() == QDialog.Accepted:
            item = dlg.get_item()
            if item: self._model.insert(row, item)

    def _cmd_copy(self):
        """Copia os VTs selecionados para o clipboard interno."""
        rows = self._sel_rows()
        if not rows: return
        self._clipboard = [_copy.deepcopy(self._model[r]) for r in rows]
        self._status.showMessage(
            f'{len(self._clipboard)} VT{"s" if len(self._clipboard)!=1 else ""} copiado(s)')

    def _cmd_paste(self, at):
        """Cola clipboard no índice `at` (None = fim)."""
        if not self._clipboard:
            self._status.showMessage('Clipboard vazio', 3000); return
        items = [_copy.deepcopy(it) for it in self._clipboard]
        if at is None:
            for item in items:
                self._model.append(item)
                idx = len(self._model) - 1
                if item.type == ItemType.FILE and item.duration <= 0:
                    self._probe(idx, item.filepath)
        else:
            at = max(0, min(at, len(self._model)))
            for i, item in enumerate(items):
                self._model.insert(at + i, item)
                if item.type == ItemType.FILE and item.duration <= 0:
                    self._probe(at + i, item.filepath)
        self._status.showMessage(
            f'{len(items)} VT{"s" if len(items)!=1 else ""} colado(s)')

    def _add_pause(self, at=None):
        """Diálogo para inserir item de pausa temporizada."""
        dur, ok = QInputDialog.getDouble(
            self, 'Inserir pausa', 'Duração da pausa (segundos):', 5.0, 1.0, 3600.0, 1)
        if not ok: return
        item = PlaylistItem.from_pause(dur)
        if at is None:
            self._model.append(item)
        else:
            self._model.insert(max(0, at), item)

    def _settings(self):
        dlg = SettingsDialog(self._cfg, self)
        if dlg.exec_() == QDialog.Accepted:
            self._cfg.update(dlg.get_config())
            self._save_cfg()
            self._apply_cfg_to_engine()

    def _rename(self, row):
        if not (0 <= row < len(self._model)): return
        name, ok = QInputDialog.getText(self, 'Renomear', 'Novo nome:',
                                        text=self._model[row].title)
        if ok and name.strip():
            self._model[row].title = name.strip(); self._model.changed.emit()

    def _toggle_pause(self, row):
        if 0 <= row < len(self._model):
            self._model[row].pause_after = not self._model[row].pause_after
            self._model.changed.emit()

    # ── reprodução ─────────────────────────────────────────────────────────────

    def _play_item(self, idx: int):
        if idx < 0 or idx >= len(self._model): return
        # Bloqueia sig_ended fantasma: stop() pode enfileirar um ended via
        # QueuedConnection que dispararia _on_ended com o novo _cur_idx.
        # O QTimer.singleShot(0) é processado DEPOIS do sinal já enfileirado.
        self._ignore_ended = True
        self._engine.stop_slate()
        self._engine.stop()
        self._cur_idx = idx
        self._running = True
        # Calcula referência de horário: "quando o item 0 teria começado"
        from datetime import timedelta
        cum_before = sum(
            (self._model[i].pause_dur if self._model[i].type == ItemType.PAUSE
             else self._model[i].duration)
            for i in range(idx) if self._model[i].duration > 0 or
            self._model[i].type == ItemType.PAUSE)
        self._sched_ref = datetime.now() - timedelta(seconds=cum_before)
        self._player_win.set_current(idx)
        self._player_win.set_running(True)
        self._engine.play(self._model[idx])
        self._refresh()
        self._status.showMessage(f'Reproduzindo: {self._model[idx].title}')
        QTimer.singleShot(0, self._arm_ended)   # desbloqueia após drenar fila

    def _arm_ended(self):
        """Reativa _on_ended após event loop drenar sig_ended fantasma do stop()."""
        self._ignore_ended = False

    def _cmd_play_seq(self):
        if self._running: return
        if self._cur_idx < 0: self._cur_idx = 0
        self._play_item(self._cur_idx)

    def _cmd_stop(self):
        self._engine.stop()
        self._running = False; self._cur_idx = -1; self._ignore_ended = False
        self._sched_ref = None
        self._player_win.set_running(False)
        self._player_win.set_current(-1)
        self._refresh(); self._status.showMessage('Parado')
        # inicia slate se configurado
        self._engine.start_slate(self._cfg)

    def _cmd_pause(self):
        # VLC preview pause (DeckLink não pausa — continua enviando o último frame)
        if self._engine._vlc_player:
            self._engine._vlc_player.pause()

    def _cmd_advance(self): self._on_ended()

    def _on_ended(self):
        if not self._running or self._ignore_ended: return
        cur  = self._cur_idx
        item = self._model[cur] if 0 <= cur < len(self._model) else None
        if item and item.pause_after:
            self._running = False; self._player_win.set_running(False)
            self._status.showMessage(f'Pausado após: {item.title}')
            self._engine.start_slate(self._cfg)
            return
        nxt = self._nxt_idx if self._nxt_idx >= 0 else cur + 1
        self._nxt_idx = -1
        if nxt < len(self._model):
            self._play_item(nxt)
        elif self._cfg.get('playlist_loop', False) and len(self._model) > 0:
            self._play_item(0)   # repetir lista
        else:
            self._cmd_stop()

    def _on_error(self, msg: str):
        self._status.showMessage(f'⚠ {msg}', 8000)

    # ── refresh ────────────────────────────────────────────────────────────────

    def _refresh(self):
        self._table.populate(self._model, self._cur_idx, self._sched_ref)
        n = len(self._model); s = int(self._model.total_dur())
        self._lbl_count.setText(f'{n} item{"s" if n!=1 else ""}')
        self._lbl_total.setText(f'Total: {s//3600:02d}:{(s%3600)//60:02d}:{s%60:02d}')

    # ── playlist I/O ───────────────────────────────────────────────────────────

    def _open(self):
        fp, _ = QFileDialog.getOpenFileName(self,'Abrir playlist',str(PLAYLISTS_DIR),'*.json')
        if not fp: return
        try:
            self._model.load_json(Path(fp).read_text('utf-8'))
            self._cur_idx = -1
            for i, item in enumerate(self._model.items()):
                if item.type == ItemType.FILE and item.duration <= 0:
                    self._probe(i, item.filepath)
            LAST_PL_FILE.write_text(json.dumps({'path': fp}), 'utf-8')
        except Exception as e: QMessageBox.warning(self,'Erro',str(e))

    def _save(self):
        ts = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
        fp, _ = QFileDialog.getSaveFileName(self,'Salvar playlist',
                    str(PLAYLISTS_DIR/f'playlist_{ts}.json'),'*.json')
        if not fp: return
        try:
            Path(fp).write_text(self._model.to_json(),'utf-8')
            LAST_PL_FILE.write_text(json.dumps({'path':fp}),'utf-8')
            self._status.showMessage(f'Salvo: {Path(fp).name}')
        except Exception as e: QMessageBox.warning(self,'Erro',str(e))

    def _load_last(self):
        if not LAST_PL_FILE.exists(): return
        try:
            fp = json.loads(LAST_PL_FILE.read_text('utf-8')).get('path','')
            if fp and Path(fp).exists():
                self._model.load_json(Path(fp).read_text('utf-8'))
                for i,item in enumerate(self._model.items()):
                    if item.type == ItemType.FILE and item.duration <= 0:
                        self._probe(i, item.filepath)
        except Exception: pass

    # ── saída ──────────────────────────────────────────────────────────────────

    def _output_config(self):
        dlg = OutputConfigDialog(self._cfg, self)
        if dlg.exec_() == QDialog.Accepted:
            new_cfg = dlg.get_config()
            self._cfg.update(new_cfg)
            self._save_cfg()
            self._apply_cfg_to_engine()

    def _output_stream(self):
        dlg = OutputStreamDialog(self)
        if dlg.exec_() == QDialog.Accepted:
            cfg = dlg.get_config()
            QMessageBox.information(self,'Em breve',
                f'Envio para {cfg["type"]} ainda não implementado.')

    # ── fechar ─────────────────────────────────────────────────────────────────

    def closeEvent(self, e):
        self._engine.destroy()
        self._save_cfg()
        # encerra o sysmon antes de fechar
        if hasattr(self._player_win, '_sysmon'):
            self._player_win._sysmon.requestInterruption()
            self._player_win._sysmon.wait(2000)
        if hasattr(self._player_win, '_clock_timer'):
            self._player_win._clock_timer.stop()
        self._player_win.close()
        super().closeEvent(e)


# ══════════════════════════════════════════════════════════════════════════════
# ENTRY POINT
# ══════════════════════════════════════════════════════════════════════════════

if __name__ == '__main__':
    import ctypes
    try: ctypes.windll.shcore.SetProcessDpiAwareness(1)
    except Exception: pass

    app = QApplication(sys.argv)
    app.setApplicationName('TVV Playout')
    app.setOrganizationName('TV Verde Vale')
    f = app.font(); f.setFamily('Segoe UI'); f.setPointSize(9); app.setFont(f)
    app.setStyleSheet(THEME)

    model  = PlaylistModel()
    engine = PlayerEngine()   # preview definido depois

    player_win   = PlayerWindow(engine, model)
    playlist_win = PlaylistWindow(engine, model, player_win)

    # posicionar: lista à esquerda, player à direita
    playlist_win.move(100, 100)
    player_win.move(800, 100)

    playlist_win.show()
    player_win.show()

    sys.exit(app.exec_())
