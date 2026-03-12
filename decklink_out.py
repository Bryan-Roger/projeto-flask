"""
decklink_out.py v12  –  Transição sem preto, arquitetura limpa
==============================================================

PRINCIPIO FUNDAMENTAL:
  enable_output()  → EnableVideoOutput + EnableAudioOutput  (chamado UMA VEZ, no open())
  prepare_next()   → StopScheduledPlayback + zera timestamps + BeginAudioPreroll
                     (chamado antes de CADA VT, SEM desligar o SDI)
  start_playback() → StartScheduledPlayback (chamado após prebuffer de cada VT)

  Resultado: o SDI nunca é interrompido entre VTs → zero tela preta.

THREADS por VT:
  dk-audio → agenda chunks de áudio com timestamps crescentes
  dk-video → agenda frames UYVY com timestamps crescentes, depois chama StartScheduledPlayback

PROTEÇÃO contra mistura de VTs:
  _stop_ev é setado em _kill_ffmpeg() ANTES de qualquer novo load/play.
  Os loops verificam _stop_ev a cada iteração.
  Threads antigas morrem antes das novas começarem (join com timeout).
"""

import ctypes, threading, time, subprocess, os, json, queue
from pathlib import Path
import comtypes, comtypes.client

try:
    import playout_log as _log
except ImportError:
    class _log:
        debug   = staticmethod(lambda m, s: print(f'[D] {m}: {s}'))
        info    = staticmethod(lambda m, s: print(f'[I] {m}: {s}'))
        warn    = staticmethod(lambda m, s: print(f'[W] {m}: {s}'))
        error   = staticmethod(lambda m, s: print(f'[E] {m}: {s}'))
        vt_info = staticmethod(lambda f: None)

# ── SDK ───────────────────────────────────────────────────────────────────────
_SDK_OK = False
_dk = None
try:
    comtypes.client.GetModule(
        r'C:\Program Files\Blackmagic Design\Desktop Video\DeckLinkAPI64.dll')
    from comtypes.gen import DeckLinkAPI as _dk
    _SDK_OK = True
except Exception as _e:
    _log.error('DeckLink', f'DeckLinkAPI não carregado: {_e}')

# ── Constantes ────────────────────────────────────────────────────────────────
OUTPUT_MODE     = _dk.bmdModeHD1080i5994              if _SDK_OK else 0
BMD_FMT_UYVY    = _dk.bmdFormat8BitYUV                if _SDK_OK else 0
BMD_AUDIO_48K   = _dk.bmdAudioSampleRate48kHz         if _SDK_OK else 0
BMD_AUDIO_S16   = _dk.bmdAudioSampleType16bitInteger  if _SDK_OK else 0
BMD_TIMESTAMPED = _dk.bmdAudioOutputStreamTimestamped if _SDK_OK else 0
BMD_OUT_DEF     = _dk.bmdVideoOutputFlagDefault       if _SDK_OK else 0

WIDTH       = 1920
HEIGHT      = 1080
ROW_BYTES   = WIDTH * 2
FRAME_BYTES = ROW_BYTES * HEIGHT   # 4 147 200 bytes/frame

TIMESCALE   = 30000    # ticks/s
FRAME_DUR   = 1001     # ticks/frame → 29.97fps
OUTPUT_FPS  = TIMESCALE / FRAME_DUR

SAMPLE_RATE = 48000
CHANNELS    = 2
BYTES_SAMP  = 2
# 48000 * 1001/30000 = 1601.6 → alternamos 1601/1602 para média exata
AUD_LO      = int(SAMPLE_RATE * FRAME_DUR // TIMESCALE)   # 1601
AUD_HI      = AUD_LO + 1                                   # 1602

POOL_SIZE   = 16   # frames no pool de vídeo do hardware
PREBUFFER   = 8    # frames pré-agendados antes de StartScheduledPlayback (~267ms)
PRELOAD_Q   = 30   # frames na fila de pré-carga do próximo VT

STATE_IDLE    = 'idle'
STATE_PLAYING = 'playing'
STATE_STOPPED = 'stopped'
STATE_ERROR   = 'error'


def _find_ffmpeg(exe='ffmpeg'):
    for p in [rf'C:\ffmpeg\bin\{exe}.exe',
              rf'C:\Users\tv\Documents\claude\{exe}.exe',
              rf'C:\Users\tv\Documents\claude\bin\{exe}.exe']:
        if os.path.isfile(p): return p
    return exe


def _probe(filepath: str) -> dict:
    default = {'fps': 29.97, 'fps_str': '30000/1001', 'width': 1920,
               'height': 1080, 'duration': 0.0, 'audio_rate': 48000, 'has_audio': True}
    try:
        fl = subprocess.CREATE_NO_WINDOW if os.name == 'nt' else 0
        r = subprocess.run(
            [_find_ffmpeg('ffprobe'), '-v', 'quiet', '-print_format', 'json',
             '-show_streams', '-show_format', filepath],
            capture_output=True, text=True, timeout=15, creationflags=fl)
        if not r.stdout: return default
        d = json.loads(r.stdout)
        info = dict(default)
        for s in d.get('streams', []):
            if s.get('codec_type') == 'video':
                fps_str = s.get('r_frame_rate', '30000/1001')
                info['fps_str'] = fps_str
                try:
                    n, den = fps_str.split('/')
                    info['fps'] = float(n) / float(den)
                except Exception: pass
                info['width']  = int(s.get('width',  1920))
                info['height'] = int(s.get('height', 1080))
            elif s.get('codec_type') == 'audio':
                info['has_audio'] = True
                try: info['audio_rate'] = int(s.get('sample_rate', 48000))
                except Exception: pass
        try: info['duration'] = float(d.get('format', {}).get('duration', 0))
        except Exception: pass
        return info
    except Exception as e:
        _log.warn('probe', str(e))
        return default


# ── _Prebuffer ────────────────────────────────────────────────────────────────

class _Prebuffer:
    """Pré-carrega o próximo VT em background enquanto o atual toca."""

    def __init__(self):
        self._fp     = ''
        self._vinfo  = {}
        self._vproc  = None
        self._aproc  = None
        self._vid_q  = queue.Queue(maxsize=PRELOAD_Q)
        self._aud_q  = queue.Queue(maxsize=PRELOAD_Q)
        self._ready  = threading.Event()
        self._stop   = threading.Event()
        self._n      = 0
        self._active = False

    def start(self, filepath: str, vinfo: dict):
        self.cancel()
        self._fp     = filepath
        self._vinfo  = dict(vinfo)
        self._n      = 0
        self._stop.clear()
        self._ready.clear()
        self._active = True
        for q in (self._vid_q, self._aud_q):
            while not q.empty():
                try: q.get_nowait()
                except queue.Empty: break
        threading.Thread(target=self._run, daemon=True, name='dk-preload').start()
        _log.info('DeckLink', f'preload iniciado: {Path(filepath).name}')

    def _run(self):
        comtypes.CoInitialize()
        fl = subprocess.CREATE_NO_WINDOW if os.name == 'nt' else 0
        vf = f'scale={WIDTH}:{HEIGHT}:flags=lanczos,fps=fps={TIMESCALE}/{FRAME_DUR}'
        vid_cmd = [_find_ffmpeg(), '-y', '-i', self._fp, '-map', '0:v:0',
                   '-vf', vf, '-pix_fmt', 'uyvy422', '-f', 'rawvideo', '-an', 'pipe:1']
        aud_cmd = [_find_ffmpeg(), '-y', '-i', self._fp, '-map', '0:a:0',
                   '-acodec', 'pcm_s16le', '-ar', str(SAMPLE_RATE), '-ac', str(CHANNELS),
                   '-f', 's16le', '-vn', 'pipe:1'] if self._vinfo.get('has_audio') else None
        try:
            self._vproc = subprocess.Popen(vid_cmd, stdout=subprocess.PIPE,
                stderr=subprocess.DEVNULL, bufsize=FRAME_BYTES * POOL_SIZE, creationflags=fl)
            if aud_cmd:
                self._aproc = subprocess.Popen(aud_cmd, stdout=subprocess.PIPE,
                    stderr=subprocess.DEVNULL,
                    bufsize=AUD_HI * CHANNELS * BYTES_SAMP * 300, creationflags=fl)
        except Exception as e:
            _log.warn('preload', f'launch: {e}')
            self._active = False; self._ready.set(); return
        try:
            # Salva referência local ao stdout ANTES do loop
            # (take() pode setar self._vproc=None enquanto o loop roda)
            vid_pipe = self._vproc.stdout
            while not self._stop.is_set():
                raw = vid_pipe.read(FRAME_BYTES)
                if len(raw) < FRAME_BYTES: break
                try:
                    self._vid_q.put(raw, timeout=2.0)
                    self._n += 1
                    if self._n == PREBUFFER:
                        self._ready.set()
                        _log.debug('DeckLink', f'preload pronto: {self._n} frames na fila')
                except queue.Full:
                    pass   # fila cheia = FFmpeg para naturalmente
        except Exception as e:
            if not self._stop.is_set(): _log.warn('preload', str(e))
        finally:
            if not self._ready.is_set(): self._ready.set()

    @property
    def is_ready(self):
        return self._ready.is_set() and self._vid_q.qsize() >= PREBUFFER

    def filepath(self): return self._fp

    def take(self):
        """
        Entrega processos e filas ao player.
        CRÍTICO 1: seta _stop para que _run() pare de ler o pipe.
        CRÍTICO 2: cria NOVAS Queue objects para não compartilhar com o player.
                   Sem isso, start() do próximo preload drena a fila ativa do player.
        """
        self._stop.set()
        vp, ap = self._vproc, self._aproc
        vq, aq = self._vid_q, self._aud_q
        # Novas filas isoladas para o próximo preload
        self._vid_q = queue.Queue(maxsize=PRELOAD_Q)
        self._aud_q = queue.Queue(maxsize=PRELOAD_Q)
        self._vproc = self._aproc = None
        self._active = False
        return vp, ap, vq, aq

    def cancel(self):
        if not self._active: return
        self._stop.set()
        for p in (self._vproc, self._aproc):
            if p:
                try: p.kill()
                except Exception: pass
        self._vproc = self._aproc = None
        self._active = False


# ── _HWOutput ─────────────────────────────────────────────────────────────────

class _HWOutput:
    """
    Gerencia IDeckLinkOutput.

    enable_output()  → EnableVideoOutput + EnableAudioOutput (UMA VEZ)
    prepare_next()   → prepara para o próximo VT:
                       - Primeira VT: StopScheduledPlayback + zera timestamps + BeginAudioPreroll
                       - VTs seguintes: NÃO para scheduling, NÃO zera timestamps
                         _vid_t/_aud_t continuam crescendo → timeline contínua sem gaps
    start_playback() → StartScheduledPlayback (apenas na PRIMEIRA VT)
    disable_output() → desliga SDI (só ao fechar o player)

    TIMELINE CONTÍNUA:
      VT1 frames: t=0..150_150  →  VT2 frames: t=150_151..300_301  →  etc.
      O hardware simplesmente reproduz os frames em ordem cronológica.
      Sem StopScheduledPlayback entre VTs = sem descarte de frames prebuffered.
    """

    def __init__(self):
        self._output      = None
        self._pool        = []
        self._pool_buf    = []
        self._pool_idx    = 0
        self._vid_t       = 0
        self._aud_t       = 0
        self._open        = False
        self._out_enabled = False
        self._playing     = False
        self._aud_last_wall = None
        self._sp_wall       = None   # perf_counter() no momento do StartScheduledPlayback
        self._vid_skip      = 0      # frames a descartar no início do próximo VT (A/V sync)

    def open(self) -> bool:
        comtypes.CoInitialize()
        try:
            it  = comtypes.client.CreateObject(
                _dk.CDeckLinkIterator, interface=_dk.IDeckLinkIterator)
            dev = it.Next()
            if not dev:
                _log.error('HW', 'Nenhum dispositivo DeckLink'); return False
            try: _log.info('DeckLink', f'Dispositivo: {dev.GetDisplayName()}')
            except Exception: pass
            self._output = dev.QueryInterface(_dk.IDeckLinkOutput)
            self._open   = True
            return True
        except Exception as e:
            _log.error('HW', f'open(): {e}'); return False

    def enable_output(self) -> bool:
        """Habilita saída SDI — chamado UMA VEZ no open() do player."""
        if not self._open:        return False
        if self._out_enabled:     return True
        try:
            self._output.EnableVideoOutput(OUTPUT_MODE, BMD_OUT_DEF)
            self._output.EnableAudioOutput(
                BMD_AUDIO_48K, BMD_AUDIO_S16, CHANNELS, BMD_TIMESTAMPED)
            self._pool = []; self._pool_buf = []
            for _ in range(POOL_SIZE):
                f = self._output.CreateVideoFrame(
                    WIDTH, HEIGHT, ROW_BYTES, BMD_FMT_UYVY, BMD_OUT_DEF)
                self._pool.append(f)
                self._pool_buf.append(f.GetBytes())
            self._out_enabled = True
            _log.info('DeckLink', f'SDI habilitado: 1080i59.94 pool={POOL_SIZE}')
            return True
        except Exception as e:
            _log.error('HW', f'enable_output(): {e}')
            import traceback; _log.error('HW', traceback.format_exc())
            return False

    def prepare_next(self) -> bool:
        """
        Prepara para o próximo VT.

        Primeira VT (_playing=False):
          → StopScheduledPlayback + zera timestamps + BeginAudioPreroll
          → Necessário para sincronizar o clock antes do primeiro StartScheduledPlayback

        VTs seguintes (_playing=True):
          → NÃO para o scheduling, NÃO zera timestamps
          → _vid_t e _aud_t continuam de onde pararam
          → O hardware vai reproduzir os frames novos em sequência natural
          → Zero gap, zero frames descartados
        """
        # Re-habilita se necessário (ex: stop() manual chamado antes da transição)
        if not self._out_enabled:
            _log.info('DeckLink', 'prepare_next: re-habilitando output SDI')
            if not self.enable_output(): return False
            try:
                self._vid_t = self._aud_t = self._pool_idx = 0
                self._playing = False
                self._output.BeginAudioPreroll()
                _log.debug('DeckLink', 'prepare_next: output re-habilitado, preroll aberto')
                return True
            except Exception as e:
                _log.warn('HW', f'prepare_next (re-enable): {e}'); return False

        if not self._playing:
            # Primeira VT: reset completo + preroll
            try:
                self._output.StopScheduledPlayback(0, TIMESCALE)
                self._output.FlushBufferedAudioSamples()
                self._vid_t = self._aud_t = self._pool_idx = 0
                self._output.BeginAudioPreroll()
                _log.debug('DeckLink', 'prepare_next: primeira VT, timestamps zerados, preroll aberto')
                return True
            except Exception as e:
                _log.warn('HW', f'prepare_next(): {e}'); return False
        else:
            # VTs seguintes: timeline contínua — não para nada
            _log.debug('DeckLink',
                f'prepare_next: continuação vid_t={self._vid_t} aud_t={self._aud_t}')
            return True

    def schedule_frame(self, raw: bytes) -> bool:
        if not self._out_enabled: return False
        try:
            idx = self._pool_idx % POOL_SIZE
            ctypes.memmove(self._pool_buf[idx], raw, FRAME_BYTES)
            self._output.ScheduleVideoFrame(
                self._pool[idx], self._vid_t, FRAME_DUR, TIMESCALE)
            self._vid_t    += FRAME_DUR
            self._pool_idx += 1
            return True
        except Exception as e:
            _log.warn('HW', f'schedule_frame t={self._vid_t}: {e}'); return False

    def schedule_audio(self, raw: bytes) -> bool:
        if not self._out_enabled or not raw: return True
        n = len(raw) // (CHANNELS * BYTES_SAMP)
        try:
            buf = (ctypes.c_char * len(raw)).from_buffer_copy(raw)
            self._output.ScheduleAudioSamples(buf, n, self._aud_t, SAMPLE_RATE)
        except Exception as e:
            _log.warn('HW', f'schedule_audio t={self._aud_t}: {e}')
        # SEMPRE incrementa aud_t — mesmo se SDK rejeitou o chunk (past timestamp).
        # Sem isso, aud_t fica preso no passado e todos os chunks seguintes são rejeitados.
        self._aud_t += n
        self._aud_last_wall = time.perf_counter()
        return True

    def start_playback(self):
        """StartScheduledPlayback — chamado APENAS na primeira VT."""
        if self._playing: return
        try:
            self._output.StartScheduledPlayback(0, TIMESCALE, 1.0)
            self._sp_wall = time.perf_counter()   # salva referência de tempo para calcular hw_pos
            self._playing = True
            _log.info('DeckLink', 'StartScheduledPlayback OK')
        except Exception as e:
            _log.error('HW', f'StartScheduledPlayback: {e}')

    def disable_output(self):
        """Desliga SDI completamente — só usar ao fechar o player."""
        if not self._out_enabled: return
        try: self._output.StopScheduledPlayback(0, TIMESCALE)
        except Exception: pass
        try: self._output.FlushBufferedAudioSamples()
        except Exception: pass
        try: self._output.DisableVideoOutput()
        except Exception: pass
        try: self._output.DisableAudioOutput()
        except Exception: pass
        self._pool = []; self._pool_buf = []
        self._out_enabled = False
        self._playing     = False
        time.sleep(0.05)

    def close(self):
        self.disable_output()
        self._open = False
        _log.info('DeckLink', 'Hardware fechado')


# ── DeckLinkPlayer ────────────────────────────────────────────────────────────

class DeckLinkPlayer:

    def __init__(self, on_ended=None, on_error=None):
        self._on_ended  = on_ended
        self._on_error  = on_error
        self._hw        = _HWOutput()
        self._prebuf    = _Prebuffer()
        self._filepath  = ''
        self._vinfo     = {}
        self._duration  = 0.0
        self._state     = STATE_IDLE
        # _stop_ev: sinaliza para as threads do VT ATUAL pararem
        # É setado em _kill_ffmpeg() ANTES de qualquer novo load/play
        self._stop_ev   = threading.Event()
        self._aud_ready = threading.Event()
        self._sp_ev     = threading.Event()   # setado pelo video_loop após StartScheduledPlayback
        self._vid_proc  = None
        self._aud_proc  = None
        self._vid_q     = None
        self._aud_q     = None
        self._vid_thrd  = None
        self._aud_thrd  = None
        self._frame_cnt = 0
        self._vol_pct   = 80
        comtypes.CoInitialize()

    def open(self) -> bool:
        if not self._hw.open():           return False
        if not self._hw.enable_output():  return False
        _log.info('DeckLink', 'Player pronto (1080i59.94, áudio timestamped)')
        return True

    @property
    def state(self): return self._state
    @property
    def is_playing(self): return self._state == STATE_PLAYING
    def get_position(self):  return self._frame_cnt / OUTPUT_FPS if OUTPUT_FPS > 0 else 0.0
    def get_duration(self):  return self._duration
    def get_remaining(self): return max(0.0, self._duration - self.get_position())

    def prebuffer_next(self, filepath: str):
        if not filepath or not os.path.isfile(filepath): return
        self._prebuf.start(filepath, _probe(filepath))

    # ── load ──────────────────────────────────────────────────────────────────

    def load(self, filepath: str) -> bool:
        # 1. Para threads e FFmpeg do VT anterior (hardware continua ativo)
        self._kill_ffmpeg()
        # 2. Reseta estado para o novo VT
        self._filepath  = filepath
        self._frame_cnt = 0
        self._vid_q = self._aud_q = None

        # 3. Verifica preload
        if self._prebuf.filepath() == filepath and self._prebuf.is_ready:
            info = self._prebuf._vinfo
            _log.info('DeckLink', f'load (preload pronto): {Path(filepath).name}')
        else:
            if self._prebuf._active and self._prebuf.filepath() != filepath:
                self._prebuf.cancel()
            info = _probe(filepath)

        self._duration = info['duration']
        self._vinfo    = info
        w, h, fps = info['width'], info['height'], info['fps']
        _log.info('DeckLink',
            f'load: {Path(filepath).name} ({self._duration:.1f}s, {fps:.3f}fps, {w}x{h})')
        _log.vt_info(filepath)
        self._state = STATE_IDLE
        return True

    # ── play ──────────────────────────────────────────────────────────────────

    def play(self) -> bool:
        if not self._filepath: return False

        # Prepara hardware: para scheduling anterior, zera timestamps, abre preroll
        # SEM desligar o SDI
        if not self._hw.prepare_next():
            _log.error('DeckLink', 'prepare_next() falhou')
            self._state = STATE_ERROR
            if self._on_error: self._on_error('prepare_next falhou')
            return False

        # Limpa eventos para este VT
        self._stop_ev.clear()
        self._aud_ready.clear()
        self._sp_ev.clear()
        self._frame_cnt = 0

        info = self._vinfo
        w, h, fps = info.get('width', 1920), info.get('height', 1080), info.get('fps', 29.97)

        # Usa preload se disponível
        using_preload = (self._prebuf.filepath() == self._filepath
                         and self._prebuf.is_ready)
        if using_preload:
            _log.info('DeckLink',
                f'play (preload): {w}x{h} {fps:.3f}fps → 1080i59.94 '
                f'({self._prebuf._vid_q.qsize()} frames)')
            self._vid_proc, self._aud_proc, self._vid_q, self._aud_q = \
                self._prebuf.take()
        else:
            if self._prebuf._active: self._prebuf.cancel()
            vf  = f'scale={WIDTH}:{HEIGHT}:flags=lanczos,fps=fps={TIMESCALE}/{FRAME_DUR}'
            fl  = subprocess.CREATE_NO_WINDOW if os.name == 'nt' else 0
            vid_cmd = [_find_ffmpeg(), '-y', '-i', self._filepath,
                       '-map', '0:v:0', '-vf', vf,
                       '-pix_fmt', 'uyvy422', '-f', 'rawvideo', '-an', 'pipe:1']
            aud_cmd = [_find_ffmpeg(), '-y', '-i', self._filepath,
                       '-map', '0:a:0', '-acodec', 'pcm_s16le',
                       '-ar', str(SAMPLE_RATE), '-ac', str(CHANNELS),
                       '-f', 's16le', '-vn', 'pipe:1'] \
                      if info.get('has_audio', True) else None
            _log.info('DeckLink', f'play: {w}x{h} {fps:.3f}fps → 1080i59.94')
            try:
                self._vid_proc = subprocess.Popen(
                    vid_cmd, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
                    bufsize=FRAME_BYTES * POOL_SIZE, creationflags=fl)
                if aud_cmd:
                    self._aud_proc = subprocess.Popen(
                        aud_cmd, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
                        bufsize=AUD_HI * CHANNELS * BYTES_SAMP * 300, creationflags=fl)
            except Exception as e:
                _log.error('DeckLink', f'FFmpeg launch: {e}')
                self._state = STATE_ERROR
                if self._on_error: self._on_error(str(e))
                return False
            self._vid_q = self._aud_q = None

        self._state    = STATE_PLAYING

        # Em continuação: sem ajuste de timestamps.
        # O throttle com tempo absoluto (start_t + n*fi) não acumula drift,
        # então aud_t e vid_t chegam às transições sempre corretos.
        self._hw._vid_skip = 0

        self._aud_thrd = threading.Thread(
            target=self._audio_loop, daemon=True, name='dk-audio')
        self._vid_thrd = threading.Thread(
            target=self._video_loop, daemon=True, name='dk-video')
        self._aud_thrd.start()
        self._vid_thrd.start()
        return True

    # ── helpers de leitura ────────────────────────────────────────────────────

    def _read_vid(self):
        """
        Frame da fila preload se disponível, senão do pipe FFmpeg.
        Tenta a fila com timeout curto para tolerar latência de put(),
        só passa para o pipe quando a fila estiver definitivamente vazia.
        """
        if self._vid_q is not None:
            try:
                return self._vid_q.get(timeout=0.05)   # 50ms — tolera latência
            except queue.Empty:
                self._vid_q = None   # fila esgotada, passa para pipe
        if self._vid_proc and self._vid_proc.stdout:
            return self._vid_proc.stdout.read(FRAME_BYTES)
        return b''

    def _read_aud(self, nb: int) -> bytes:
        """Chunk de áudio da fila preload se disponível, senão do pipe FFmpeg."""
        if self._aud_q is not None:
            try:
                raw = self._aud_q.get_nowait()
                if len(raw) >= nb: return raw[:nb]
                self._aud_q = None
            except queue.Empty: self._aud_q = None
        if self._aud_proc and self._aud_proc.stdout:
            return self._aud_proc.stdout.read(nb)
        return b''

    # ── loop de áudio ─────────────────────────────────────────────────────────

    def _audio_loop(self):
        comtypes.CoInitialize()
        if not self._aud_proc:
            self._aud_ready.set()
            return

        fi = FRAME_DUR / TIMESCALE  # ~33.37ms por frame

        def nb(idx):
            return (AUD_HI if idx % 3 == 2 else AUD_LO) * CHANNELS * BYTES_SAMP

        continuation = self._hw._playing
        idx = 0; total_samp = 0
        prebuf_done = continuation
        # Tempo absoluto de referência para throttle sem drift.
        # next_t = start_t + n*fi elimina o acúmulo de erro do Windows timer.
        # next_t += fi acumula ~2ms/s de drift → 120ms em 60s → aud_t cai no passado.
        start_t = None
        next_t  = None

        if continuation:
            # ── SYNC aud_t ao clock do hardware ──────────────────────────────
            # Problema: entre o fim da VT anterior e o início desta, há um gap
            # de vários segundos (kill_ffmpeg, VLC stop, probe, spawn FFmpeg).
            # Durante esse gap o hardware avança, mas aud_t fica congelado.
            # Resultado: todos os samples desta VT ficam no passado → SDK
            # os descarta silenciosamente → VT sem áudio.
            #
            # Solução: usar _sp_wall (perf_counter() no StartScheduledPlayback)
            # para estimar a posição atual do hardware e avançar aud_t se
            # necessário, adicionando uma margem de 200ms para garantir que
            # os primeiros samples sejam sempre futuros.
            if self._hw._sp_wall is not None:
                elapsed_s  = time.perf_counter() - self._hw._sp_wall
                hw_aud_pos = int(elapsed_s * SAMPLE_RATE)
                margin     = int(SAMPLE_RATE * 0.20)   # 200ms de margem
                if hw_aud_pos + margin > self._hw._aud_t:
                    old_aud_t = self._hw._aud_t
                    self._hw._aud_t = hw_aud_pos + margin
                    _log.debug('DeckLink',
                        f'aud_t sync: {old_aud_t} → {self._hw._aud_t} '
                        f'(hw_pos={hw_aud_pos} gap={elapsed_s - old_aud_t/SAMPLE_RATE:.2f}s)')
            # ─────────────────────────────────────────────────────────────────
            start_t = time.perf_counter()
            next_t  = start_t + fi
            self._aud_ready.set()

        try:
            while not self._stop_ev.is_set():
                raw = self._read_aud(nb(idx))
                if not raw: break

                self._hw.schedule_audio(self._apply_volume(raw))
                total_samp += len(raw) // (CHANNELS * BYTES_SAMP)
                idx += 1

                if not prebuf_done and idx >= PREBUFFER:
                    prebuf_done = True
                    self._aud_ready.set()
                    # Aguarda StartScheduledPlayback e inicia throttle com referência absoluta
                    self._sp_ev.wait(timeout=5.0)
                    # ── SYNC aud_t para fontes lentas (SRT/RTMP) ─────────────
                    # Em streams de rede, o prebuffer pode completar DEPOIS de
                    # StartScheduledPlayback. Nesse caso aud_t=0~267ms enquanto
                    # o hardware já está em frente → SDK descarta todos os frames
                    # como "passado" → sem áudio.
                    # Mesma técnica usada no path de continuação entre VTs.
                    if self._hw._sp_wall is not None:
                        elapsed_s  = time.perf_counter() - self._hw._sp_wall
                        hw_aud_pos = int(elapsed_s * SAMPLE_RATE)
                        margin     = int(SAMPLE_RATE * 0.25)   # 250ms de margem
                        if hw_aud_pos + margin > self._hw._aud_t:
                            old = self._hw._aud_t
                            self._hw._aud_t = hw_aud_pos + margin
                            _log.debug('DeckLink',
                                f'aud_t sync (stream late): {old} → {self._hw._aud_t} '
                                f'(hw={hw_aud_pos} elapsed={elapsed_s:.3f}s)')
                    # ─────────────────────────────────────────────────────────
                    start_t = time.perf_counter()
                    next_t  = start_t + fi
                    _log.debug('DeckLink',
                        f'áudio prebuffer OK: {idx} frames '
                        f'({total_samp / SAMPLE_RATE * 1000:.0f}ms)')

                if prebuf_done and next_t:
                    w = next_t - time.perf_counter()
                    if w > 0.002: time.sleep(w - 0.001)
                    while time.perf_counter() < next_t: pass
                    # Tempo absoluto: start_t + n*fi — sem acúmulo de drift
                    next_t = start_t + (idx + 1) * fi

        except Exception as e:
            if not self._stop_ev.is_set():
                _log.error('DeckLink', f'audio_loop: {e}')
        finally:
            if not prebuf_done: self._aud_ready.set()
            _log.debug('DeckLink',
                f'audio_loop fim: {idx} frames ({total_samp / SAMPLE_RATE:.1f}s)')

    # ── loop de vídeo ─────────────────────────────────────────────────────────

    def _video_loop(self):
        comtypes.CoInitialize()
        fi = FRAME_DUR / TIMESCALE

        # Em modo continuação, não há prebuffer nem StartScheduledPlayback
        continuation = self._hw._playing
        prebuf = 0
        started = continuation
        # Tempo absoluto de referência — sem drift acumulado
        start_t = time.perf_counter() if continuation else None
        next_t  = (start_t + fi) if continuation else None

        if continuation:
            _log.debug('DeckLink', f'vídeo continuação: vid_t={self._hw._vid_t}')
            self._sp_ev.set()   # em continuação não há StartScheduledPlayback — libera audio_loop imediatamente

        try:
            # Só aguarda o áudio na primeira VT
            if not continuation:
                # Streams de rede (SRT/RTMP) podem levar mais de 3s para
                # estabilizar o prebuffer de áudio. Usa 8s de timeout.
                if not self._aud_ready.wait(timeout=8.0):
                    _log.warn('DeckLink', 'timeout aguardando áudio prebuffer')

            while not self._stop_ev.is_set():
                if started and next_t:
                    w = next_t - time.perf_counter()
                    if w > 0.002: time.sleep(w - 0.001)
                    while time.perf_counter() < next_t: pass

                raw = self._read_vid()
                if not raw or len(raw) < FRAME_BYTES: break

                if not self._hw.schedule_frame(raw): break
                self._frame_cnt += 1
                prebuf += 1

                if not started and prebuf >= PREBUFFER:
                    # Primeira VT: inicia playback após prebuffer
                    self._hw.start_playback()
                    self._sp_ev.set()   # libera audio_loop para iniciar throttle
                    started = True
                    start_t = time.perf_counter()
                    next_t  = start_t + fi
                elif started:
                    # Tempo absoluto: start_t + n*fi — sem drift
                    next_t = start_t + prebuf * fi

                if self._frame_cnt % 150 == 0:
                    _log.debug('DeckLink',
                        f'frame {self._frame_cnt} pos={self.get_position():.1f}s')

        except Exception as e:
            if not self._stop_ev.is_set():
                _log.error('DeckLink', f'video_loop: {e}')
        finally:
            if not self._stop_ev.is_set():
                _log.info('DeckLink',
                    f'fim VT: {self._frame_cnt} frames ({self.get_position():.1f}s)')
                self._state = STATE_STOPPED
                if self._on_ended: self._on_ended()

    # ── controles ─────────────────────────────────────────────────────────────

    def _kill_ffmpeg(self):
        """
        Para as threads e processos FFmpeg do VT atual.
        NÃO mexe no hardware — SDI continua ativo.
        CUIDADO: pode ser chamado de dentro do _video_loop (via on_ended).
        Nesse caso, NÃO faz join na própria thread (deadlock).
        """
        self._stop_ev.set()
        self._aud_ready.set()   # desbloqueia audio_loop se estiver esperando

        # Mata pipes para desbloquear threads que estão em read()
        for p in (self._vid_proc, self._aud_proc):
            if p:
                try: p.kill()
                except Exception: pass

        cur = threading.current_thread()
        # Só faz join em threads que NÃO são a thread atual (evita deadlock)
        if (self._vid_thrd and self._vid_thrd.is_alive()
                and self._vid_thrd is not cur):
            self._vid_thrd.join(timeout=1.0)
        if (self._aud_thrd and self._aud_thrd.is_alive()
                and self._aud_thrd is not cur):
            self._aud_thrd.join(timeout=1.0)

        self._vid_proc = self._aud_proc = None
        self._vid_q    = self._aud_q    = None
        self._vid_thrd = self._aud_thrd = None

    def stop(self):
        """Stop manual (operador) — também desliga output SDI."""
        self._kill_ffmpeg()
        self._hw.disable_output()
        self._state = STATE_IDLE
        _log.info('DeckLink', 'stop()')

    def set_volume(self, pct: int):
        self._vol_pct = max(0, min(100, int(pct)))

    def _apply_volume(self, raw: bytes) -> bytes:
        """Aplica volume aos samples PCM s16le. Rápido via array de int16."""
        if self._vol_pct == 100 or not raw: return raw
        if self._vol_pct == 0: return bytes(len(raw))
        import array
        gain  = self._vol_pct / 100.0
        samps = array.array('h', raw)   # int16 signed
        for i in range(len(samps)):
            v = int(samps[i] * gain)
            samps[i] = max(-32768, min(32767, v))
        return samps.tobytes()

    # ── aliases de compatibilidade ────────────────────────────────────────────
    def build(self, filepath: str) -> bool: return self.load(filepath)
    def pause(self):
        if self._state == STATE_PLAYING:
            self._stop_ev.set(); self._state = STATE_STOPPED
    def resume(self):
        if self._state == STATE_STOPPED and self._filepath: self.play()
    def prebuild(self, fp): pass

    def destroy(self):
        self._prebuf.cancel()
        self._kill_ffmpeg()
        self._hw.close()

    def __del__(self):
        try: self.destroy()
        except Exception: pass


DeckLinkGraph = DeckLinkPlayer
