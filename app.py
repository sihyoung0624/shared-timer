"""
==========================================================================
 모두의 타이머 (Shared Timer)
==========================================================================
 목적: 같은 와이파이 안에서 여러 사람이 각자 노트북/폰으로
       동일한 카운트다운을 실시간으로 보는 타이머.

 핵심 설계:
  - 뷰어 링크 / 컨트롤러 링크를 '분리'한다.
    · 뷰어 링크   : 시간만 본다 (발표자/청중용)
    · 컨트롤러 링크: 시작·정지·리셋 제어 (진행자용, 비밀 토큰 포함)
  - 컨트롤러 토큰(제어 비밀번호 역할)을 모르면 제어 불가.
  - 데이터는 메모리에만 저장 → 서버 끄면 사라짐.

 v2.1 변경점 (버그 2개 수정):
  - [버그①] 진행 중 '시작' 재클릭 시 타이머 작업이 중복 생성되어
            2배속으로 줄던 문제 → 세대번호(epoch)로 옛 작업 자동 종료.
  - [버그②] 접속자가 나가도 수가 안 줄고 30명 제한이 누적으로 막히던 문제
            → sid↔room 매핑으로 입장/퇴장을 정확히 집계.

 필요 패키지:  pip install flask flask-socketio
 실행:        python app.py
==========================================================================
"""

from flask import Flask, request, jsonify
from flask_socketio import SocketIO, emit, join_room
import uuid, time, threading, socket, os
from io import BytesIO
import qrcode
import qrcode.image.svg   # SVG 방식이라 PIL(이미지 라이브러리) 설치가 필요 없음

app = Flask(__name__)
# [보안] 비밀키는 환경변수 SECRET_KEY 로 주입. 없으면 개발용 임시값(★배포 시 반드시 설정!).
app.config['SECRET_KEY'] = os.environ.get('SECRET_KEY', 'dev-only-change-me-in-production')

# [보안] 접속 허용 출처(CORS). 배포 시 환경변수 CORS_ORIGINS 에 도메인을 콤마로 지정.
#   예) CORS_ORIGINS="https://my-timer.onrender.com"  (지정 없으면 개발 편의상 전체 허용)
_cors = os.environ.get('CORS_ORIGINS', '*')
_cors_origins = '*' if _cors.strip() == '*' else [o.strip() for o in _cors.split(',') if o.strip()]
socketio = SocketIO(app, cors_allowed_origins=_cors_origins)

rooms = {}
rooms_lock = threading.Lock()   # 동시 사용 시 데이터 꼬임 방지
sid_to_room = {}                # [버그②수정] 소켓ID → 방ID (퇴장 시 인원 정확히 차감)
MAX_VIEWERS = 30
MAX_ROOMS = int(os.environ.get('MAX_ROOMS', 500))      # [남용방지] 동시에 존재 가능한 방 수 상한
ROOM_TTL = int(os.environ.get('ROOM_TTL', 6 * 3600))   # [남용방지] 생성 후 이 시간(초) 지나면 자동 정리


def get_local_ip():
    """같은 와이파이의 다른 기기가 접속할 IP를 찾아준다 (안내용)."""
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(('8.8.8.8', 80))
        ip = s.getsockname()[0]
        s.close()
        return ip
    except Exception:
        return '127.0.0.1'


def public_base_url():
    """링크·QR에 넣을 '바깥에서 접속 가능한 주소'를 결정한다.
    - 배포 시: 환경변수 PUBLIC_BASE_URL 을 그대로 사용(가장 확실, 권장).
    - 로컬에서 localhost 로 접속: 같은 와이파이 기기를 위해 LAN IP 로 치환.
    - 공개 도메인으로 접속: 들어온 주소를 그대로 사용(HTTPS 프록시 뒤 대응 포함)."""
    env = os.environ.get('PUBLIC_BASE_URL')
    if env:
        return env.rstrip('/')
    host = request.host                         # 예: 'localhost:5050' 또는 'timer.onrender.com'
    hostname = host.split(':')[0]
    if hostname in ('localhost', '127.0.0.1'):
        port = host.split(':')[-1] if ':' in host else '5050'
        return f'http://{get_local_ip()}:{port}'
    scheme = request.headers.get('X-Forwarded-Proto', request.scheme)  # 프록시 뒤 https 인식
    return f'{scheme}://{host}'


def cleanup_rooms():
    """[남용방지] 오래된 방을 주기적으로 정리해 메모리 누수를 막는다.
    인터넷에 공개하면 누구나 방을 만들 수 있으므로 자동 청소가 반드시 필요하다."""
    while True:
        time.sleep(600)   # 10분마다 점검
        now = time.time()
        with rooms_lock:
            stale = [rid for rid, r in rooms.items()
                     if now - r.get('created_at', now) > ROOM_TTL]
            for rid in stale:
                rooms.pop(rid, None)
            # 사라진 방을 가리키는 소켓 매핑도 함께 정리
            dead = [s for s, rid in sid_to_room.items() if rid not in rooms]
            for s in dead:
                sid_to_room.pop(s, None)


def auto_starter():
    """[예약 시작] 지정 시각(start_at, Unix타임스탬프)이 되면 자동으로 타이머를 시작한다.
    '절대 시각'으로 판단하므로 서버 시간대와 무관하게 정확하다."""
    while True:
        time.sleep(1)
        now = time.time()
        to_start = []
        with rooms_lock:
            for rid, room in rooms.items():
                if room.get('status') == 'scheduled' and room.get('start_at') and now >= room['start_at']:
                    room['status'] = 'running'
                    room['start_at'] = None
                    room['epoch'] += 1
                    to_start.append((rid, room['epoch'], _snapshot(room)))
        for rid, ep, snap in to_start:
            socketio.emit('tick', snap, room=rid)
            threading.Thread(target=run_timer, args=(rid, ep), daemon=True).start()


def run_timer(room_id, my_epoch):
    """1초마다 남은 시간을 줄이고 모든 접속자에게 방송한다.
    [버그①수정] my_epoch 가 방의 현재 epoch 와 다르면 '옛 작업'이므로 스스로 종료.
    → 진행 중 재시작/빠른 pause→start 시에도 작업이 절대 중복되지 않는다."""
    while True:
        time.sleep(1)
        fired_msg = None
        with rooms_lock:
            room = rooms.get(room_id)
            if room is None:
                return
            if room['epoch'] != my_epoch:   # 새 세대가 시작됨 → 나는 옛 작업, 종료
                return
            if room['status'] != 'running':
                return
            if room['remaining'] > 0:
                room['remaining'] -= 1
            # [예약 메시지] 남은시간이 예약 시각 이하가 되면 자동 발동(각 1회)
            for item in room['schedule']:
                if not item.get('fired') and room['remaining'] <= item['at']:
                    item['fired'] = True
                    room['message'] = item['text']
                    fired_msg = item['text']
            if room['remaining'] <= 0:
                room['status'] = 'finished'
                snap = _snapshot(room)
                socketio.emit('tick', snap, room=room_id)
                if fired_msg is not None:
                    socketio.emit('message', {'message': fired_msg}, room=room_id)
                socketio.emit('finished', snap, room=room_id)
                return
            snap = _snapshot(room)
        socketio.emit('tick', snap, room=room_id)
        if fired_msg is not None:   # 예약 메시지 자동 발동 → 전원 표시 + 알림음
            socketio.emit('message', {'message': fired_msg}, room=room_id)


def _snapshot(room):
    """외부로 보낼 안전한 상태값.
    [보안] control_token 은 절대 포함하지 않는다."""
    return {
        'remaining': room['remaining'],
        'duration': room['duration'],
        'status': room['status'],
        'viewers': room['viewers'],
        'message': room.get('message', ''),   # 화면에 띄운 메시지(없으면 빈 값)
        'start_at': room.get('start_at'),      # 예약 시작 시각(대기 화면 카운트다운용)
    }


@app.route('/')
def home():
    return PAGE_HOME


@app.route('/create', methods=['POST'])
def create():
    data = request.get_json(force=True, silent=True) or {}
    try:
        minutes = int(data.get('minutes', 5))
        seconds = int(data.get('seconds', 0))
    except (ValueError, TypeError):
        return jsonify({'error': '시간 형식이 올바르지 않습니다.'}), 400

    total = minutes * 60 + seconds
    if total <= 0:
        return jsonify({'error': '1초 이상으로 설정해주세요.'}), 400
    if total > 24 * 3600:
        return jsonify({'error': '최대 24시간까지 설정 가능합니다.'}), 400

    usage = str(data.get('usage', '미선택'))[:20]
    room_id = uuid.uuid4().hex[:6]
    control_token = uuid.uuid4().hex

    with rooms_lock:
        # [남용방지] 방이 너무 많으면 새로 만들지 않는다 (메모리 보호).
        if len(rooms) >= MAX_ROOMS:
            return jsonify({'error': '현재 생성된 타이머가 너무 많습니다. 잠시 후 다시 시도해주세요.'}), 429
        rooms[room_id] = {
            'control_token': control_token,
            'duration': total, 'remaining': total,
            'status': 'idle', 'usage': usage,
            'viewers': 0,
            'epoch': 0,                 # [버그①수정] 타이머 작업 세대번호
            'created_at': time.time(),  # [남용방지] 자동 정리 기준 시각
            'message': '',              # 실시간 메시지(공대장 지시 등). 컨트롤러만 변경 가능
            'schedule': [],             # 예약 메시지 [{at:남은초, text, fired}]. 컨트롤러만 변경
            'start_at': None,           # 예약 시작 시각(Unix타임스탬프). None=예약 없음
        }

    # 뷰어용 절대주소(폰/외부에서 바로 열림). 로컬=LAN IP, 배포=공개 도메인으로 자동 결정.
    base = public_base_url()
    return jsonify({
        'room_id': room_id,
        'viewer_url': f'/v/{room_id}',
        'control_url': f'/c/{room_id}/{control_token}',
        'viewer_lan_url': f'{base}/v/{room_id}',   # 뷰어용 절대주소
    })


@app.route('/qr/<room_id>')
def qr_code(room_id):
    """뷰어 링크의 QR 코드(SVG)를 돌려준다.
    [보안] 뷰어 링크만 QR 로 만든다. 컨트롤러 토큰은 절대 포함하지 않는다.
    [편의] localhost 접속이어도 폰이 닿도록 LAN IP 를 강제로 사용한다."""
    with rooms_lock:
        exists = room_id in rooms
    if not exists:
        return PAGE_NOT_FOUND, 404
    viewer_url = f'{public_base_url()}/v/{room_id}'
    img = qrcode.make(viewer_url, image_factory=qrcode.image.svg.SvgImage)
    buf = BytesIO()
    img.save(buf)
    resp = app.response_class(buf.getvalue(), mimetype='image/svg+xml')
    resp.headers['Cache-Control'] = 'no-store'
    return resp


@app.route('/v/<room_id>')
def viewer(room_id):
    with rooms_lock:
        exists = room_id in rooms
    if not exists:
        return PAGE_NOT_FOUND, 404
    return render_timer_page(room_id, is_controller=False, token='')


@app.route('/c/<room_id>/<token>')
def controller(room_id, token):
    with rooms_lock:
        room = rooms.get(room_id)
        valid = room is not None and room['control_token'] == token
    if not valid:
        return PAGE_NOT_FOUND, 404
    return render_timer_page(room_id, is_controller=True, token=token)


@socketio.on('join')
def on_join(data):
    room_id = data.get('room_id')
    sid = request.sid
    with rooms_lock:
        room = rooms.get(room_id)
        if room is None:
            emit('error_msg', {'message': '존재하지 않는 타이머입니다.'})
            return
        # [버그②수정] 같은 소켓의 중복 join 은 인원을 또 세지 않는다.
        if sid not in sid_to_room:
            if room['viewers'] >= MAX_VIEWERS:
                emit('error_msg', {'message': f'최대 {MAX_VIEWERS}명까지 접속할 수 있습니다.'})
                return
            room['viewers'] += 1
            sid_to_room[sid] = room_id
        snap = _snapshot(room)
    join_room(room_id)
    emit('state', snap)
    socketio.emit('tick', snap, room=room_id)


def _check_controller(data):
    """[보안 핵심] 서버에서 토큰을 직접 검증. 클라이언트 말을 믿지 않는다."""
    room_id = data.get('room_id')
    token = data.get('token')
    room = rooms.get(room_id)
    if room is None or room['control_token'] != token:
        return None, None
    return room_id, room


@socketio.on('control')
def on_control(data):
    action = data.get('action')
    start_thread = False
    thread_epoch = None
    msg_changed = False
    sched_changed = False
    with rooms_lock:
        room_id, room = _check_controller(data)
        if room is None:
            emit('error_msg', {'message': '제어 권한이 없습니다.'})
            return

        if action == 'start':
            # [버그①수정] idle/paused → running 으로 '실제 전환될 때만'
            # 세대번호를 올리고 새 작업을 시작한다. 이미 running 이면 무시.
            if room['status'] in ('idle', 'paused') and room['remaining'] > 0:
                room['status'] = 'running'
                room['epoch'] += 1
                start_thread = True
                thread_epoch = room['epoch']
        elif action == 'pause':
            if room['status'] == 'running':
                room['status'] = 'paused'
        elif action == 'reset':
            room['status'] = 'idle'
            room['remaining'] = room['duration']
            room['epoch'] += 1      # 돌고 있던 옛 작업도 함께 종료시킴
            room['start_at'] = None # 예약 시작도 함께 취소
            for item in room['schedule']:
                item['fired'] = False   # 예약 메시지도 다시 발동 가능하게 초기화
        elif action == 'adjust':
            try:
                delta = int(data.get('delta', 0))
            except (ValueError, TypeError):
                delta = 0
            room['remaining'] = max(0, min(24 * 3600, room['remaining'] + delta))
        elif action == 'message':
            # [보안] 길이만 제한. 화면에는 textContent 로 넣어 XSS 를 원천 차단한다.
            room['message'] = str(data.get('text', ''))[:200]
            msg_changed = True
        elif action == 'clear_message':
            room['message'] = ''
            msg_changed = True
        elif action == 'schedule_add':
            try:
                at = int(data.get('at', 0))
            except (ValueError, TypeError):
                at = 0
            at = max(0, min(24 * 3600, at))
            text = str(data.get('text', ''))[:200]
            if text and at > 0:
                room['schedule'].append({'at': at, 'text': text, 'fired': False})
                room['schedule'].sort(key=lambda x: -x['at'])   # 먼 시간(먼저 발동)부터 정렬
            sched_changed = True
        elif action == 'schedule_remove':
            try:
                idx = int(data.get('index', -1))
            except (ValueError, TypeError):
                idx = -1
            if 0 <= idx < len(room['schedule']):
                room['schedule'].pop(idx)
            sched_changed = True
        elif action == 'schedule_clear':
            room['schedule'] = []
            sched_changed = True
        elif action == 'schedule_start':
            # [예약 시작] 컨트롤러 브라우저가 계산한 '절대 시각(ms)'을 받는다.
            # → 서버 시간대와 무관하게 정확. 그 시각까지 status='scheduled'.
            try:
                ms = float(data.get('start_at_ms', 0))
            except (ValueError, TypeError):
                ms = 0
            if ms > 0:
                room['status'] = 'scheduled'
                room['start_at'] = ms / 1000.0
                room['remaining'] = room['duration']   # 예약 시 시간 초기화
                room['epoch'] += 1                      # 돌던 작업 있으면 중단
        elif action == 'cancel_start':
            if room['status'] == 'scheduled':
                room['status'] = 'idle'
                room['start_at'] = None
        else:
            emit('error_msg', {'message': '알 수 없는 명령입니다.'})
            return

        snap = _snapshot(room)
        sched_copy = list(room['schedule'])

    socketio.emit('tick', snap, room=room_id)
    if msg_changed:   # 메시지 전용 이벤트(알림음 트리거용). 새 메시지=소리, 지우기=조용히
        socketio.emit('message', {'message': snap['message']}, room=room_id)
    if sched_changed:  # [보안] 예약 목록은 조작한 컨트롤러 본인에게만 회신(뷰어엔 노출 안 함)
        emit('schedule_state', {'schedule': sched_copy})
    if start_thread:   # 스레드 시작은 lock 밖에서 (데드락 방지)
        threading.Thread(target=run_timer, args=(room_id, thread_epoch), daemon=True).start()


@socketio.on('get_schedule')
def on_get_schedule(data):
    """컨트롤러가 자기 화면에 예약 목록을 불러올 때. [보안] 토큰 검증 후 본인에게만 회신."""
    with rooms_lock:
        room_id, room = _check_controller(data)
        if room is None:
            return
        sched = list(room['schedule'])
    emit('schedule_state', {'schedule': sched})


@socketio.on('disconnect')
def on_disconnect():
    # [버그②수정] 나간 소켓이 있던 방의 인원을 정확히 1 차감한다.
    sid = request.sid
    snap = None
    room_id = None
    with rooms_lock:
        room_id = sid_to_room.pop(sid, None)
        if room_id is not None:
            room = rooms.get(room_id)
            if room and room['viewers'] > 0:
                room['viewers'] -= 1
                snap = _snapshot(room)
    if snap is not None:
        socketio.emit('tick', snap, room=room_id)


def render_timer_page(room_id, is_controller, token):
    controls_html = CONTROLS_HTML if is_controller else ''
    role_label = '진행자 화면' if is_controller else '뷰어 화면'
    return TIMER_PAGE_TEMPLATE \
        .replace('{{ROOM_ID}}', room_id) \
        .replace('{{TOKEN}}', token) \
        .replace('{{IS_CONTROLLER}}', 'true' if is_controller else 'false') \
        .replace('{{STAGE}}', 'ctrl' if is_controller else 'view') \
        .replace('{{CONTROLS}}', controls_html) \
        .replace('{{ROLE_LABEL}}', role_label)


PAGE_HOME = '''
<!DOCTYPE html><html lang="ko"><head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>모두의 타이머 · 만들기</title>
<meta name="description" content="링크·QR 하나로 다같이 보는 실시간 타이머">
<meta property="og:title" content="모두의 타이머">
<meta property="og:description" content="링크·QR 하나로 다같이 보는 실시간 타이머">
<meta property="og:type" content="website">
<style>
* { box-sizing: border-box; }
body { font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;
 background:#0f172a;color:#e2e8f0;margin:0;padding:20px;min-height:100vh;
 display:flex;align-items:center;justify-content:center; }
.card { background:#1e293b;padding:32px;border-radius:16px;width:100%;max-width:420px;
 box-shadow:0 10px 40px rgba(0,0,0,.4); }
h1 { font-size:22px;margin:0 0 4px; }
.sub { color:#94a3b8;font-size:14px;margin-bottom:24px; }
label { display:block;font-size:13px;color:#cbd5e1;margin:16px 0 6px; }
.time-row { display:flex;gap:10px; }
input,select { width:100%;padding:12px;border-radius:10px;border:1px solid #334155;
 background:#0f172a;color:#e2e8f0;font-size:16px; }
button { width:100%;padding:14px;margin-top:24px;border:none;border-radius:10px;
 background:#3b82f6;color:#fff;font-size:16px;font-weight:600;cursor:pointer; }
.result { margin-top:24px;display:none; }
.link-box { background:#0f172a;border:1px solid #334155;border-radius:10px;padding:14px;margin:10px 0; }
.link-box .title { font-size:12px;color:#94a3b8;margin-bottom:6px; }
.link-box .desc { font-size:11px;color:#64748b;margin-top:6px; }
.link-row { display:flex;gap:8px; }
.link-row input { font-size:13px; }
.copy-btn { width:auto;padding:0 16px;margin:0;background:#475569;white-space:nowrap; }
.open-btn { background:#22c55e;margin-top:8px; }
.qr-wrap { margin-top:12px;text-align:center;background:#fff;border-radius:10px;padding:16px; }
.qr-wrap img { width:190px;height:190px;display:block;margin:0 auto; }
.qr-hint { font-size:12px;color:#0f172a;margin-top:10px;line-height:1.45; }
</style></head><body>
<div class="card">
 <h1>모두의 타이머</h1>
 <div class="sub">링크·QR 하나로 다같이 보는 실시간 타이머</div>
 <label>시간 설정</label>
 <div class="time-row">
  <div><input type="number" id="minutes" value="5" min="0" max="1440">
   <div style="font-size:11px;color:#64748b;text-align:center;margin-top:4px;">분</div></div>
  <div><input type="number" id="seconds" value="0" min="0" max="59">
   <div style="font-size:11px;color:#64748b;text-align:center;margin-top:4px;">초</div></div>
 </div>
 <label>어디에 쓰시나요? <span style="color:#64748b;">(선택)</span></label>
 <select id="usage">
  <option value="미선택">선택 안 함</option>
  <option value="발표">발표 / 프레젠테이션</option>
  <option value="회의">회의 / 미팅</option>
  <option value="수업">수업 / 강의</option>
  <option value="면접심사">면접 / 심사</option>
  <option value="행사">행사 / 공연</option>
  <option value="운동">운동 / 트레이닝</option>
  <option value="기타">기타</option>
 </select>
 <button onclick="createTimer()">타이머 만들기</button>
 <div class="result" id="result">
  <div class="link-box">
   <div class="title">뷰어 링크 (발표자·청중에게)</div>
   <div class="link-row"><input type="text" id="viewerUrl" readonly>
    <button class="copy-btn" onclick="copy('viewerUrl')">복사</button></div>
   <div class="desc">시간만 볼 수 있습니다. 제어는 불가합니다.</div>
   <div class="qr-wrap" id="qrWrap" style="display:none;">
    <img id="qrImg" alt="뷰어 접속 QR 코드">
    <div class="qr-hint">📱 사람들에게 이 QR을 보여주세요.<br>폰 카메라로 찍으면 바로 접속됩니다. (모두 같은 와이파이 필요)</div>
   </div>
  </div>
  <div class="link-box">
   <div class="title">컨트롤러 링크 (진행자 본인만)</div>
   <div class="link-row"><input type="text" id="controlUrl" readonly>
    <button class="copy-btn" onclick="copy('controlUrl')">복사</button></div>
   <div class="desc">시작·정지·리셋이 가능합니다. 남에게 주지 마세요.</div>
   <button class="open-btn" onclick="openControl()">내 컨트롤러 화면 열기</button>
  </div>
 </div>
</div>
<script>
let controlPath='';
async function createTimer(){
 const minutes=document.getElementById('minutes').value;
 const seconds=document.getElementById('seconds').value;
 const usage=document.getElementById('usage').value;
 const res=await fetch('/create',{method:'POST',
  headers:{'Content-Type':'application/json'},
  body:JSON.stringify({minutes,seconds,usage})});
 const data=await res.json();
 if(data.error){alert(data.error);return;}
 const origin=window.location.origin;
 // 뷰어 주소는 폰에서도 바로 열리는 LAN 주소로 보여준다(localhost 함정 방지).
 document.getElementById('viewerUrl').value=data.viewer_lan_url||(origin+data.viewer_url);
 document.getElementById('controlUrl').value=origin+data.control_url;
 controlPath=data.control_url;
 // 뷰어 링크 QR 표시 (다수가 폰으로 동시 접속 가능)
 document.getElementById('qrImg').src='/qr/'+data.room_id;
 document.getElementById('qrWrap').style.display='block';
 document.getElementById('result').style.display='block';
}
function copy(id){const el=document.getElementById(id);el.select();
 document.execCommand('copy');alert('복사되었습니다!');}
function openControl(){if(controlPath)window.location.href=controlPath;}
</script></body></html>
'''


CONTROLS_HTML = '''
<div class="panel">
 <div class="panel-t">재생 컨트롤</div>
 <div class="row">
  <button class="btn primary" onclick="ctrl('start')">시작</button>
  <button class="btn" onclick="ctrl('pause')">일시정지</button>
  <button class="btn" onclick="ctrl('reset')">리셋</button>
 </div>
</div>
<div class="panel">
 <div class="panel-t">시간 미세조정</div>
 <div class="row">
  <button class="btn" onclick="adjust(-60)">-1분</button>
  <button class="btn" onclick="adjust(-10)">-10초</button>
  <button class="btn" onclick="adjust(10)">+10초</button>
  <button class="btn" onclick="adjust(60)">+1분</button>
 </div>
</div>
<div class="panel">
 <div class="panel-t">즉석 메시지 <span class="panel-sub">전원 화면에 크게 표시</span></div>
 <input class="field" type="text" id="msgInput" maxlength="200"
   placeholder="예: 잠시 후 재개합니다"
   onkeydown="if(event.key==='Enter')sendMessage()">
 <div class="row">
  <button class="btn sky" onclick="sendMessage()">메시지 표시</button>
  <button class="btn" onclick="clearMessage()">지우기</button>
 </div>
</div>
<div class="panel">
 <div class="panel-t">예약 시작 <span class="panel-sub">지정 시각에 자동 시작</span></div>
 <input class="field" type="time" id="startTime">
 <div class="row">
  <button class="btn sky" onclick="scheduleStart()">이 시각에 시작</button>
  <button class="btn" onclick="cancelStart()">예약 취소</button>
 </div>
</div>
<div class="panel">
 <div class="panel-t">예약 메시지 <span class="panel-sub">남은 시간이 되면 자동 표시</span></div>
 <div class="row">
  <input class="field num" type="number" id="schMin" min="0" placeholder="분" value="1">
  <input class="field num" type="number" id="schSec" min="0" max="59" placeholder="초" value="0">
  <input class="field" type="text" id="schText" maxlength="200" placeholder="표시할 메시지"
    onkeydown="if(event.key==='Enter')addSchedule()" style="flex:1;min-width:130px;">
 </div>
 <button class="btn sky" onclick="addSchedule()">예약 추가</button>
 <div class="sched-list" id="schedList"></div>
</div>
'''


TIMER_PAGE_TEMPLATE = '''
<!DOCTYPE html><html lang="ko"><head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1, maximum-scale=1">
<title>모두의 타이머</title>
<meta name="description" content="링크·QR 하나로 다같이 보는 실시간 타이머">
<meta property="og:title" content="모두의 타이머">
<meta property="og:description" content="링크·QR 하나로 다같이 보는 실시간 타이머">
<meta property="og:type" content="website">
<script src="https://cdnjs.cloudflare.com/ajax/libs/socket.io/4.5.4/socket.io.min.js"></script>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@600;700&display=swap" rel="stylesheet">
<style>
* { box-sizing:border-box;margin:0;padding:0; }
html,body { min-height:100%; }
body { font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;
 background:#0f172a;color:#f1f5f9;overflow-y:auto;transition:background .4s; }
body.warn { background:#3f2e10; }
body.danger { background:#3d1417; }
body.done { background:#2a0f12; }
.stage { max-width:600px;margin:0 auto;min-height:100dvh;
 display:flex;flex-direction:column;align-items:center;justify-content:center;
 gap:14px;padding:20px 16px 44px; }
.topbar { width:100%;display:flex;justify-content:space-between;align-items:center;
 gap:8px;flex-wrap:wrap;margin-bottom:2px; }
.tb-right { display:flex;gap:6px;flex-wrap:wrap; }
.badge { font-size:12px;color:#94a3b8;background:#16233b;border:1px solid #2c3a56;
 padding:6px 12px;border-radius:999px;white-space:nowrap; }
.badge b { color:#e2e8f0; }
button.badge { cursor:pointer;color:#cbd5e1; }
button.badge.on { color:#7dd3fc;border-color:#35507e; }
.timer { font-family:'IBM Plex Mono',ui-monospace,monospace;font-weight:700;
 line-height:1;letter-spacing:1px;color:#f8fafc;transition:color .3s; }
.stage.view .timer { font-size:clamp(80px,22vw,320px); }
.stage.ctrl .timer { font-size:clamp(56px,13vw,132px); }
@keyframes blink { 0%,100%{opacity:1;} 50%{opacity:.2;} }
body.done .timer { animation:blink 1s steps(1) infinite; }
.status-text { font-size:15px;color:#94a3b8;min-height:20px;text-align:center; }
.msg-box { display:none;font-size:clamp(22px,5vw,46px);font-weight:800;color:#fde047;
 text-align:center;padding:2px 12px;max-width:96vw;word-break:keep-all;line-height:1.3;
 text-shadow:0 2px 10px rgba(0,0,0,.5); }
@keyframes msgpop { 0%{transform:scale(.85);opacity:0;} 100%{transform:scale(1);opacity:1;} }
.msg-box.show { display:block;animation:msgpop .25s ease-out; }
.panel { width:100%;background:#16233b;border:1px solid #2c3a56;border-radius:18px;
 padding:16px;display:flex;flex-direction:column;gap:10px; }
.panel-t { font-size:13px;font-weight:700;color:#cbd5e1;letter-spacing:.3px; }
.panel-sub { font-size:12px;font-weight:400;color:#5b6b86;margin-left:6px; }
.row { display:flex;gap:10px;flex-wrap:wrap; }
.btn { flex:1;min-width:84px;font-size:17px;font-weight:600;padding:14px 12px;
 border:1px solid #2c3a56;border-radius:14px;cursor:pointer;
 background:#1c2b47;color:#e2e8f0;transition:filter .15s; }
.btn:active { filter:brightness(1.25); }
.btn.primary { background:#22c55e;color:#052e16;border-color:transparent; }
.btn.sky { background:#38bdf8;color:#052538;border-color:transparent; }
.field { width:100%;padding:13px 14px;border-radius:12px;border:1px solid #2b4066;
 background:#0d1830;color:#e2e8f0;font-size:16px; }
.field.num { width:76px;flex:0 0 auto;text-align:center; }
.sched-list { display:flex;flex-direction:column;gap:6px;margin-top:2px; }
.sched-row { display:flex;justify-content:space-between;align-items:center;gap:8px;
 background:#0d1830;border:1px solid #223150;border-radius:12px;padding:9px 12px;font-size:14px;color:#e2e8f0; }
.sched-row.fired { opacity:.4;text-decoration:line-through; }
.sched-row button { padding:7px 12px;border:none;border-radius:9px;background:#2c3a56;
 color:#e2e8f0;font-size:13px;cursor:pointer;flex-shrink:0; }
</style></head><body>
<div class="stage {{STAGE}}">
<div class="topbar">
 <span class="badge">{{ROLE_LABEL}}</span>
 <div class="tb-right">
  <span class="badge">접속 <b id="viewerCount">0</b></span>
  <button class="badge" id="alarmBadge" onclick="enableSound()">🔔 소리 켜기</button>
  <button class="badge" onclick="toggleFullscreen()">⛶ 전체화면</button>
 </div>
</div>
<div class="msg-box" id="msgBox"></div>
<div class="timer" id="timer">--:--</div>
<div class="status-text" id="statusText">연결 중...</div>
{{CONTROLS}}
</div>
<script>
const ROOM_ID='{{ROOM_ID}}';
const TOKEN='{{TOKEN}}';
const IS_CONTROLLER={{IS_CONTROLLER}};
const socket=io();
const timerEl=document.getElementById('timer');
const statusEl=document.getElementById('statusText');
const viewerEl=document.getElementById('viewerCount');

// --- 종료 알림음 / 진동 ---
// [참고] 추가 음원 파일 없이 브라우저 내장 Web Audio 로 비프음을 만든다.
let audioCtx=null, alarmed=false;
function getCtx(){
 if(!audioCtx){ const AC=window.AudioContext||window.webkitAudioContext; if(AC) audioCtx=new AC(); }
 return audioCtx;
}
function beep(freq,dur,when){
 const ctx=getCtx(); if(!ctx) return;
 const t=ctx.currentTime+(when||0);
 const osc=ctx.createOscillator(), g=ctx.createGain();
 osc.connect(g); g.connect(ctx.destination);
 osc.type='sine'; osc.frequency.value=freq;
 g.gain.setValueAtTime(0.0001,t);
 g.gain.exponentialRampToValueAtTime(0.4,t+0.02);
 g.gain.exponentialRampToValueAtTime(0.0001,t+dur);
 osc.start(t); osc.stop(t+dur+0.03);
}
// [모바일 대응] 폰 브라우저는 사용자가 화면을 한 번 누르기 전엔 소리를 막는다.
// '소리 켜기' 버튼으로 잠금을 풀고, 짧은 테스트음을 들려준다.
function enableSound(){
 const ctx=getCtx(); if(!ctx) return;
 if(ctx.state==='suspended') ctx.resume();
 beep(660,0.15,0);
 const b=document.getElementById('alarmBadge');
 b.textContent='🔔 소리 켜짐'; b.classList.add('on');
}
function playAlarm(){
 if(alarmed) return; alarmed=true;        // 종료 시 1회만 울림
 beep(880,0.3,0); beep(880,0.3,0.42); beep(988,0.5,0.84);
 if(navigator.vibrate) navigator.vibrate([300,150,300,150,400]);  // 안드로이드 진동
}
// 화면을 누르면 오디오 잠금 해제(컨트롤러는 버튼 조작 시 자동 해제됨)
function tryUnlock(){ const ctx=getCtx(); if(ctx && ctx.state==='suspended') ctx.resume(); }
document.addEventListener('click', tryUnlock);
document.addEventListener('touchstart', tryUnlock, {passive:true});

socket.on('connect',()=>{ socket.emit('join',{room_id:ROOM_ID}); if(IS_CONTROLLER) socket.emit('get_schedule',{room_id:ROOM_ID,token:TOKEN}); });
socket.on('schedule_state',(d)=>{ renderSchedule(d.schedule||[]); });
socket.on('state',render);
socket.on('tick',render);
socket.on('finished',render);
socket.on('message',(d)=>{ showMessage(d.message); if(d.message) messageBeep(); });
socket.on('error_msg',(d)=>{ statusEl.textContent='주의: '+d.message; });
function render(s){
 if(s.viewers!==undefined) viewerEl.textContent=s.viewers;
 if(s.message!==undefined) showMessage(s.message);
 if(s.status==='scheduled'){ startScheduledView(s.start_at); return; }
 stopScheduledView();
 if(s.remaining===undefined) return;
 timerEl.textContent=fmt(s.remaining);
 const map={idle:'대기 중',running:'진행 중',paused:'일시정지',finished:'⏰ 시간 종료!'};
 statusEl.textContent=map[s.status]||'';
 if(s.status==='finished'){
  document.body.className='done';
  playAlarm();
 } else {
  alarmed=false;   // 리셋되면 다음 종료에 다시 울리도록 초기화
  if(s.remaining<=30){ document.body.className='danger'; }
  else if(s.remaining<=120){ document.body.className='warn'; }
  else { document.body.className=''; }
 }
}
function fmt(total){
 const h=Math.floor(total/3600),m=Math.floor((total%3600)/60),s=total%60;
 const pad=(n)=>String(n).padStart(2,'0');
 return h>0?`${pad(h)}:${pad(m)}:${pad(s)}`:`${pad(m)}:${pad(s)}`;
}
function ctrl(action){ socket.emit('control',{room_id:ROOM_ID,token:TOKEN,action}); }
function adjust(delta){ socket.emit('control',{room_id:ROOM_ID,token:TOKEN,action:'adjust',delta}); }
// --- 실시간 메시지 ---
function showMessage(msg){
 const box=document.getElementById('msgBox'); if(!box) return;
 box.textContent = msg || '';           // [보안] textContent 사용 → XSS 원천 차단
 if(msg){ box.classList.add('show'); } else { box.classList.remove('show'); }
}
function messageBeep(){ beep(1046,0.14,0); beep(1319,0.2,0.15); }  // 딩-동(종료음과 구분)
function sendMessage(){
 const inp=document.getElementById('msgInput'); if(!inp) return;
 const t=inp.value.trim(); if(!t) return;
 socket.emit('control',{room_id:ROOM_ID,token:TOKEN,action:'message',text:t});
 inp.value='';
}
function clearMessage(){ socket.emit('control',{room_id:ROOM_ID,token:TOKEN,action:'clear_message'}); }
// --- 예약 메시지 (컨트롤러 전용) ---
function addSchedule(){
 const m=parseInt(document.getElementById('schMin').value)||0;
 const s=parseInt(document.getElementById('schSec').value)||0;
 const at=m*60+s;
 const t=document.getElementById('schText').value.trim();
 if(!t||at<=0) return;
 socket.emit('control',{room_id:ROOM_ID,token:TOKEN,action:'schedule_add',at:at,text:t});
 document.getElementById('schText').value='';
}
function removeSchedule(i){ socket.emit('control',{room_id:ROOM_ID,token:TOKEN,action:'schedule_remove',index:i}); }
function renderSchedule(list){
 const box=document.getElementById('schedList'); if(!box) return;
 box.textContent='';                        // 목록 초기화
 list.forEach((item,i)=>{
  const row=document.createElement('div');
  row.className='sched-row'+(item.fired?' fired':'');
  const t=Math.floor(item.at/60)+':'+String(item.at%60).padStart(2,'0');
  const label=document.createElement('span');
  label.textContent=t+'  →  '+item.text;    // [보안] textContent = XSS 원천 차단
  const del=document.createElement('button');
  del.textContent='삭제'; del.onclick=()=>removeSchedule(i);
  row.appendChild(label); row.appendChild(del);
  box.appendChild(row);
 });
}
// --- 예약 시작 (지정 시각에 자동 시작) ---
let schedInterval=null, schedStartAt=0;
function scheduleStart(){
 const v=document.getElementById('startTime').value;   // "15:00"
 if(!v) return;
 const p=v.split(':'); const h=parseInt(p[0]), m=parseInt(p[1]);
 const d=new Date(); d.setHours(h,m,0,0);
 if(d.getTime()<=Date.now()) d.setDate(d.getDate()+1);  // 이미 지난 시각이면 다음날로
 socket.emit('control',{room_id:ROOM_ID,token:TOKEN,action:'schedule_start',start_at_ms:d.getTime()});
}
function cancelStart(){ socket.emit('control',{room_id:ROOM_ID,token:TOKEN,action:'cancel_start'}); }
function startScheduledView(startAt){
 schedStartAt=startAt||0;
 statusEl.textContent='예약 대기 중 · '+fmtClock(schedStartAt)+' 시작';
 document.body.className='';
 updateSchedCountdown();
 if(!schedInterval) schedInterval=setInterval(updateSchedCountdown,250);
}
function stopScheduledView(){ if(schedInterval){ clearInterval(schedInterval); schedInterval=null; } }
function updateSchedCountdown(){
 const remain=Math.max(0,Math.ceil(schedStartAt-Date.now()/1000));
 timerEl.textContent=fmt(remain);
}
function fmtClock(ts){
 const d=new Date(ts*1000);
 const h=d.getHours(), m=d.getMinutes();
 const ap=h<12?'오전':'오후'; let hh=h%12; if(hh===0)hh=12;
 return ap+' '+hh+':'+String(m).padStart(2,'0');
}
function toggleFullscreen(){
 if(!document.fullscreenElement){ document.documentElement.requestFullscreen().catch(()=>{}); }
 else { document.exitFullscreen(); }
}
</script></body></html>
'''


PAGE_NOT_FOUND = '''
<!DOCTYPE html><html lang="ko"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<style>body{font-family:sans-serif;background:#0f172a;color:#e2e8f0;display:flex;
height:100vh;align-items:center;justify-content:center;margin:0;text-align:center;}
a{color:#3b82f6;}</style></head>
<body><div><h1>타이머를 찾을 수 없습니다</h1>
<p>링크가 만료되었거나 잘못되었습니다.<br><a href="/">새 타이머 만들기</a></p></div></body></html>
'''


# [남용방지] 오래된 방 자동 정리 스레드 시작.
# 모듈 로드 시 실행되므로 개발 실행(python app.py)·배포 실행(gunicorn) 모두에서 동작한다.
threading.Thread(target=cleanup_rooms, daemon=True).start()
threading.Thread(target=auto_starter, daemon=True).start()


if __name__ == '__main__':
    # [환경] 포트는 환경변수 PORT 로 바꿀 수 있게 둠 (기본 5050).
    #  - 이 PC는 5000번을 다른 프로그램(자동매매 제어판)이 쓰고 있어 5050 사용.
    #  - 개발팀 전달 시: 환경변수 PORT 로 자유롭게 변경 가능. (예: set PORT=8000)
    PORT = int(os.environ.get('PORT', 5050))
    ip = get_local_ip()
    print("=" * 60)
    print("  모두의 타이머 시작됨")
    print("=" * 60)
    print(f"  이 컴퓨터:         http://localhost:{PORT}")
    print(f"  같은 와이파이 기기: http://{ip}:{PORT}")
    print("=" * 60)
    print("  (Ctrl+C 로 종료)")
    socketio.run(app, host='0.0.0.0', port=PORT, debug=False, allow_unsafe_werkzeug=True)
