아래는 `sftp.yaml` + `sftp_v2.py`를 실제 운영에 투입하기 위한 **아주 상세한 사용 가이드**입니다.
(예제와 체크리스트 → 설치/설정 → 실행 레시피 → 튜닝/보안 → 문제 해결 → FAQ 순으로 구성)

---

## 코드 패키지 구조 (요약)

- `sftp_v2.py`: 최상위 실행 스크립트 (기존 경로 유지)
- `sftp_tool/`: 패키지화된 구현
  - `config.py`: YAML/ENV 로딩, 데이터 클래스
  - `cli.py`: CLI 옵션 정의
  - `uploader.py`: 업로드 파이프라인, 레포트/알림
  - `special_ops.py`: 원격 디렉터리/SSH/Elasticsearch 유틸리티
  - `main.py`: 엔트리포인트 조립

## 0) 한눈에 보는 구성 & 핵심 개념

* **실행 파일**: `sftp_v2.py`

  * `sftp.yaml`의 값을 읽어 **SFTP 접속/전송 정책/필터/보고서/알림**까지 자동화
  * **부분 재개(Resume)**, **전역/파일별 속도 제한(토큰버킷)**, **서버별 정책**, **전체/개별 진행률** 제공
* **환경 파일**: `sftp.yaml`

  * 모든 하드코딩 제거: **호스트/경로/필터/재시도/속도/보고서/알림/end.touch** 등 전부 중앙관리
* **레포트**: `reports/<YYYYMMDDHHMM>/`에 `summary.json`, `success.csv`, `failed.csv`, `skipped.csv`, (설치 시) `report.xlsx`

> 참고 문헌
>
> * Paramiko SFTP `put(confirm, callback)` 문서: `confirm=True`는 **전송 후 `stat()`로 원격 파일 크기 검증**을 의미합니다. 크기 불일치면 예외가 발생합니다. ([Read the Docs][1])
> * SFTP 파일 열기 모드: `open(..., mode='a')`는 **append(추가 기록)** 의미입니다(“'a' for appending” 명시). ([CCP4 FTP][2])
> * SFTP 채널 튜닝: `from_transport(window_size, max_packet_size)`로 **윈도/패킷 크기 조정** 가능—전송 속도에 영향. ([Read the Docs][1])
> * 와일드카드 필터: Python 표준 `fnmatch` 규칙(`* ? [seq]`). ([Python documentation][3])
> * 속도 제한(토큰버킷) 개념: **장기 평균률 제한 + 버스트 허용**. ([Wikipedia][4])
> * 병렬 업로드: `ThreadPoolExecutor`는 **I/O 바운드 작업 병렬화**에 적합. ([Python documentation][5])

---

## 1) 설치 & 사전 점검

### 1.1 파이썬 환경

* Python 3.9+ 권장 (Windows/WSL/Linux/Mac 모두 지원)
* 필수: `paramiko`

  ```bash
  uv pip install -e "."
  ```
* 선택(엑셀 보고서): `pandas`, `openpyxl`

  ```bash
  uv pip install -e ".[reporting]"
  ```

> Paramiko는 SSHv2 클라이언트 라이브러리로, `SSHClient`로 접속 후 `open_sftp()`에서 `SFTPClient`를 얻어 파일 전송을 수행합니다. ([Paramiko][6])

### 1.2 디렉터리 준비

* `sftp_v2.py`와 **동일 폴더**에 `sftp.yaml`를 둡니다.
* 로컬 데이터 루트(예: `\\Gdp\ai\upload`) 밑에 **처리 순서대로** 하위 폴더를 배치:

  ```
  \\Gdp\ai\upload
  ├─ 250922_2       (신규 파일)
  ├─ 250923_지방    (기존 24개 파일)
  └─ 250923_odt     (추가 파일)
  ```

---

## 2) `sftp.yaml` 작성(모든 설정 중앙관리)

> **필수**: 본인의 환경에 맞게 값 채운 뒤 저장하세요.

```yaml
# defaults 섹션은 공통 설정, profiles.<name>은 환경별 override
default_profile: dev

defaults:
  SFTP_PORT: 22
  SFTP_USERNAME: root
  SFTP_PASSWORD: null  # CLI --password 나 환경변수로 입력
  SFTP_KEY_PATH: ~/.ssh/id_rsa

  REMOTE_ROOT: /remotepath/
  LOCAL_ROOT: "\\Gdp\ai\upload"
  ORDER:
    - 250922_2
    - 250923_기본
    - 250923_odt

  EXCLUDE_EXTS:
    - .mp4
    - .exe
    - .swf
    - .zip
    - .meta

  FILTER_INCLUDE_GLOBS: []
  FILTER_EXCLUDE_GLOBS:
    - "~$*"
    - "*.tmp"

  FILTER_MIN_SIZE: 0
  FILTER_MAX_SIZE: 0
  FILTER_NEWEST_SCOPE: off
  FILTER_NEWEST_COUNT: 1

  REQUIRE_META: true
  CONFIRM_PUT: true
  INCLUDE_ROOT_NAME: true
  SKIP_IF_EXISTS: size_equal
  RESUME_ENABLED: true
  UPLOAD_CHUNK_SIZE: 32768

  RATE_LIMIT_BPS: 0
  RATE_LIMIT_PER_FILE_BPS: 0

  MAX_WORKERS: 5
  RETRIES: 3
  BACKOFF_BASE: 1.0
  BACKOFF_MAX: 10.0
  BACKOFF_JITTER: 0.3

  VERIFY_HASH: true
  HASH_ALGO: sha256
  HASH_MAX_BYTES: 209715200

  PROGRESS: true
  PROGRESS_PRINT_INTERVAL: 0.5

  LOG_FILE: sftp_upload.log
  TIMEOUT: 30
  STRICT_HOST_KEY_CHECKING: false

  CREATE_END_TOUCH: false
  TOUCH_ONLY: false
  END_TOUCH_NAME: end.touch

  QUARANTINE_EXCLUDED: true
  QUARANTINE_FAILED: true

  SLACK_WEBHOOK_URL: null
  SMTP_SERVER: null
  SMTP_PORT: 587
  SMTP_USER: null
  SMTP_PASS: null
  SMTP_USE_TLS: true
  EMAIL_FROM: null
  EMAIL_TO: []

  SERVER_POLICIES:
 * **ELASTICSEARCH**: Elasticsearch/Kibana 연결 정보. `base_url`, `kibana_url`, `api_user/api_pass`, `verify_ssl`, `timeout`, `default_index`, `nodes(SSH 접속 정보)` 등을 설정합니다.
    "192.168.127.12":
      max_workers: 5
      rate_limit_bps: 0
      retries: 3
      sftp_window_size: null
      sftp_max_packet_size: null
    "backup.example.com":
      max_workers: 2
      rate_limit_bps: 1500000
      retries: 5
      verify_hash: true

  ELASTICSEARCH:
    base_url: http://localhost:9200
    kibana_url: http://localhost:5601
    api_user: null
    api_pass: null
    verify_ssl: true
    timeout: 10
    default_index: null
    nodes:
      - name: es-local
        host: 192.168.127.12
        ssh_port: 22
        ssh_user: root
        ssh_key_path: ~/.ssh/id_rsa

profiles:
  dev:
    SFTP_HOST: 192.168.127.12
    ELASTICSEARCH:
      base_url: http://dev-es.example.com:9200
      kibana_url: http://dev-kibana.example.com:5601
      nodes:
        - name: es-dev-1
          host: 192.168.127.12
          ssh_user: root
          ssh_key_path: ~/.ssh/id_rsa
        - name: es-dev-2
          host: 192.168.127.13
          ssh_user: root
          ssh_key_path: ~/.ssh/id_rsa
  prd:
    SFTP_HOST: prd.example.com  # 실제 PRD 호스트로 교체
    REMOTE_ROOT: /remotepath/
    ELASTICSEARCH:
      base_url: https://prd-es.example.com:9243
      kibana_url: https://prd-kibana.example.com:5601
      verify_ssl: true
      nodes:
        - name: es-prd-1
          host: prd-es-node1.example.com
          ssh_user: sftp
          ssh_key_path: ~/.ssh/prd_es
        - name: es-prd-2
          host: prd-es-node2.example.com
          ssh_user: sftp
          ssh_key_path: ~/.ssh/prd_es
```


> **참고**
> - 기존 `sftp.env` (KEY=VALUE) 파일은 `sftp.yaml`로 대체되었습니다. `--env sftp.env`로 레거시 형식을 사용할 수 있지만, YAML defaults/profiles 기반 구성을 권장합니다.
> - `default_profile`를 지정하면 `--profile` 인자를 생략해도 해당 값을 사용합니다. 프로필 이름을 바꿀 때는 `--profile prd` 와 같이 명시하세요.



### 2.1 주요 키 상세

* **REMOTE\_ROOT**: 원격 루트(끝에 `/` 권장).
* **ORDER**: 처리 순서. `--local-dirs`를 쓰면 무시됩니다.
* **EXCLUDE\_EXTS**: 확장자 제외(소문자). `.meta`는 기본적으로 업로드 대상에서 제외되며, **동반 파일 여부(REQUIRE\_META)** 검증에만 사용됩니다.
* **FILTER\_INCLUDE\_GLOBS / FILTER\_EXCLUDE\_GLOBS**: `fnmatch` 패턴. 예) `*.jpg;*.png` / `~$*;*.tmp` 등. (표준 규칙: `* ? [seq]`) ([Python documentation][3])
* **FILTER\_MIN/MAX\_SIZE**: 바이트 단위(0 = 무제한).
* **FILTER\_NEWEST\_SCOPE/COUNT**: 최신 파일만 선택. `per_base`는 각 베이스 폴더별 상위 N개, `global`은 전체 후보 중 상위 N개.
* **SKIP\_IF\_EXISTS**: 원격에 같은 이름 존재 시

  * `size_equal`: **크기 동일**이면 스킵
  * `mtime_ge`: 원격 mtime ≥ 로컬 mtime이면 스킵
  * `never`: 항상 덮어씀/재개
* **RESUME\_ENABLED**: 부분 재개. 원격 크기 < 로컬 크기면 원격 파일을 **append 모드**로 열어 이어쓰기(자세한 동작은 6.2절). ([CCP4 FTP][2])
* **UPLOAD\_CHUNK\_SIZE**: 스트리밍 시 쓰기 청크 크기(바이트).
* **RATE\_LIMIT\_BPS / RATE\_LIMIT\_PER\_FILE\_BPS**: 전역/파일별 **토큰버킷** 속도 제한(0=무제한). 장기 평균률을 제한하면서 단기 버스트를 허용합니다. ([Wikipedia][4])
* **MAX\_WORKERS**: 동시 업로드 스레드 수(네트워크/서버 부하 상황에 맞춰 조정). ([Python documentation][5])
* **VERIFY\_HASH / HASH\_ALGO / HASH\_MAX\_BYTES**: 전송 후 **무결성 검증**(샘플링 가능).
* **STRICT\_HOST\_KEY\_CHECKING**: **1 권장**(보안). `0`이면 `AutoAddPolicy` 로 미등록 호스트키 수락 → MITM 위험. ([CodeQL][7])
* **CREATE\_END\_TOUCH / TOUCH\_ONLY / END\_TOUCH\_NAME**: `end.touch` 자동/수동 생성.
* **SERVER\_POLICIES**: 호스트별 오버라이드(동시성/재시도/속도/윈도·패킷 크기/검증/스킵규칙 등).

  * (고급) `sftp_window_size`, `sftp_max_packet_size`를 지정하면 **SFTP 채널 파라미터 튜닝**이 가능합니다. ([Read the Docs][1])

---

## 3) 빠른 시작 (10분 가이드)

### 3.1 설정 확인

```bash
python sftp_v2.py --profile dev --show-config
```

* `sftp.yaml` 읽은 후 유효한 설정을 한 번에 확인합니다(민감정보는 `***` 마스킹).

### 3.2 기본 업로드(요청하신 순서대로)

```bash
python sftp_v2.py --profile dev
```

* `ORDER=250922_2,250923_지방,250923_odt` 순으로 스캔/필터/업로드
* 진행 중 **개별/전체 진행률 로그**와 **전송 속도(MB/s)** 출력
* 완료 후 `reports/<타임스탬프>/`에 결과 리포트 생성

### 3.3 특정 하위 폴더만 (ORDER 무시)

```bash
python sftp_v2.py --profile dev --local-dirs 250922_2 250923_odt\partA
```

### 3.4 드라이런(검증/리포트만)

```bash
python sftp_v2.py --profile dev --dry-run
```

* 실제 전송 없이 후보 파일, 필터 결과, 통계를 **리포트로만** 생성

### 3.5 `end.touch` 수동 생성만

```bash
# sftp.yaml에서 TOUCH_ONLY=1 로 두고 실행하거나,
python sftp_v2.py --profile prd  # TOUCH_ONLY=1이면 업로드 없이 touch만 수행

### 4) 운영 유틸리티

* **원격 디렉터리 확인**: `python sftp_v2.py --profile dev --list-remote` (기본 `REMOTE_ROOT`, 다른 경로는 `--list-remote /path/to/check`).
* **Elasticsearch 실행 노드 SSH 명령**: `python sftp_v2.py --profile dev --ssh-command "systemctl status elasticsearch"` (특정 노드만: `--ssh-nodes es-dev-1,es-dev-2`).
* **Elasticsearch/Kibana API 조회**: `python sftp_v2.py --profile dev --es-query "POST:/_analyze" --es-body "{"analyzer":"kor","text":"생기부"}"` (JSON 파일 사용 시 `--es-body @payload.json`).

> `--list-remote`, `--ssh-command`, `--es-query` 옵션은 업로드와 별도로 동작하며, 실행 후에는 즉시 종료됩니다.
```

또는 `TOUCH_ONLY=0, CREATE_END_TOUCH=0`인 상태에서 일회성으로 쓸 경우:

```bash
# (별도 CLI 플래그 없이 sftp.yaml에서만 제어하는 설계입니다)
# 일시적으로 sftp.yaml에서 TOUCH_ONLY=1로 바꾸고 실행 → 원복
```

---

## 4) 자주 쓰는 운영 레시피(Recipes)

### 4.1 “이미지와 PDF만” 올리기 + 임시파일 제외

```ini
FILTER_INCLUDE_GLOBS=*.jpg;*.jpeg;*.png;*.pdf
FILTER_EXCLUDE_GLOBS=~$*;*.tmp;*.bak
```

* `fnmatch` 규칙으로 이름 매칭. 대소문자 민감도는 OS 규칙을 따릅니다. ([Python documentation][3])

### 4.2 “최신 5개만” (베이스별)

```ini
FILTER_NEWEST_SCOPE=per_base
FILTER_NEWEST_COUNT=5
```

### 4.3 “50MB 이상 \~ 2GB 이하”만

```ini
FILTER_MIN_SIZE=52428800
FILTER_MAX_SIZE=2147483648
```

### 4.4 부분 재개 + 속도 제한(전역 10Mbps, 파일별 4Mbps)

```ini
RESUME_ENABLED=1
RATE_LIMIT_BPS=1250000         # ≈ 10 Mbps
RATE_LIMIT_PER_FILE_BPS=500000 # ≈ 4 Mbps
```

* 토큰버킷 기반으로 **장기 평균률 보장**, 순간 버스트는 capacity 범위 내 허용. ([Wikipedia][4])

### 4.5 원격에 동일 파일 있으면 “크기 같으면 스킵”

```ini
SKIP_IF_EXISTS=size_equal
```

### 4.6 전송 후 자동 `end.touch` 생성

```ini
CREATE_END_TOUCH=1
END_TOUCH_NAME=end.touch
```

### 4.7 Slack + Email 알림

```ini
SLACK_WEBHOOK_URL=https://hooks.slack.com/services/...
SMTP_SERVER=smtp.example.com
SMTP_PORT=587
SMTP_USER=ops@example.com
SMTP_PASS=******           # 필요하면 실행 시 --smtp-pass 로 프롬프트
SMTP_USE_TLS=1
EMAIL_FROM=ops@example.com
EMAIL_TO=admin1@example.com,admin2@example.com
```

### 4.8 서버별 정책으로 느린 백업 서버 최적화

```ini
SERVER_POLICIES={
  "backup.example.com": {
    "max_workers": 2,
    "rate_limit_bps": 1500000,
    "retries": 5,
    "verify_hash": true,
    "sftp_window_size": 131072,
    "sftp_max_packet_size": 32768
  }
}
```

* `from_transport(window_size, max_packet_size)`로 채널 튜닝. **효과는 서버/망 환경에 의존**합니다. ([Read the Docs][1])

---

## 5) 결과물 확인(보고서/로그)

* `reports/<YYYYMMDDHHMM>/`

  * `summary.json`: 전체 통계(스캔/선정/제외/성공/실패/총용량/소요시간 등)
  * `success.csv`: 성공 목록(로컬/원격/크기/시간/평균속도/재시도/해시검증)
  * `failed.csv`: 실패 목록(에러 메시지 포함)
  * `skipped.csv`: 제외 사유(확장자/글롭/크기/메타누락)
  * (선택) `report.xlsx`: 동일 내용의 시트로 구성(설치 시 자동 생성)
* `sftp_upload.log`: 상세 실행 로그(진행률/전송속도/예외/재시도)

---

## 6) 동작 원리 (운영자가 알아두면 좋은 내부 메커니즘)

### 6.1 스캔 → 필터링

1. `ORDER` 순서대로 베이스 폴더를 탐색
2. **확장자/와일드카드/크기** 필터 적용(표준 `fnmatch`) ([Python documentation][3])
3. `.meta` 요구 시 **동명.meta 존재**하는 파일만 후보로 선정
4. `FILTER_NEWEST_SCOPE`가 per\_base/global이면 **mtime 최신 N개만** 유지

### 6.2 SFTP 업로드 & 부분 재개

* 원격에 파일이 있고 `RESUME_ENABLED=1`인 경우:

  * **원격 크기 < 로컬 크기**면 로컬을 `seek(offset=remote_size)`로 이동
  * 원격 파일을 **append 모드**로 열고 이어서 씁니다(모드 `'a'`: append). ([CCP4 FTP][2])
* 전송 후 `CONFIRM_PUT=1`이면 **`stat()`로 크기 검증**(불일치 시 실패 처리). ([Read the Docs][1])
* `VERIFY_HASH=1`이면 **샘플링 해시**(기본 200MB)로 무결성 재확인

> 참고: Paramiko의 `SFTPClient.open`/`SFTPFile`은 파이썬의 파일 모드와 동일한 `'r','w','a','r+','w+','a+'` 의미를 가집니다. ([CCP4 FTP][2])

### 6.3 속도 제한(토큰버킷)

* **전역 버킷**과 **파일별 버킷**을 각각 운용
* 각 쓰기 청크마다 `consume(n)`으로 토큰을 차감하고 부족하면 **필요 시간만 슬립**
* 장기적으로 **평균 전송률이 설정 bps**로 수렴하며, 단기 버스트는 capacity 범위 내 허용됩니다. ([Wikipedia][4])

### 6.4 병렬 업로드/재시도

* `ThreadPoolExecutor(max_workers=...)`로 파일 단위 병렬 처리 (I/O 바운드에 적합) ([Python documentation][5])
* 실패 시 **지수 백오프 + 지터**로 재시도(기본 3회)

### 6.5 SFTP 채널 튜닝

* 서버별 정책으로 `sftp_window_size`, `sftp_max_packet_size` 지정 가능
* `SFTPClient.from_transport(..., window_size, max_packet_size)` 사용—**전송 속도에 영향**할 수 있습니다. ([Read the Docs][1])

---

## 7) 보안 모범사례

1. **호스트키 검증 활성화**:
   `STRICT_HOST_KEY_CHECKING=1` 로 설정해 **미등록 호스트키를 거부**(기본 정책은 RejectPolicy). `AutoAddPolicy`/`WarningPolicy`는 MITM 위험으로 **권고되지 않습니다**. ([Paramiko][8])
2. **호스트키 사전 등록**:
   `SSHClient.load_system_host_keys()` 혹은 `load_host_keys()`로 known\_hosts를 사용하고, 필요한 경우 안전한 채널로 **사전에 호스트키를 수집/검증**하여 배포하세요. ([Paramiko][9])
3. **비밀번호 대신 키 인증** + 키 암호 설정 권장
4. **권한 최소화**: 원격 업로드 경로 권한을 최소화하고, 별도 계정 사용
5. **로그 보존**: 업로드 감사용 로그/리포트 장기 보관

---

## 8) 성능/안정성 튜닝 팁

* **대역폭이 좁거나 혼잡**:
  `RATE_LIMIT_BPS`로 전체 트래픽을 제한하고, 파일별 `RATE_LIMIT_PER_FILE_BPS`를 낮춰 **헤드오브라인 블로킹 방지**
* **서버 CPU/RAM 한계**:
  `MAX_WORKERS`를 줄이고 `sftp_window_size`/`max_packet_size`를 기본값으로 유지(오히려 높이면 역효과 가능) ([Read the Docs][1])
* **대용량 파일 위주**:

  * `UPLOAD_CHUNK_SIZE`를 64\~256KB로 상향(환경에 따라 측정)
  * `HASH_MAX_BYTES`를 적정 수준으로(전체 해시가 꼭 필요하면 0)
* **짧은 연결/장애가 잦음**:

  * `RETRIES` 확대, `BACKOFF_*` 완만하게
  * (커스터마이징) `Transport.set_keepalive()`를 적용해 keepalive 사용 고려(필요 시 코드 확장)
* **원격 디스크 느림**:
  `max_workers`를 줄이고 `RATE_LIMIT_PER_FILE_BPS`로 디스크 큐를 안정화

---

## 9) 문제 해결(Troubleshooting)

| 증상                                                | 점검/해결                                                                                                                                                                         |
| ------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **`AuthenticationException`/`Permission denied`** | 계정/키/권한 확인. 원격 경로 권한/소유자 확인                                                                                                                                                   |
| **`SSHException: Unknown server ... host key`**   | `STRICT_HOST_KEY_CHECKING=1`일 때 미등록 호스트키. 사전 등록(known\_hosts) 또는 안전한 절차로 키 반영. ([Paramiko][8])                                                                                |
| **`IOError: size mismatch ...`**                  | 전송 후 `stat()` 검증 불일치. 네트워크 드롭/디스크 문제. 재시도/재개/속도 제한 적용. ([Read the Docs][1])                                                                                                   |
| **append 재개가 동작 안 하는 것 같음**                       | 원격 서버가 SFTP 파일 append를 지원하지 않거나 파일시스템 이슈일 수 있음. 덮어쓰기 모드로 강제 전송(`SKIP_IF_EXISTS=never`, `RESUME_ENABLED=0`) 후 검증. (일반적으로 Paramiko는 `mode='a'`를 **append**로 처리) ([CCP4 FTP][2]) |
| **속도가 들쑥날쑥**                                      | 전역/파일별 제한값 조정, 병렬 수 조정, 채널 튜닝(`sftp_window_size`/`max_packet_size`) 시험. 효과는 환경 의존. ([Read the Docs][1])                                                                       |
| **글롭 필터가 기대와 다름**                                 | `fnmatch`는 경로가 아니라 **파일명** 기준 매칭입니다(현재 구현). 패턴/대소문자 규칙 재확인. ([Python documentation][3])                                                                                       |
| **엑셀 보고서가 안 생김**                                  | `pandas`/`openpyxl` 미설치 또는 오류. CSV로 동일 정보 제공됨.                                                                                                                                |
| **Slack/Email 안 옴**                               | 프록시/방화벽, SMTP 인증/포트/TLS, 수신자 설정 확인                                                                                                                                            |

---

## 10) FAQ

**Q1. `.meta`가 없는 파일도 업로드하고 싶은데요?**
→ `REQUIRE_META=0`로 설정하면 `.meta` 없이도 업로드합니다.

**Q2. 원격에 이미 같은 이름이 있으면 어떻게 하나요?**
→ `SKIP_IF_EXISTS`로 제어합니다. `size_equal`(추천), `mtime_ge`, `never` 중 선택.

**Q3. 부분 재개와 `confirm_put`/해시 검증의 관계는?**
→ 재개 후 전체 크기 일치(`stat`)와 선택적 해시 검증을 통해 **무결성 보장**(샘플링 해시도 가능). `confirm_put`은 Paramiko의 `put(confirm=True)`가 하는 것과 동일한 아이디어입니다. ([Read the Docs][1])

**Q4. 진행률은 어떻게 계산되나요?**
→ **전체 바이트 총합** 대비 **업로드된 바이트**를 주기적으로 집계하여 `%`/MB/s를 로그로 출력합니다. 파일 단위 진행률은 내부 write 루프(청크 업로드)에서 합산됩니다.

**Q5. 서버별 정책으로 채널 파라미터를 바꿔도 되나요?**
→ 가능합니다. `SFTPClient.from_transport(..., window_size, max_packet_size)` 사용. 단, **속도 영향은 환경/서버 구현에 따라 상이**합니다. ([Read the Docs][1])

---

## 11) 보너스: 운영 체크리스트

* [ ] `STRICT_HOST_KEY_CHECKING=1` (운영 반영 전 **known\_hosts** 준비) ([Paramiko][8])
* [ ] `ORDER`/`LOCAL_ROOT` 점검, UNC 경로 접근 권한 확인
* [ ] 필터(`INCLUDE/EXCLUDE_GLOBS`, `MIN/MAX_SIZE`, `NEWEST_*`) 시뮬레이션: **`--dry-run`**
* [ ] 속도/동시성(`RATE_LIMIT_*`, `MAX_WORKERS`)에 맞는 서버/망 부하 확인
* [ ] 보고서/로그 저장 위치 백업 정책(감사/추적용)
* [ ] 장애/복구 훈련: 네트워크 끊김 → 재시도/재개 확인, 불일치 시 해시검증

---

## 12) 부록: 핵심 참고 문서

* **Paramiko SFTP `put/putfo(confirm, callback)`** – 전송 후 `stat()` 크기 확인, 콜백 시그니처. ([Read the Docs][1])
* **Paramiko `SFTPClient.open` 모드** – `'a'`는 append. ([CCP4 FTP][2])
* **SFTP 채널 파라미터** – `from_transport(window_size, max_packet_size)`. ([Read the Docs][1])
* **Python `fnmatch` 규칙** – 와일드카드 매칭. ([Python documentation][3])
* **토큰버킷 알고리즘** – 속도 제한 개념/특성. ([Wikipedia][4])
* **ThreadPoolExecutor** – 병렬 실행(스레드 풀). ([Python documentation][5])
* **호스트키 검증 권고** – AutoAddPolicy/WarningPolicy 지양, RejectPolicy/known\_hosts 권장. ([CodeQL][7])

---

### 마무리

이 가이드는 **운영 관점에서 즉시 적용**할 수 있도록 설계되었습니다.
추가로 원하시면 **증분(변경분만) 업로드**, **특정 확장자별 다른 정책**, **보고서 자동 배포(S3/사내 포털)**, **세부 대시보드** 등도 확장해 드릴 수 있습니다.

[1]: https://readthedocs.org/projects/paramiko-docs/downloads/pdf/2.2/?utm_source=chatgpt.com "Paramiko"
[2]: https://ftp.ccp4.ac.uk/ccp4/7.0/unpacked/checkout/paramiko-1.15.2/paramiko/sftp_client.py?utm_source=chatgpt.com "sftp_client.py"
[3]: https://docs.python.org/3/library/fnmatch.html?utm_source=chatgpt.com "fnmatch — Unix filename pattern matching"
[4]: https://en.wikipedia.org/wiki/Token_bucket?utm_source=chatgpt.com "Token bucket"
[5]: https://docs.python.org/3/library/concurrent.futures.html?utm_source=chatgpt.com "concurrent.futures — Launching parallel tasks"
[6]: https://docs.paramiko.org/en/3.3/api/client.html?utm_source=chatgpt.com "Client"
[7]: https://codeql.github.com/codeql-query-help/python/py-paramiko-missing-host-key-validation/?utm_source=chatgpt.com "Accepting unknown SSH host keys when using Paramiko"
[8]: https://docs.paramiko.org/en/1.17/api/client.html?utm_source=chatgpt.com "Client"
[9]: https://docs.paramiko.org/en/stable/api/client.html?utm_source=chatgpt.com "Client"
## 5) Inventory / Metadata Workflow

1. **Initial scan**
   ```bash
   python sftp_v2.py --profile dev --inventory-scan
   ```
   - Hash mode, remote root, and database path follow `INVENTORY_*` settings in `sftp.yaml`.
   - Override at runtime with flags such as `--inventory-hash-mode full` or `--inventory-db ./inventory.sqlite`.

2. **Change report**
   ```bash
   python sftp_v2.py --profile dev --inventory-report --inventory-run-id 3
   ```
   - Writes CSV/JSON under `reports/inventory/<timestamp>`.
   - Files: `added.csv`, `modified.csv`, `renamed.csv`, `deleted.csv`, `summary.json`.

3. **PostgreSQL enrichment**
   ```bash
   python sftp_v2.py --profile dev --inventory-enrich-db --inventory-run-id 3
   ```
   - Requires `POSTGRES` block in `sftp.yaml`.
   - Generates `enriched.csv` by joining on the physical filename. Configure column list via `POSTGRES.columns`.

> Columns shown above are minimal samples; add/remove fields by editing the `POSTGRES.columns` array in `sftp.yaml`.
