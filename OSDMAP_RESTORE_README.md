# Ceph OSDMap 복원 도구

이 도구는 OSD store에서 OSDMap을 읽어서 MON store에 복원하는 C++ 프로그램입니다. MON의 OSDMap이 과거 버전으로 되돌아간 상황에서 OSD의 최신 OSDMap 정보를 복원할 때 사용합니다.

## 상황 설명

- **현재 상황**: MON의 OSDMap epoch가 4444, OSD의 OSDMap epoch가 4888
- **목표**: OSD의 모든 epoch version OSDMap을 MON에 복원하여 MON의 정보를 최신으로 업데이트

## 빌드 방법

```bash
cd ceph
./do_cmake.sh
cd build
ninja ceph-osdmap-restore-tool
```

## 사용법

### 기본 사용법

```bash
# OSDMap file에서 MON으로 epoch 설정
ceph-osdmap-restore-tool \
  --osdmap-file osd.4445 \
  --mon-path /var/lib/ceph/mon/ceph-a

# 드라이 런으로 시뮬레이션
ceph-osdmap-restore-tool \
  --osdmap-file osd.4445 \
  --mon-path /var/lib/ceph/mon/ceph-a
  --dry-run
```

### 옵션 설명
- `--osdmap-file arg`: 복원할 OSDMap 파일 경로
- `--mon-path arg`: MON store 경로
- `--dry-run`: 실제 저장하지 않고 시뮬레이션만
- `--force`: 강제로 덮어쓰기
- `-h [ --help ]`: 도움말 출력

## 주의사항

⚠️ **중요**: 이 도구는 Ceph 클러스터의 핵심 데이터를 수정합니다. 사용 전에 반드시 백업을 생성하세요.

### 안전한 사용을 위한 권장사항

1. **백업 생성**: 복원 전에 MON 데이터를 백업
2. **드라이 런**: 먼저 `--dry-run` 옵션으로 시뮬레이션 실행
3. **단계적 복원**: 큰 epoch 범위 대신 작은 범위로 나누어 복원
4. **클러스터 상태 확인**: 복원 후 `ceph status`로 클러스터 상태 확인

## 예시 시나리오

### 시나리오 1: 전체 복원

```bash
# 1. 현재 상태 확인
ceph status
ceph osd dump | head -5

# 2. OSD 에서 OSDMap 추출
ceph-objectstore-tool \
  --data-path /var/lib/ceph/osd/osd-1 \
  --op get-osdmap \
  --epoch 4445 \
  --file /tmp/osdmap.4445.bin


# 2. 드라이 런으로 확인
ceph-osdmap-restore-tool \
  --osdmap-file /tmp/osdmap.4445.bin \
  --mon-path /var/lib/ceph/mon/ceph-a \
  --dry-run

# 3. 실제 복원 실행
ceph-osdmap-restore-tool \
  --osdmap-file /tmp/osdmap.4445.bin \
  --mon-path /var/lib/ceph/mon/ceph-a

# 4. 복원 결과 확인
ceph status
ceph osd dump | head -5
```
