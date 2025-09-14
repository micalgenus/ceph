#include <iostream>
#include <string>
#include <vector>
#include <memory>
#include <sstream>
#include <boost/program_options.hpp>
#include <filesystem>

#include "common/ceph_context.h"
#include "common/config.h"
#include "common/errno.h"
#include "global/global_init.h"
#include "mon/MonitorDBStore.h"
#include "kv/KeyValueDB.h"
#include "osd/OSDMap.h"
#include "include/buffer.h"

using namespace std;
namespace po = boost::program_options;

class OSDMapRestoreTool {
private:
  string osdmap_file_path;
  string mon_path;
  bool dry_run;
  bool force;

public:
  OSDMapRestoreTool() : dry_run(false), force(false) {
  }

  int parse_args(int argc, char* argv[]) {
    po::options_description desc("OSDMap 복원 도구 - 파일에서 MON으로");
    desc.add_options()
      ("osdmap-file", po::value<string>(&osdmap_file_path)->required(), 
       "복원할 OSDMap 파일 경로")
      ("mon-path", po::value<string>(&mon_path)->required(),
       "MON store 경로")
      ("dry-run", po::bool_switch(&dry_run),
       "실제 저장하지 않고 시뮬레이션만")
      ("force", po::bool_switch(&force),
       "강제로 덮어쓰기")
      ("help,h", "도움말 출력");

    po::variables_map vm;
    try {
      po::store(po::parse_command_line(argc, argv, desc), vm);
      
      if (vm.count("help")) {
        cout << desc << std::endl;
        return 1;
      }
      
      po::notify(vm);
    } catch (const exception& e) {
      cerr << "인수 파싱 오류: " << e.what() << std::endl;
      return -EINVAL;
    }
    
    return 0;
  }

  void print_info(const string& msg) {
    cout << "[INFO] " << msg << std::endl;
  }

  void print_error(const string& msg) {
    cerr << "[ERROR] " << msg << std::endl;
  }

  void print_warning(const string& msg) {
    cout << "[WARNING] " << msg << std::endl;
  }

  // 파일명에서 epoch 추출 (예: osdmap.4567 -> 4567)
  epoch_t extract_epoch_from_filename(const string& file_path) {
    std::filesystem::path p(file_path);
    string filename = p.filename().string();
    
    // osdmap.숫자 형태에서 숫자 추출
    size_t dot_pos = filename.find('.');
    if (dot_pos == string::npos) {
      print_error("파일명에서 epoch를 찾을 수 없습니다. osdmap.숫자 형태여야 합니다: " + filename);
      return 0;
    }
    
    string epoch_str = filename.substr(dot_pos + 1);
    try {
      epoch_t epoch = stoul(epoch_str);
      print_info("파일명에서 epoch " + to_string(epoch) + " 추출됨");
      return epoch;
    } catch (const exception& e) {
      print_error("epoch 파싱 실패: " + epoch_str + " - " + string(e.what()));
      return 0;
    }
  }

  // OSDMap 파일 읽기
  int read_osdmap_from_file(const string& file_path, OSDMap& osdmap) {
    try {
      bufferlist bl;
      int ret = bl.read_file(file_path.c_str(), nullptr);
      if (ret < 0) {
        print_error("OSDMap 파일 읽기 실패: " + cpp_strerror(ret));
        return ret;
      }

      if (bl.length() == 0) {
        print_error("OSDMap 파일이 비어있습니다");
        return -EINVAL;
      }

      print_info("OSDMap 파일 크기: " + to_string(bl.length()) + " bytes");

      // 안전한 디코딩을 위해 try-catch로 감싸고 iterator 검증
      bufferlist::const_iterator p = bl.cbegin();
      if (p.end()) {
        print_error("OSDMap buffer iterator가 유효하지 않습니다");
        return -EINVAL;
      }

      // 디코딩 전에 buffer 상태 확인
      print_info("Buffer iterator 위치: " + to_string(p.get_off()) + "/" + to_string(bl.length()));
      
      try {
        osdmap.decode(p);
      } catch (const ceph::buffer::error& e) {
        print_error("OSDMap 디코딩 중 buffer 에러: " + string(e.what()));
        return -EINVAL;
      } catch (const std::exception& e) {
        print_error("OSDMap 디코딩 중 오류: " + string(e.what()));
        return -EINVAL;
      }
      
      print_info("OSDMap 파일 읽기 성공 (epoch " + to_string(osdmap.get_epoch()) + ")");
      
      // OSDMap의 feature bits 확인
      uint64_t osdmap_features = osdmap.get_encoding_features();
      print_info("OSDMap encoding features: 0x" + to_string(osdmap_features));
      
      // MON 호환성 확인
      uint64_t mon_features = CEPH_FEATURES_ALL | CEPH_FEATURE_RESERVED;
      if ((osdmap_features & mon_features) != osdmap_features) {
        print_warning("OSDMap의 일부 feature bits가 MON과 호환되지 않을 수 있습니다");
        print_warning("OSDMap features: 0x" + to_string(osdmap_features));
        print_warning("MON features: 0x" + to_string(mon_features));
      }
      
      return 0;
    } catch (const exception& e) {
      print_error("OSDMap 파일 파싱 실패: " + string(e.what()));
      return -EINVAL;
    }
  }

  // 이전 OSDMap에서 incremental 생성
  int create_incremental_from_previous(MonitorDBStore *mon_store, epoch_t epoch, 
                                       const OSDMap& current_osdmap, OSDMap::Incremental& inc) {
    if (epoch <= 1) {
      print_info("epoch " + to_string(epoch) + "은 첫 번째 맵이므로 incremental이 없습니다");
      return 0; // 첫 번째 맵은 incremental이 없음
    }

    try {
      // 이전 epoch의 OSDMap 로드
      epoch_t prev_epoch = epoch - 1;
      bufferlist prev_bl;
      int ret = mon_store->get("osdmap", "full_" + to_string(prev_epoch), prev_bl);
      
      if (ret == -ENOENT) {
        // 이전 full map이 없으면 incremental map에서 재구성 시도
        ret = mon_store->get("osdmap", to_string(prev_epoch), prev_bl);
        if (ret == -ENOENT) {
          print_warning("이전 epoch " + to_string(prev_epoch) + "의 맵을 찾을 수 없습니다. incremental 생성 건너뜀");
          return 0;
        }
      }
      
      if (ret < 0) {
        print_error("이전 epoch " + to_string(prev_epoch) + " 맵 로드 실패: " + cpp_strerror(ret));
        return ret;
      }

      // 이전 OSDMap 디코딩
      OSDMap prev_osdmap;
      try {
        bufferlist::const_iterator p = prev_bl.cbegin();
        if (p.end()) {
          print_error("이전 OSDMap buffer iterator가 유효하지 않습니다");
          return -EINVAL;
        }
        prev_osdmap.decode(p);
      } catch (const ceph::buffer::error& e) {
        print_error("이전 OSDMap 디코딩 중 buffer 에러: " + string(e.what()));
        return -EINVAL;
      } catch (const std::exception& e) {
        print_error("이전 OSDMap 디코딩 실패: " + string(e.what()));
        return -EINVAL;
      }
      
      print_info("이전 epoch " + to_string(prev_epoch) + " OSDMap 로드 성공");
      
      // incremental 생성 (현재 맵과 이전 맵의 차이점 계산)
      inc = OSDMap::Incremental(epoch);
      inc.fsid = current_osdmap.get_fsid();
      
      // OSD 상태 변경사항 계산
      for (int i = 0; i < current_osdmap.get_max_osd(); i++) {
        if (current_osdmap.exists(i) != prev_osdmap.exists(i)) {
          if (current_osdmap.exists(i)) {
            inc.new_state[i] = CEPH_OSD_NEW;
            inc.new_weight[i] = current_osdmap.get_weight(i);
          } else {
            inc.new_state[i] = CEPH_OSD_DESTROYED;
          }
        } else if (current_osdmap.exists(i)) {
          // OSD가 존재하는 경우 상태나 가중치 변경 확인
          if (current_osdmap.is_up(i) != prev_osdmap.is_up(i)) {
            inc.new_state[i] = current_osdmap.is_up(i) ? CEPH_OSD_UP : 0;
          }
          if (current_osdmap.get_weight(i) != prev_osdmap.get_weight(i)) {
            inc.new_weight[i] = current_osdmap.get_weight(i);
          }
        }
      }
      
      // 풀 변경사항 계산
      const auto& current_pools = current_osdmap.get_pools();
      const auto& prev_pools = prev_osdmap.get_pools();
      
      for (const auto& pool : current_pools) {
        int64_t pool_id = pool.first;
        if (prev_pools.find(pool_id) == prev_pools.end()) {
          // 새 풀 추가
          inc.new_pools[pool_id] = pool.second;
        } else {
          // 풀 변경사항 확인 - 간단하게 모든 풀을 업데이트로 처리
          // (pg_pool_t 비교 연산자가 복잡하므로 모든 풀을 새로 설정)
          inc.new_pools[pool_id] = pool.second;
        }
      }
      
      // 삭제된 풀 확인
      for (const auto& pool : prev_pools) {
        int64_t pool_id = pool.first;
        if (current_pools.find(pool_id) == current_pools.end()) {
          inc.old_pools.insert(pool_id);
        }
      }
      
      print_info("epoch " + to_string(epoch) + " incremental 생성 완료");
      return 0;
      
    } catch (const std::exception& e) {
      print_error("incremental 생성 중 예외 발생: " + string(e.what()));
      return -EINVAL;
    }
  }

  // MON store에 OSDMap 저장 (full + incremental)
  int store_osdmap_to_mon(MonitorDBStore *mon_store, epoch_t epoch, const OSDMap& osdmap) {
    if (dry_run) {
      print_info("[DRY RUN] OSDMap epoch " + to_string(epoch) + "을 MON store에 저장 시뮬레이션");
      print_info("[DRY RUN] full_" + to_string(epoch) + " 키로 저장");
      print_info("[DRY RUN] " + to_string(epoch) + " 키로 incremental 저장");
      print_info("[DRY RUN] full_latest와 last_committed를 " + to_string(epoch) + "로 업데이트");
      if (!mon_store) {
        print_info("[DRY RUN] MON store가 없어서 실제 저장은 건너뜁니다");
      }
      return 0;
    }

    try {
      MonitorDBStore::Transaction t;
      
      // OSDMap을 bufferlist로 인코딩 (MON 호환 형태로)
      bufferlist osdmap_bl;
      try {
        // MON이 사용하는 feature bits로 인코딩
        uint64_t mon_features = CEPH_FEATURES_ALL | CEPH_FEATURE_RESERVED;
        print_info("OSDMap을 MON 호환 형태로 인코딩 (features: 0x" + to_string(mon_features) + ")");
        osdmap.encode(osdmap_bl, mon_features);
        print_info("인코딩된 OSDMap 크기: " + to_string(osdmap_bl.length()) + " bytes");
      } catch (const exception& e) {
        print_error("OSDMap 인코딩 실패: " + string(e.what()));
        return -EINVAL;
      }
      
      // MON store에 저장 (full map) - full_4494 형태
      string full_key = "full_" + to_string(epoch);
      t.put("osdmap", full_key, osdmap_bl);
      print_info("OSDMap을 " + full_key + " 키로 저장");
      
      // Incremental map 생성 및 저장
      if (mon_store) {
        OSDMap::Incremental inc;
        int ret = create_incremental_from_previous(mon_store, epoch, osdmap, inc);
        if (ret == 0 && inc.epoch == epoch) {
          // incremental을 bufferlist로 인코딩
          bufferlist inc_bl;
          try {
            uint64_t mon_features = CEPH_FEATURES_ALL | CEPH_FEATURE_RESERVED;
            print_info("Incremental OSDMap을 MON 호환 형태로 인코딩");
            inc.encode(inc_bl, mon_features);
            print_info("인코딩된 Incremental OSDMap 크기: " + to_string(inc_bl.length()) + " bytes");
            
            // MON store에 저장 (incremental map) - 4494 형태
            string inc_key = to_string(epoch);
            t.put("osdmap", inc_key, inc_bl);
            print_info("Incremental OSDMap을 " + inc_key + " 키로 저장");
          } catch (const exception& e) {
            print_error("Incremental OSDMap 인코딩 실패: " + string(e.what()));
            print_warning("epoch " + to_string(epoch) + " incremental 저장 건너뜀");
          }
        } else {
          print_warning("epoch " + to_string(epoch) + " incremental 생성 실패 또는 첫 번째 맵");
        }
      }
      
      // epoch를 version_t (8바이트) 형태로 인코딩 (Ceph 표준 방식)
      bufferlist epoch_bl;
      version_t epoch_version = (version_t)epoch; // version_t는 uint64_t (8바이트)
      epoch_bl.append((char*)&epoch_version, sizeof(version_t));
      
      // full_latest 업데이트
      t.put("osdmap", "full_latest", epoch_bl);
      print_info("full_latest를 " + to_string(epoch) + "로 업데이트");
      
      // last_committed 업데이트
      t.put("osdmap", "last_committed", epoch_bl);
      print_info("last_committed를 " + to_string(epoch) + "로 업데이트");
      
      // 트랜잭션 적용
      MonitorDBStore::TransactionRef t_ref = std::make_shared<MonitorDBStore::Transaction>(std::move(t));
      int ret = mon_store->apply_transaction(t_ref);
      if (ret < 0) {
        print_error("MON store에 OSDMap epoch " + to_string(epoch) + " 저장 실패: " + cpp_strerror(ret));
        return ret;
      }
      
      print_info("OSDMap epoch " + to_string(epoch) + "을 MON store에 저장 완료 (full + incremental)");
      return 0;
    } catch (const std::exception& e) {
      print_error("MON store 저장 중 예외 발생: " + string(e.what()));
      return -EINVAL;
    }
  }

  // MON 프로세스 실행 상태 확인
  bool is_mon_process_running() {
    int ret = system("pgrep -f 'ceph-mon' > /dev/null 2>&1");
    return (ret == 0);
  }


  // MON store 열기 (ceph-monstore-tool 방식 사용)
  int open_mon_store(unique_ptr<MonitorDBStore>& mon_store) {
    print_info("MON store 열기: " + mon_path);
    
    // MON 프로세스 실행 상태 확인
    if (is_mon_process_running()) {
      print_warning("MON 프로세스가 실행 중입니다. 파일 잠금으로 인한 I/O 에러가 발생할 수 있습니다.");
      print_info("권장사항: MON 프로세스를 중지하거나 다른 MON 인스턴스를 사용하세요.");
    }
    
    // ceph-monstore-tool과 동일한 방식으로 단순하게 처리
    // 복잡한 경로 탐색 대신 직접 경로 사용
    string store_path = mon_path;
    print_info("사용할 store 경로: " + store_path);
    
    // MonitorDBStore 생성 및 열기 (ceph_monstore_tool.cc와 동일한 방식)
    mon_store.reset(new MonitorDBStore(store_path));
    
    stringstream ss;
    int ret = mon_store->open(ss);
    if (ret < 0) {
      print_error("MON store 열기 실패: " + cpp_strerror(ret));
      if (!ss.str().empty()) {
        print_error("상세 에러: " + ss.str());
      }
      print_error("시도한 경로: " + store_path);
      
      // I/O 에러인 경우 상세한 진단 정보 제공
      if (ret == -EIO) {
        print_error("I/O 에러 발생 - 가능한 원인:");
        print_error("1. MON 프로세스가 실행 중이어서 파일이 잠겨있음");
        print_error("2. 파일 시스템 오류");
        print_error("3. 권한 문제");
        print_error("4. 파일 손상");
      }
      
      // dry-run 모드에서는 더 유용한 정보 제공
      if (dry_run) {
        print_info("DRY RUN 모드: 실제 store 열기 실패했지만 시뮬레이션을 계속합니다.");
        
        // dry-run 모드에서는 store를 null로 설정
        mon_store.reset(nullptr);
        return 0; // 성공으로 처리하여 시뮬레이션 계속
      }
      
      // store 열기 실패 시 에러 반환
      return ret;
    }
    
    print_info("MON store 로드 성공");
    return 0;
  }

  // MON store에서 현재 last_committed epoch 확인
  epoch_t get_last_committed_epoch(MonitorDBStore *mon_store) {
    // store가 null인 경우 안전하게 처리
    if (!mon_store) {
      print_info("MON store가 없어서 last_committed를 0으로 가정합니다 (새로운 store)");
      return 0;
    }
    
    try {
      // 디버깅: osdmap 테이블의 모든 키 확인
      print_info("osdmap 테이블의 키들을 확인합니다...");
      auto it = mon_store->get_iterator("osdmap");
      if (it) {
        it->seek_to_first();
        while (it->valid()) {
          string key = it->key();
          print_info("osdmap 키 발견: " + key);
          it->next();
        }
      }
      
      bufferlist bl;
      int ret = mon_store->get("osdmap", "last_committed", bl);
      if (ret < 0) {
        if (ret == -ENOENT) {
          print_info("MON store에 last_committed가 없습니다 (새로운 store)");
          return 0;
        }
        print_error("last_committed 조회 실패: " + cpp_strerror(ret));
        return (epoch_t)-1; // Error indicator for unsigned type
      }
      
      // last_committed는 version_t (8바이트) 형태로 저장됨 (Ceph 표준 방식)
      if (bl.length() != sizeof(version_t)) {
        print_error("last_committed 크기가 예상과 다릅니다: " + to_string(bl.length()) + " bytes (예상: " + to_string(sizeof(version_t)) + " bytes)");
        return (epoch_t)-1;
      }
      
      // 8바이트 version_t에서 epoch_t로 변환
      version_t last_version;
      memcpy(&last_version, bl.c_str(), sizeof(version_t));
      epoch_t last_epoch = (epoch_t)last_version;
      
      // 디버깅: raw 데이터 출력
      print_info("last_committed raw data: " + to_string(bl.length()) + " bytes");
      for (size_t i = 0; i < bl.length(); i++) {
        print_info("  byte[" + to_string(i) + "] = 0x" + to_string((unsigned char)bl.c_str()[i]));
      }
      
      print_info("현재 MON store의 last_committed epoch: " + to_string(last_epoch));
      return last_epoch;
    } catch (const exception& e) {
      print_error("last_committed 파싱 실패: " + string(e.what()));
      return (epoch_t)-1; // Error indicator
    }
  }

  // 메인 복원 로직
  int restore_osdmap() {
    print_info("=== OSDMap 복원 시작 (파일 기반) ===");
    print_info("OSDMap 파일: " + osdmap_file_path);
    print_info("MON 경로: " + mon_path);
    print_info("강제 복원: " + string(force ? "예" : "아니오"));
    print_info("드라이 런: " + string(dry_run ? "예" : "아니오"));
    
    // OSDMap 파일 존재 확인
    if (access(osdmap_file_path.c_str(), R_OK) != 0) {
      print_error("OSDMap 파일을 찾을 수 없습니다: " + osdmap_file_path);
      return -ENOENT;
    }
    
    // MON store 열기
    unique_ptr<MonitorDBStore> mon_store;
    int ret = open_mon_store(mon_store);
    if (ret < 0) {
      // dry-run 모드에서는 store 열기 실패해도 시뮬레이션 계속
      if (dry_run) {
        print_warning("DRY RUN: MON store 열기 실패했지만 시뮬레이션을 계속합니다.");
        mon_store.reset(nullptr); // null로 설정하여 dry-run 모드임을 표시
      } else {
        return ret;
      }
    }
    
    // RAII를 위한 store 정리 함수
    auto cleanup_store = [&]() {
      if (mon_store) {
        try {
          mon_store->close();
          print_info("MON store 정리 완료");
        } catch (const std::exception& e) {
          print_warning("MON store 정리 중 예외 발생: " + string(e.what()));
        }
        mon_store.reset();
      }
    };
    
    // 파일명에서 epoch 추출
    epoch_t epoch = extract_epoch_from_filename(osdmap_file_path);
    if (epoch == 0) {
      print_error("파일명에서 epoch를 추출할 수 없습니다");
      cleanup_store();
      return -EINVAL;
    }
    
    // 현재 MON store의 last_committed epoch 확인
    epoch_t last_epoch;
    print_info("DEBUG: dry_run=" + string(dry_run ? "true" : "false") + ", mon_store=" + string(mon_store ? "not null" : "null"));
    
    if (dry_run && !mon_store) {
      // dry-run 모드에서 store가 null인 경우
      last_epoch = 0;
      print_info("DRY RUN: store가 없어서 last_committed를 0으로 가정합니다");
    } else if (!mon_store) {
      // store가 null인 경우 (dry-run이 아닌 경우에도)
      last_epoch = 0;
      print_info("MON store가 없어서 last_committed를 0으로 가정합니다");
    } else {
      print_info("DEBUG: get_last_committed_epoch 호출 전");
      last_epoch = get_last_committed_epoch(mon_store.get());
      print_info("DEBUG: get_last_committed_epoch 호출 후, last_epoch=" + to_string(last_epoch));
      if (last_epoch == (epoch_t)-1) {
        print_error("last_committed epoch 조회 실패");
        cleanup_store();
        return -EINVAL;
      }
    }
    
    // epoch 검증 (연속성 확인)
    if (last_epoch == 0) {
      // 새로운 store인 경우, epoch가 1 이상이면 허용
      if (epoch < 1) {
        print_error("새로운 store에서 epoch는 1 이상이어야 합니다");
        cleanup_store();
        return -EINVAL;
      }
      print_info("새로운 store에 epoch " + to_string(epoch) + " 설정");
    } else {
      // 기존 store인 경우 연속성 확인
      epoch_t expected_epoch = last_epoch + 1;
      if (epoch != expected_epoch) {
        print_error("새로운 epoch(" + to_string(epoch) + ")는 현재 last_committed epoch(" + to_string(last_epoch) + ") + 1 = " + to_string(expected_epoch) + "이어야 합니다");
        if (!force) {
          print_error("--force 옵션을 사용하여 강제로 덮어쓸 수 있습니다 (위험함)");
          cleanup_store();
          return -EINVAL;
        }
        print_warning("--force 옵션으로 강제 덮어쓰기 진행 (epoch 연속성 무시)");
      } else {
        print_info("epoch 연속성 확인됨: " + to_string(last_epoch) + " -> " + to_string(epoch));
      }
    }
    
    // OSDMap 파일 읽기
    OSDMap osdmap;
    ret = read_osdmap_from_file(osdmap_file_path, osdmap);
    if (ret < 0) {
      cleanup_store();
      return ret;
    }
    
    // MON store에 저장
    ret = store_osdmap_to_mon(mon_store.get(), epoch, osdmap);
    if (ret < 0) {
      cleanup_store();
      return ret;
    }
    
    print_info("=== OSDMap 복원 완료 ===");
    
    // MonitorDBStore 정리
    cleanup_store();
    
    return 0;
  }
  
  ~OSDMapRestoreTool() {
  }
};

int main(int argc, char* argv[]) {
  // ceph-monstore-tool과 동일한 방식으로 초기화
  std::vector<const char*> args;
  auto cct = global_init(
    NULL, args, CEPH_ENTITY_TYPE_MON,
    CODE_ENVIRONMENT_UTILITY,
    CINIT_FLAG_NO_MON_CONFIG);
  common_init_finish(g_ceph_context);
  cct->_conf.apply_changes(nullptr);

  OSDMapRestoreTool tool;
  
  int ret = tool.parse_args(argc, argv);
  if (ret != 0) {
    return ret;
  }
  
  ret = tool.restore_osdmap();
  if (ret < 0) {
    cerr << "OSDMap 복원 실패: " << cpp_strerror(ret) << std::endl;
    return 1;
  }
  
  cout << "OSDMap 복원 성공!" << std::endl;
  return 0;
}
