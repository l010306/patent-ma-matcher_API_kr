# -*- coding: utf-8 -*-
"""
Step 4: Compustat Master Dictionary Builder (Compustat 슈퍼 딕셔너리 빌더)
===========================================
Consolidates Compustat matching results from Step 3 into a master dictionary.
(Step 3의 Compustat 매칭 결과를 통합하여 마스터 사전을 구성합니다.)
"""

import pandas as pd
import os
import pickle
import logging
from datetime import datetime
from collections import Counter

# ==========================================
# 로그 설정 (Configure Logging)
# ==========================================
os.makedirs('pipeline_outputs/logs', exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'pipeline_outputs/logs/compustat_dict_building_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log',
                            encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ==========================================
# 1. 설정 (Configuration): 입력/출력 파일 및 열 이름 설정
# ==========================================
CONFIG = {
    'Acquiror_to_Compustat': {
        'inputs': [
            'Compustat_Auto_Matched_Acquiror.xlsx',
            'Compustat_Manual_Review_Acquiror.xlsx'
        ],
        'key_col': 'Dictionary_Company_Name',
        'val_col': 'Matched_Compustat_Original',
        'out_pkl': 'Master_Dict_Compustat_Acquiror.pkl',
        'out_excel': 'Master_Dict_Compustat_Acquiror_VIEW.xlsx'
    },
    'Target_to_Compustat': {
        'inputs': [
            'Compustat_Auto_Matched_Target.xlsx',
            'Compustat_Manual_Review_Target.xlsx'
        ],
        'key_col': 'Dictionary_Company_Name',
        'val_col': 'Matched_Compustat_Original',
        'out_pkl': 'Master_Dict_Compustat_Target.pkl',
        'out_excel': 'Master_Dict_Compustat_Target_VIEW.xlsx'
    }
}


# ==========================================
# 2. 메인 처리 함수 (Main Processing Function)
# ==========================================

def build_compustat_dictionary(dict_type, config):
    """
    Compustat 전용 마스터 사전 (슈퍼 딕셔너리) 구축
    (构建 Compustat 专属超级字典)
    """
    files_list = config['inputs']
    key_col = config['key_col']
    val_col = config['val_col']

    logger.info("=" * 60)
    logger.info(" [Step 4] Building Compustat Master Dictionary (Compustat 메인 사전 구축) ")
    logger.info("=" * 60)

    master_dict = {}
    source_stats = []
    conflicts = []

    for file_name in files_list:
        file_path = os.path.join('/Users/lidachuan/Desktop/RA_work/pipeline_outputs', file_name)
        if not os.path.exists(file_path):
            logger.warning(f"⚠️  건너뜀 (Skip): 파일을 찾을 수 없습니다. {file_path}")
            continue

        logger.info(f"\n파일 읽는 중 (Reading): {file_name}")

        try:
            df = pd.read_excel(file_path)

            # 필수 열 존재 여부 확인 (Validate required columns)
            if key_col not in df.columns or val_col not in df.columns:
                logger.error(f"   ❌ 오류 (Error): 파일에 필수 열인 '{key_col}' 또는 '{val_col}'이 없습니다. 건너뜹니다.")
                continue

            # 빈 값 정리 (Clean up NaN values)
            required_cols = [key_col, val_col]
            df_valid = df.dropna(subset=required_cols)
            df_valid = df_valid[
                (df_valid[key_col].astype(str).str.strip() != "") &
                (df_valid[val_col].astype(str).str.strip() != "")
                ]

            # 바보 방지 (Failsafe): 수동 검토 표에 MISMATCH로 표시된 항목이 있는 경우 그것을 제거합니다
            if 'Match_Type' in df_valid.columns:
                df_valid = df_valid[~df_valid['Match_Type'].astype(str).str.contains('MISMATCH', case=False, na=False)]

            logger.info(f"   유효한 행 수 (Valid Rows): {len(df_valid)}")

            count_new = 0
            count_duplicate = 0
            count_conflict = 0

            for idx, row in df_valid.iterrows():
                raw_key = str(row[key_col]).strip()
                matched_val = str(row[val_col]).strip()

                if raw_key not in master_dict:
                    master_dict[raw_key] = matched_val
                    count_new += 1
                else:
                    existing = master_dict[raw_key]
                    if existing == matched_val:
                        count_duplicate += 1
                    else:
                        count_conflict += 1
                        conflicts.append({
                            'Dict_Type': dict_type,
                            'Dictionary_Company_Name': raw_key,
                            'Existing_Compustat_Match': existing,
                            'New_Compustat_Match': matched_val,
                            'Source_File': file_name
                        })
                        logger.warning(f"   ⚠️  충돌 (Conflict): '{raw_key}'가 이미 '{existing}'(으)로 매핑되어 있습니다. 새 값 '{matched_val}'을 무시합니다.")

            logger.info(f"   ✅ 처리 완료 (Processed): 새 항목 {count_new}개, 중복 {count_duplicate}개, 충돌 {count_conflict}개")

            source_stats.append({
                'Dictionary': dict_type,
                'File': file_name,
                'Valid_Rows': len(df_valid),
                'New_Mappings': count_new,
                'Duplicates': count_duplicate,
                'Conflicts': count_conflict
            })

        except Exception as e:
            logger.error(f"   ❌ 읽기 실패 (Read Failed): {e}")

    return master_dict, source_stats, conflicts


def save_dictionary(dict_type, master_dict, config):
    """사전 및 Excel 백업 저장 (Save dictionary and Excel backup)"""
    base_dir = '/Users/lidachuan/Desktop/RA_work/pipeline_outputs'
    out_pkl = os.path.join(base_dir, config['out_pkl'])
    out_excel = os.path.join(base_dir, config['out_excel'])

    if not master_dict:
        logger.warning(f"⚠️ 경고 (Warning): 사전 {dict_type}가 비어 있습니다! 추출된 매핑 관계가 없습니다.")
        return False

    with open(out_pkl, 'wb') as f:
        pickle.dump(master_dict, f)
    logger.info(f"✅ {dict_type} Pickle로 저장 완료: {os.path.basename(out_pkl)}")

    df_out = pd.DataFrame(
        list(master_dict.items()),
        columns=[config['key_col'], config['val_col']]
    )
    df_out = df_out.sort_values(config['val_col']).reset_index(drop=True)
    df_out.to_excel(out_excel, index=False)
    logger.info(f"✅ {dict_type} Excel로 저장 완료: {os.path.basename(out_excel)}")

    return True


def print_summary(dict_type, master_dict, source_stats, conflicts):
    """요약 정보 출력 (Print Summary)"""
    logger.info("\n" + "-" * 40)
    logger.info(f"[{dict_type}] 구축 요약 (Build Summary)")
    logger.info("-" * 40)

    logger.info(f"📊 전체 통계 (Overall Statistics):")
    logger.info(f"   - 총 독립 매핑 관계 수 (Total Mappings): {len(master_dict):,}")
    logger.info(f"   - 처리 파일 수 (Processed Files): {len(source_stats)}")
    logger.info(f"   - 감지된 충돌 데이터 (Detected Conflicts): {len(conflicts)}")

    # 가장 많이 매핑된 Compustat 개체 통계
    target_counts = Counter(master_dict.values())
    top_targets = target_counts.most_common(5)

    logger.info(f"\n🏢 M&A 변형 명칭을 가장 많이 포함하고 있는 Compustat 법인 (가장 많이 발견됨 Top 5):")
    for company, count in top_targets:
        logger.info(f"   {company}: {count} 개의 M&A 명칭 별칭 연결")


# ==========================================
# 3. 메인 실행 흐름 (Main Execution Flow)
# ==========================================

def main():
    start_time = datetime.now()

    all_stats = []
    all_conflicts = []

    for dict_type, config in CONFIG.items():
        master_dict, source_stats, conflicts = build_compustat_dictionary(dict_type, config)
        if master_dict:
            save_dictionary(dict_type, master_dict, config)
            print_summary(dict_type, master_dict, source_stats, conflicts)

        all_stats.extend(source_stats)
        all_conflicts.extend(conflicts)

    # 모든 통계 및 충돌 데이터 통합 저장
    base_dir = '/Users/lidachuan/Desktop/RA_work/pipeline_outputs'
    if all_stats:
        pd.DataFrame(all_stats).to_excel(os.path.join(base_dir, 'Compustat_Dict_Build_Stats.xlsx'), index=False)
    if all_conflicts:
        pd.DataFrame(all_conflicts).to_excel(os.path.join(base_dir, 'Compustat_Dict_Conflicts.xlsx'), index=False)

    duration = (datetime.now() - start_time).total_seconds()
    logger.info("\n" + "=" * 60)
    logger.info(f"🎉 Compustat 두 세트 사전 구축에 모두 성공했습니다! (총 소요 시간: {duration:.2f} 초)")
    logger.info("=" * 60)

    return True


if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)
