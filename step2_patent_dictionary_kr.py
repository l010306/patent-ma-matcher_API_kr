# -*- coding: utf-8 -*-
"""
Step 2: Patent Master Dictionary Builder (특허 슈퍼 딕셔너리 빌더)
========================================
Consolidates automatic and manual matches from Step 1 into a master dictionary.
(Step 1에서 얻은 자동 및 수동 매칭 결과를 마스터 사전으로 통합합니다.)
"""


import pandas as pd
import os
import pickle
import logging
from datetime import datetime

# ==========================================
# 로그 설정 (Configure Logging)
# ==========================================
os.makedirs('pipeline_outputs/logs', exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'pipeline_outputs/logs/dict_building_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ==========================================
# 1. 설정 (Configuration): 입력 및 출력 파일
# ==========================================
# 두 그룹으로 나누어 독립적으로 구축 진행 (分为两组进行独立构建)
CONFIG = {
    'Acquiror': {
        'inputs': [
            'Final_Auto_Matched_Acquiror.xlsx',
            'Final_Manual_Review_Acquiror.xlsx'
        ],
        'match_col': 'Acquiror_Original',
        'out_pkl': 'Master_Dict_Acquiror.pkl',
        'out_excel': 'Master_Dict_Acquiror_VIEW.xlsx'
    },
    'Target': {
        'inputs': [
            'Final_Auto_Matched_Target.xlsx',
            'Final_Manual_Review_Target.xlsx'
        ],
        'match_col': 'Target_Original',
        'out_pkl': 'Master_Dict_Target.pkl',
        'out_excel': 'Master_Dict_Target_VIEW.xlsx'
    }
}

# ==========================================
# 2. 메인 처리 함수 (Main Processing Function)
# ==========================================

def build_master_dictionary(dict_type, config):
    """
    특정 유형의 마스터 딕셔너리 구축 (Acquiror 또는 Target)
    (构建特定类型的超级字典 (Acquiror 或 Target))
    """
    files_list = config['inputs']
    match_col = config['match_col']
    
    logger.info("=" * 60)
    logger.info(" [Step 2] Building Patent Master Dictionary (특허 마스터 사전 구축 중)")
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
            
            if match_col not in df.columns or 'Assignee_Original' not in df.columns:
                logger.error(f"   ❌ 오류 (Error): 파일에 필수 열인 'Assignee_Original' 또는 '{match_col}'이 없습니다. 건너뜁니다.")
                continue
            
            required_cols = ['Assignee_Original', match_col]
            df_valid = df.dropna(subset=required_cols)
            df_valid = df_valid[
                (df_valid['Assignee_Original'].astype(str).str.strip() != "") &
                (df_valid[match_col].astype(str).str.strip() != "")
            ]
            
            # Match_Type 열이 존재하는 경우 MISMATCH 문자열이 포함된 행을 제외합니다 (바보 방지 메커니즘, 삭제되었더라도 상관없음)
            if 'Match_Type' in df_valid.columns:
                df_valid = df_valid[~df_valid['Match_Type'].astype(str).str.contains('MISMATCH', case=False, na=False)]
            
            logger.info(f"   유효한 행 수 (Valid Rows): {len(df_valid)}")
            
            count_new = 0
            count_duplicate = 0
            count_conflict = 0
            
            for idx, row in df_valid.iterrows():
                assignee_raw = str(row['Assignee_Original']).strip()
                matched_name  = str(row[match_col]).strip()
                
                if assignee_raw not in master_dict:
                    master_dict[assignee_raw] = matched_name
                    count_new += 1
                else:
                    existing = master_dict[assignee_raw]
                    if existing == matched_name:
                        count_duplicate += 1
                    else:
                        count_conflict += 1
                        conflicts.append({
                            'Dict_Type': dict_type,
                            'Assignee': assignee_raw,
                            'Existing_Match': existing,
                            'New_Match': matched_name,
                            'Source_File': file_name
                        })
                        logger.warning(f"   ⚠️  충돌 (Conflict): '{assignee_raw}'가 이미 '{existing}' 정보를 가지고 있습니다. 새 값 '{matched_name}'을(를) 무시합니다.")
            
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
    logger.info(f"✅ {dict_type} Pickle로 저장되었습니다: {out_pkl}")
    
    df_out = pd.DataFrame(
        list(master_dict.items()), 
        columns=['Assignee_Original_Name', 'Matched_Company_Name']
    )
    df_out = df_out.sort_values('Matched_Company_Name').reset_index(drop=True)
    df_out.to_excel(out_excel, index=False)
    logger.info(f"✅ {dict_type} Excel로 저장되었습니다: {out_excel}")
    
    return True


def print_summary(master_dict, source_stats, conflicts):
    """요약 정보 출력 (Print Summary)"""
    logger.info("\n" + "=" * 60)
    logger.info("구축 완료 요약 (Build Completion Summary)")
    logger.info("=" * 60)
    
    logger.info(f"\n📊 통계 요약 (Stats Overview):")
    logger.info(f"   - 총 매핑 수 (Total Mappings): {len(master_dict):,}")
    logger.info(f"   - 처리된 파일 수 (Files Processed): {len(source_stats)}")
    logger.info(f"   - 확인된 충돌 데이터 (Conflicts Detected): {len(conflicts)}")
    
    if source_stats:
        logger.info(f"\n📁 파일별 기여도 (Contributions by File):")
        for stat in source_stats:
            logger.info(f"   {stat['File']}")
            logger.info(f"      추가: {stat['New_Mappings']}, 중복: {stat['Duplicates']}, 충돌: {stat['Conflicts']}")
    
    # 동일한 회사로 매핑된 변형 개수 통계
    from collections import Counter
    acquiror_counts = Counter(master_dict.values())
    top_companies = acquiror_counts.most_common(10)
    
    logger.info(f"\n🏢 가장 많은 변형을 가진 회사 (가장 많이 발견됨 Top 10):")
    for company, count in top_companies:
        logger.info(f"   {company}: {count} 개의 다른 이름(변형) 존재")
    
    if conflicts:
        logger.info(f"\n⚠️  경고: {len(conflicts)}개의 충돌이 발견되었습니다. 'Dictionary_Conflicts.xlsx' 파일을 확인해 주세요.")
        logger.info("   충돌 처리 전략: 가장 처음 발견된 매핑 관계를 유지합니다.")


# ==========================================
# 3. 메인 실행 흐름 (Main Execution Flow)
# ==========================================

def main():
    start_time = datetime.now()
    
    all_stats = []
    all_conflicts = []
    
    for dict_type, config in CONFIG.items():
        master_dict, source_stats, conflicts = build_master_dictionary(dict_type, config)
        save_dictionary(dict_type, master_dict, config)
        
        all_stats.extend(source_stats)
        all_conflicts.extend(conflicts)
        print_summary(master_dict, source_stats, conflicts)
    
    # 모든 통계 및 충돌 데이터를 한번에 모아서 저장
    base_dir = '/Users/lidachuan/Desktop/RA_work/pipeline_outputs'
    if all_stats:
        pd.DataFrame(all_stats).to_excel(os.path.join(base_dir, 'Dictionary_Build_Statistics.xlsx'), index=False)
    if all_conflicts:
        pd.DataFrame(all_conflicts).to_excel(os.path.join(base_dir, 'Dictionary_Conflicts.xlsx'), index=False)
        
    duration = (datetime.now() - start_time).total_seconds()
    logger.info(f"\n⏱  총 소요 시간 (Total duration): {duration:.2f} 초")
    logger.info("\n" + "=" * 60)
    logger.info("🎉 두 세트의 마스터 사전(슈퍼 딕셔너리)이 성공적으로 구축되었습니다!")
    logger.info("=" * 60)
    
    return True


if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)
