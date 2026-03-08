# -*- coding: utf-8 -*-
"""
Step 5: Final Data Aggregation Engine (최종 데이터 집계 엔진)
=====================================
Integrates Patent statistics and Compustat IDs into the final M&A dataset.
(특허 통계 및 Compustat ID를 최종 M&A 데이터세트에 통합합니다.)
"""

import pandas as pd
import numpy as np
import pickle
import os
import logging
from datetime import datetime
from tqdm import tqdm

# ==============================================================================
# 🌟🌟🌟 【설정 매개변수 (Configuration)】 🌟🌟🌟
# ==============================================================================

# 1. 메인 데이터베이스 템플릿 경로 (Main database template path)
PATH_FINAL_OUTCOME = "/Users/lidachuan/Desktop/RA_work/expected_outcome/final_outcome_fn.xlsx"

# 2. 특허 마스터 사전 경로 (Patent Super Dictionary Path: M&A -> 특허 별칭)
PATH_MASTER_DICT_ACQ = "/Users/lidachuan/Desktop/RA_work/pipeline_outputs/Master_Dict_Acquiror.pkl"
PATH_MASTER_DICT_TGT = "/Users/lidachuan/Desktop/RA_work/pipeline_outputs/Master_Dict_Target.pkl"

# 3. Compustat 마스터 사전 경로 (Compustat Super Dictionary Path: M&A -> Compustat 표준 이름)
PATH_COMP_DICT_ACQ = "/Users/lidachuan/Desktop/RA_work/pipeline_outputs/Master_Dict_Compustat_Acquiror.pkl"
PATH_COMP_DICT_TGT = "/Users/lidachuan/Desktop/RA_work/pipeline_outputs/Master_Dict_Compustat_Target.pkl"

# 4. 기본 데이터 데이블 경로 (Base Data Paths)
PATH_COMPUSTAT_RAW = "/Users/lidachuan/Desktop/RA_work/compustat_dataset/compustat_19802025.csv"
PATH_PATENT_DB = "/Users/lidachuan/Desktop/RA_work/patent_dataset/2010_2019/2019.csv"

# 5. 최종 산출물 경로 (Final Output Path)
PATH_OUTPUT_COMPLETE = "/Users/lidachuan/Desktop/RA_work/final_outcome_COMPLETE.xlsx"

# -----------------------------------------------------------
# 테스트 모드: step 1과 동일하게 유지(정수 입력)하거나 None 을 입력하세요
TESTING_ROWS = 500
# -----------------------------------------------------------

# 만약 테스트 모드라면, 입력되는 특허 목록을 step1에서 생성된 테스트용 전용 파일로 자동 전환합니다.
if TESTING_ROWS:
    _base, _ext = os.path.splitext(PATH_PATENT_DB)
    PATH_PATENT_DB = f"{_base}_test_{TESTING_ROWS}{_ext}"

# ==============================================================================

# 로그 설정 (Configure Logging)
os.makedirs('pipeline_outputs/logs', exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler(f'pipeline_outputs/logs/Final_Aggregation_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger('FinalAggregation')


# ==========================================
# 유틸리티 함수 (Utility Functions)
# ==========================================

def calculate_inventor_count_vectorized(df, inventor_cols):
    """
    벡터화된 통계 산출(Vectorized statistics): inventors 열 및 이름 열 개수 중 큰 값을 따릅니다.
    """
    num_from_column = pd.to_numeric(df['inventors'], errors='coerce').fillna(0)
    num_from_names = df[inventor_cols].notna().sum(axis=1)
    return np.maximum(num_from_column, num_from_names)


def load_super_dictionary(pkl_path):
    """일반 딕셔너리 로더 (Generic dictionary loader)"""
    if not os.path.exists(pkl_path):
        logger.warning(f"⚠️ 딕셔너리 {os.path.basename(pkl_path)} 가 존재하지 않습니다. 빈 딕셔너리를 반환합니다(Empty).")
        return {}
    with open(pkl_path, 'rb') as f:
        master_dict = pickle.load(f)
    logger.info(f"✅ 딕셔너리를 성공적으로 불러왔습니다: {os.path.basename(pkl_path)} (매핑 관계 {len(master_dict)}건 포함)")
    return master_dict


# ==========================================
# 핵심 처리 파이프라인 (Core Processing Pipeline)
# ==========================================

def execute_pipeline(role, df_template, dict_patent, dict_comp, df_comp_raw_unique, df_patent):
    """
    파이프라인 실행:
    1. Compustat ID 첨부. (Compustat 딕셔너리 매핑을 통해 연결)
    2. 특허(Patent) 데이터 및 통계. (Patent 딕셔너리 매핑을 통해 연결)
    """
    logger.info(f"\n>>>> 🚀 [{role}] 데이터 스트림 처리(Data Stream Processing) 시작...")
    merge_key = f"{role.lower()}_name"

    if merge_key not in df_template.columns:
        logger.error(f"❌ 메인 테이블에 필수 매칭 키 열({merge_key})이 없습니다. {role} 건너뜀.")
        return df_template

    df_result = df_template.copy()

    # 채울 열 초기화
    for col in ['gvkey', 'cusip', 'cik', 'compustat_name']:
        if col not in df_result.columns:
            df_result[col] = None

    # Compustat 기반 데이터를 고속 조회 사전(딕셔너리) 구조로 변환: {conm: {gvkey, cusip, cik}}
    conm_to_ids = df_comp_raw_unique.set_index('conm')[['gvkey', 'cusip', 'cik']].to_dict('index')

    # ---------------------------------------------------------
    # PART A: Compustat IDs 추가 (슈퍼 딕셔너리를 통해)
    # ---------------------------------------------------------
    logger.info(f"[{role}] Compustat ID 초고속 바인딩(Fast Binding) 중...")
    if dict_comp:
        matched_ids = 0
        for idx in df_result.index:
            ma_name = str(df_result.at[idx, merge_key]).strip()

            # 1. 메인 테이블 회사명 -> Compustat 표준 이름 변환
            if ma_name in dict_comp:
                comp_name = dict_comp[ma_name]

                # 2. Compustat 표준 이름 -> 주요 ID(3개) 추출
                if comp_name in conm_to_ids:
                    ids = conm_to_ids[comp_name]

                    # 값이 비어있을 때만 데이터를 채우며, 원래 있는 기존 값을 지우지 않음 (디폴트 방어)
                    if pd.isna(df_result.at[idx, 'gvkey']): df_result.at[idx, 'gvkey'] = ids.get('gvkey')
                    if pd.isna(df_result.at[idx, 'cusip']): df_result.at[idx, 'cusip'] = ids.get('cusip')
                    if pd.isna(df_result.at[idx, 'cik']):   df_result.at[idx, 'cik'] = ids.get('cik')
                    if pd.isna(df_result.at[idx, 'compustat_name']): df_result.at[idx, 'compustat_name'] = comp_name

                    matched_ids += 1

        logger.info(f"[{role}] Compustat 필드 채우기 완료(Filled)! 맵핑 성공률: {matched_ids} / {len(df_result)}")
    else:
        logger.warning(f"[{role}] Compustat 딕셔너리가 비어 있습니다. ID 바인딩 단계를 건너뜁니다.")

    # ---------------------------------------------------------
    # PART B: Patent 데이터 추가 (기존 로직 유지)
    # ---------------------------------------------------------
    logger.info(f"[{role}] 특허 연합 통계 및 결합 계산 중 (Aggregation Calculation)...")
    df_matched_patent = df_patent.copy()

    df_matched_patent['assignee_stripped'] = df_matched_patent['assignee'].astype(str).str.strip()
    # Patent 사전을 이용하여 매핑(맵핑) 변환
    df_matched_patent['Matched_Company'] = df_matched_patent['assignee_stripped'].map(dict_patent)

    df_matched_patent = df_matched_patent.dropna(subset=['Matched_Company']).copy()

    if df_matched_patent.empty:
        logger.warning(f"[{role}] 슈퍼 딕셔너리로 검색된 올해 특허 정보가 없습니다 (No Patent Match)!")
    else:
        df_matched_patent['application_year'] = pd.to_numeric(df_matched_patent['application_year'], errors='coerce')
        df_matched_patent = df_matched_patent.dropna(subset=['application_year'])
        df_matched_patent['application_year'] = df_matched_patent['application_year'].astype(int)

        inventor_name_cols = [f'inventor_name{i}' for i in range(1, 11)]
        for col in inventor_name_cols:
            if col not in df_matched_patent.columns:
                df_matched_patent[col] = np.nan

        df_matched_patent['final_inventor_count'] = calculate_inventor_count_vectorized(
            df_matched_patent, inventor_name_cols
        )

        # 총 특허 수 및 총 발명자 수(합) 추가 통계
        df_summary_total = df_matched_patent.groupby('Matched_Company').agg({
            'assignee': 'count',
            'final_inventor_count': 'sum'
        }).reset_index()
        df_summary_total.rename(columns={
            'assignee': 'total_patent_count',
            'final_inventor_count': 'total_inventor_sum',
            'Matched_Company': merge_key
        }, inplace=True)

        df_grouped = df_matched_patent.groupby(['Matched_Company', 'application_year']).agg({
            'assignee': 'count',
            'final_inventor_count': 'sum'
        }).reset_index()

        pivot_patent = df_grouped.pivot(index='Matched_Company', columns='application_year', values='assignee')
        pivot_patent.columns = [f'patent_{int(col)}' for col in pivot_patent.columns]

        pivot_inventor = df_grouped.pivot(index='Matched_Company', columns='application_year',
                                          values='final_inventor_count')
        pivot_inventor.columns = [f'patent_inventor_{int(col)}' for col in pivot_inventor.columns]

        df_stats = pd.concat([pivot_patent, pivot_inventor], axis=1).reset_index()
        df_stats = df_stats.rename(columns={'Matched_Company': merge_key})

        df_names = df_matched_patent.groupby('Matched_Company')['assignee'].apply(lambda x: list(set(x))).reset_index()
        max_len = df_names['assignee'].apply(len).max() if not df_names.empty else 0
        name_cols = ['patent_name'] + [f'patent_name_{i}' for i in range(1, max_len)]
        names_expanded = pd.DataFrame(df_names['assignee'].tolist(), index=df_names.index)
        names_expanded = names_expanded.iloc[:, :len(name_cols)]
        names_expanded.columns = name_cols[:names_expanded.shape[1]]
        df_names = pd.concat([df_names[['Matched_Company']], names_expanded], axis=1)
        df_names = df_names.rename(columns={'Matched_Company': merge_key})

        cols_to_remove = [c for c in df_result.columns if c.startswith('patent_') or c.startswith('patent_inventor_') or c.startswith('total_patent_') or c.startswith('total_inventor_')]
        if cols_to_remove:
            df_result = df_result.drop(columns=cols_to_remove, errors='ignore')

        df_result = pd.merge(df_result, df_stats, on=merge_key, how='left')
        df_result = pd.merge(df_result, df_names, on=merge_key, how='left')
        
        # 합계 통계(개수) 데이터 병합
        df_result = pd.merge(df_result, df_summary_total, on=merge_key, how='left')

        stat_cols = [c for c in df_result.columns if
                     (c.startswith('patent_') or c.startswith('patent_inventor_') or c.startswith('total_patent_') or c.startswith('total_inventor_')) and 'name' not in c]
        df_result[stat_cols] = df_result[stat_cols].fillna(0).astype(int)

        companies_with_patents = (df_result[stat_cols].sum(axis=1) > 0).sum()
        logger.info(f"[{role}] 특허 데이터 병합 완료! 특허 정보가 확보된 회사는 {companies_with_patents} / {len(df_result)} 개 입니다.")

    return df_result


# ==========================================
# 메인 프로세스 (Main Process)
# ==========================================

def main():
    logger.info("=" * 60)
    logger.info(" [Step 5] Starting Final Data Aggregation Engine (최종 통합 엔진 시작) ")
    logger.info("=" * 60)

    # 1. 데이터 소스 마운트 (Mount Data Sources)
    logger.info("\n--- [1] 원본 DB(테이블)와 슈퍼 딕셔너리 로드 중 ---")

    # Acquiror 와 Target 시트를 개별적으로(각각) 로드
    df_template_raw_acq = pd.read_excel(PATH_FINAL_OUTCOME, sheet_name='acquiror')
    df_template_raw_tgt = pd.read_excel(PATH_FINAL_OUTCOME, sheet_name='target')

    if TESTING_ROWS:
        df_template_acq = df_template_raw_acq.head(TESTING_ROWS)
        df_template_tgt = df_template_raw_tgt.head(TESTING_ROWS)
        logger.warning(f"⚠️ 테스트 모드(Test Mode): 결합 템플릿(메인 테이블)으로 처음 {TESTING_ROWS} 개 행만 유지합니다.")
    else:
        df_template_acq = df_template_raw_acq.copy()
        df_template_tgt = df_template_raw_tgt.copy()

    # Compustat 기본 맵 추출 및 conm을 기준으로 직접 중복을 제거하여 신속한 매핑에 대비
    df_comp_raw = pd.read_csv(PATH_COMPUSTAT_RAW, usecols=['conm', 'gvkey', 'cusip', 'cik'], dtype=str,
                              low_memory=False)
    df_comp_raw_unique = df_comp_raw.drop_duplicates(subset=['conm']).copy()
    logger.info(f"Compustat 기본 맵 로드 성공 (Unique Entries): 독립 법인 엔티티 {len(df_comp_raw_unique)} 개 ")

    # 4개의 슈퍼 딕셔너리 동시에(모두) 로드
    dict_patent_acq = load_super_dictionary(PATH_MASTER_DICT_ACQ)
    dict_patent_tgt = load_super_dictionary(PATH_MASTER_DICT_TGT)
    dict_comp_acq = load_super_dictionary(PATH_COMP_DICT_ACQ)
    dict_comp_tgt = load_super_dictionary(PATH_COMP_DICT_TGT)

    df_patent = pd.read_csv(PATH_PATENT_DB, low_memory=False)
    df_patent.dropna(subset=['assignee'], inplace=True)
    logger.info(f"특허 연도 테이블(Data) 로드 성공: 사용할 독립 특허수 {len(df_patent)} 개")

    # 2. 핵심(집계) 계산 스트림 실행
    logger.info("\n--- [2] 이중 결합 병합 파이프라인(Dual Pipeline) 실행 컴퓨팅 ---")
    df_final_acquiror = execute_pipeline("Acquiror", df_template_acq, dict_patent_acq, dict_comp_acq, df_comp_raw_unique,
                                         df_patent)
    df_final_target = execute_pipeline("Target", df_template_tgt, dict_patent_tgt, dict_comp_tgt, df_comp_raw_unique,
                                       df_patent)

    # 3. 산출물 내보내기 및 엑셀 저장 (Export)
    logger.info("\n--- [3] 영구 저장(엑셀 내보내기, Exporting to excel) ---")
    try:
        with pd.ExcelWriter(PATH_OUTPUT_COMPLETE) as writer:
            df_final_acquiror.to_excel(writer, sheet_name='Acquiror', index=False)
            df_final_target.to_excel(writer, sheet_name='Target', index=False)
        logger.info(f"🎉 최종 다중 시트 저장 성공! 추출물 파일 경로 (Output path): {PATH_OUTPUT_COMPLETE}")
    except Exception as e:
        logger.error(f"❌ 임무 저장 실패(Failed to export): {e}")


if __name__ == "__main__":
    main()
