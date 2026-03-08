# -*- coding: utf-8 -*-
"""
Step 3: Compustat Matching Pipeline (Compustat 매칭 파이프라인)
================================================
Function (기능): 퍼지 로직과 AI를 사용하여 M&A 회사명을 Compustat 엔티티(법인)에 일치시킵니다.
(Matches M&A company names to Compustat entities using fuzzy logic and AI.)
"""

import pandas as pd
import re
import os
import logging
from rapidfuzz import process, fuzz
from tqdm import tqdm
from datetime import datetime
from google import genai
from google.genai import types

# ==============================================================================
# 🌟🌟🌟 【이곳에서 설정 매개변수를 수정하세요 (Configuration)】 🌟🌟🌟
# ==============================================================================

# 1. API 설정 (API Configuration)
API_KEY = "---"
MODEL_STAGE_1 = "gemini-3.1-flash-lite-preview"
BATCH_SIZE = 100

# 2. 매칭 임계값 (기본 모델과 동일한 극도로 엄격한 기준 적용)
FUZZY_THRESHOLD = 85

# 3. 데이터 경로 설정 (Data Path Configuration)
PATH_DICT_ACQ = "/Users/lidachuan/Desktop/RA_work/pipeline_outputs/Master_Dict_Acquiror_VIEW.xlsx"
PATH_DICT_TGT = "/Users/lidachuan/Desktop/RA_work/pipeline_outputs/Master_Dict_Target_VIEW.xlsx"
PATH_COMPUSTAT = "/Users/lidachuan/Desktop/RA_work/compustat_dataset/compustat_19802025.csv"
OUTPUT_DIR = "/Users/lidachuan/Desktop/RA_work/pipeline_outputs"



# ==============================================================================

client = genai.Client(api_key=API_KEY)

# 로그 설정 (Configure Logging)
os.makedirs('pipeline_outputs/logs', exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler(f'pipeline_outputs/logs/Compustat_Pipeline_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger('CompustatPipeline')


# ==========================================
# 핵심 함수: 정제 및 대형 모델 판단 (기본 모델의 엄격한 기준 복제)
# ==========================================

def clean_company_name(name):
    """표준 정제 함수 (과도한 정제를 방지하고 법인 주체의 특징을 유지함)"""
    if pd.isna(name) or not isinstance(name, str):
        return ""
    name = str(name).upper().strip()
    name = name.replace('&', ' AND ')
    name = name.replace('-', ' ')
    name = name.replace("'", '')
    
    abbreviations = {
        r'\bINTL\b': 'INTERNATIONAL',
        r'\bNATL\b': 'NATIONAL',
        r'\bCORP\b': 'CORPORATION',
        r'\bINC\b': 'INCORPORATED',
        r'\bMFG\b': 'MANUFACTURING',
        r'\bTECH\b': 'TECHNOLOGY',
        r'\bSYS\b': 'SYSTEMS',
    }
    for abbr, full in abbreviations.items():
        name = re.sub(abbr, full, name)
        
    s_map = {
        r'\bCORP\.?\b': 'CORPORATION',
        r'\bINC\.?\b': 'INCORPORATED',
        r'\bLTD\.?\b': 'LIMITED',
        r'\bCO\.?\b': 'COMPANY',
        r'\bL\.L\.C\.?\b': 'LLC'
    }
    for k, v in s_map.items():
        name = re.sub(k, v, name, flags=re.IGNORECASE)
        
    name = re.sub(r'[^A-Z0-9\s]', ' ', name)
    name = re.sub(r'\s+', ' ', name).strip()
    return name

# [한국어 번역 참고용 - 시스템 프롬프트 원문은 변경하지 않음]
# 역할: 귀하는 세계 최고의 금융/법률 데이터 감사 전문가입니다. 임무는 [회사A]와 [회사B]가 동일한 법적 인격체와 비즈니스 실체를 가졌는지 판단하는 것입니다.
# 규칙: 
# 1. 【철자가 매우 유사하지만 실체가 다름】: Dynamic vs Dynamic Flow Computers LLC (퍼지 임계값을 사용했으므로 흔히 발생하며, 반드시 MISMATCH 처리해야 함)
# 2. 【회사 형태 독립성 유지】: Inc vs LLC (MISMATCH)
# 3. 【모회사/자회사 독립성 유지】: IBM China vs IBM Corp (MISMATCH)
# 출력 요구사항: 오직 MISMATCH 또는 UNCERTAIN 만으로 대답하세요.
# - MISMATCH: 동일 회사가 아님이 확실하므로 즉시 삭제합니다.
# - UNCERTAIN: 동일 회사일 확률이 매우 높거나 과거 이름/동음이의어/가벼운 오타인 경우 인간의 직접 검토 단계로 넘깁니다. 줄 단위로 판단하여 반환하세요.
def call_gemini_stage1_batch(pairs):
    system_instruction = """
您是一个世界顶尖的金融法律数据审计专家。您的任务是判断[公司A]和[公司B]是否具有相同的法律人格和商业实质。
规则：
1. 【拼写极其相似但实质不同】：Dynamic vs Dynamic Flow Computers LLC（由于我们用的比较模糊的阈值，这种情况常发生，必须判定为 MISMATCH）
2. 【保留公司形态独立性】：Inc vs LLC（MISMATCH）
3. 【保留母子公司独立性】：IBM China vs IBM Corp（MISMATCH）

输出要求：
只回答 MISMATCH 或 UNCERTAIN。
- MISMATCH: 确定不是同一家公司，直接抹除。
- UNCERTAIN: 极大可能是同一家公司，或者确实属于曾用名/同音字/轻微笔误，允许进入人工审核阶段。
请按行返回结果。
"""
    prompt = "请对以下公司对进行判断：\n"
    for i, (a, b) in enumerate(pairs):
        prompt += f"{i+1}. [公司A]: {a} vs [公司B]: {b}\n"
    
    try:
        response = client.models.generate_content(
            model=MODEL_STAGE_1,
            contents=prompt,
            config=types.GenerateContentConfig(
                system_instruction=system_instruction,
                temperature=0.0
            )
        )
        return response.text.strip().split('\n')
    except Exception as e:
        logger.error(f"API 요청 실패 (API Request Failed): {e}")
        return ["UNCERTAIN"] * len(pairs)

def process_dictionary(dict_type, dict_path, comp_clean_set, comp_clean_list, clean_to_original_comp):
    logger.info("=" * 60)
    logger.info(f"--- 딕셔너리 처리 중 (Processing Dictionary): [{dict_type}] ---")
    logger.info("=" * 60)
    
    df_dict = pd.read_excel(dict_path)
    unique_names = list(set(df_dict['Matched_Company_Name'].dropna()))
    df_ma = pd.DataFrame({'company_name': unique_names})
    
    logger.info(f"🚀 {dict_type} 전체 매칭 대기 엔티티 읽는 중... (총 {len(df_ma)} 개의 회사)")
        
    df_ma['clean_company'] = df_ma['company_name'].apply(clean_company_name)
    df_ma = df_ma[df_ma['clean_company'] != ""].copy()
    
    # 3. 차단 (Intercept/Filter)
    logger.info(f"\n--- [3] 신속한 필터링 및 퍼지 검색 ({dict_type}) ---")
    auto_matches = []
    fuzzy_candidates = []
    
    ma_unique_companies = df_ma[['company_name', 'clean_company']].drop_duplicates(subset=['clean_company'])
    
    for _, row in tqdm(ma_unique_companies.iterrows(), total=len(ma_unique_companies), desc=f"{dict_type} 인터셉트 중(拦截)"):
        orig = row['company_name']
        clean = row['clean_company']
        
        # 정확히 일치 찾기 (Exact Match)
        if clean in comp_clean_set:
            auto_matches.append({
                'Dictionary_Company_Name': orig,
                'Company_Clean': clean,
                'Matched_Compustat_Clean': clean,
                'Matched_Compustat_Original': clean_to_original_comp[clean],
                'Match_Type': 'Strict_Auto',
                'Score': 100
            })
            continue
            
        # 퍼지 검색 (Fuzzy Match)
        f_match = process.extractOne(clean, comp_clean_list, scorer=fuzz.ratio, score_cutoff=FUZZY_THRESHOLD)
        if f_match:
            candidate_clean, score, _ = f_match
            fuzzy_candidates.append({
                'Dictionary_Company_Name': orig,
                'Company_Clean': clean,
                'Matched_Compustat_Clean': candidate_clean,
                'Matched_Compustat_Original': clean_to_original_comp[candidate_clean],
                'Match_Type': 'Fuzzy_Uncertain',
                'Score': score
            })

    logger.info(f" -> 확인 및 자동 통과(정확히 일치): {len(auto_matches)} 건")
    logger.info(f" -> 퍼지 검색(대형 모델 심사 대기): {len(fuzzy_candidates)} 건 (곧 검토 대상으로 전송)")
    
    # 4. Gemini 대형 모델 재검토
    logger.info(f"\n--- [4] 대형 모델 판단 시작 ({dict_type}) ---")
    manual_review_matches = []
    
    if fuzzy_candidates:
        pairs_to_judge = [(item['Company_Clean'], item['Matched_Compustat_Clean']) for item in fuzzy_candidates]
        
        for i in tqdm(range(0, len(pairs_to_judge), BATCH_SIZE), desc="API 전송 (API 送审)"):
            batch = pairs_to_judge[i:i+BATCH_SIZE]
            batch_items = fuzzy_candidates[i:i+BATCH_SIZE]
            
            results = call_gemini_stage1_batch(batch)
            
            for item, res in zip(batch_items, results):
                if 'MISMATCH' not in res.upper():
                    manual_review_matches.append(item)
                    
    logger.info(f"대형 모델의 판단이 완료되었습니다. 고위험 오류 {len(fuzzy_candidates) - len(manual_review_matches)}개를 최종적으로 제거했습니다!")

    # 5. 결과 저장 (Save Results)
    logger.info(f"\n--- [5] 최종 레포트 내보내기 ({dict_type}) ---")
    if auto_matches:
        out_auto = os.path.join(OUTPUT_DIR, f'Compustat_Auto_Matched_{dict_type}.xlsx')
        pd.DataFrame(auto_matches).to_excel(out_auto, index=False)
        logger.info(f"✅ 자동 통과 명부를 저장했습니다: {os.path.basename(out_auto)} ({len(auto_matches)} 건)")
        
    if manual_review_matches:
        out_manual = os.path.join(OUTPUT_DIR, f'Compustat_Manual_Review_{dict_type}.xlsx')
        pd.DataFrame(manual_review_matches).to_excel(out_manual, index=False)
        logger.info(f"✅ 수동 복검 명부를 저장했습니다: {os.path.basename(out_manual)} ({len(manual_review_matches)} 건)")


def main():
    logger.info("=" * 60)
    logger.info(" [Step 3] Initializing Compustat Matching Pipeline (Compustat 매칭 파이프라인 초기화 중)")
    logger.info("=" * 60)

    # 1. Compustat 데이터를 사전 로드하고 베이스 라이브러리를 설정 (시간 절약을 위해 한 번만 로드)
    logger.info("\n--- [1] 전역 Compustat 베이스 로드 중 ---")
    try:
        df_comp = pd.read_csv(PATH_COMPUSTAT, usecols=['conm'], low_memory=False)
    except:
        df_comp = pd.read_csv(PATH_COMPUSTAT, low_memory=False)
    
    df_comp['clean_conm'] = df_comp['conm'].apply(clean_company_name)
    compustat_unique = df_comp[df_comp['clean_conm'] != ""][['conm', 'clean_conm']].drop_duplicates(subset=['clean_conm'])
    comp_clean_set = set(compustat_unique['clean_conm'])
    comp_clean_list = list(compustat_unique['clean_conm'])
    clean_to_original_comp = dict(zip(compustat_unique['clean_conm'], compustat_unique['conm']))
    
    logger.info(f"Compustat 내의 독립 엔티티(법인) 수: {len(comp_clean_list)}")

    # 2. Acquiror 및 Target 을 개별 처리
    process_dictionary('Acquiror', PATH_DICT_ACQ, comp_clean_set, comp_clean_list, clean_to_original_comp)
    process_dictionary('Target', PATH_DICT_TGT, comp_clean_set, comp_clean_list, clean_to_original_comp)

    logger.info("\n🎉 모든 파이프라인 실행 완료 (All pipelines successfully executed)!")

if __name__ == "__main__":
    main()
