# -*- coding: utf-8 -*-
"""
Step 1: Patent Matching Pipeline (특허 매칭 파이프라인)
==================================
Features (기능):
- Automated cleaning (자동 데이터 정제: Exact Match + Fuzz Ratio)
- AI-based verification (AI 기반 검증: Gemini API 검토)
"""

import os
import json
import time
import math
import logging
import re
from datetime import datetime
import pandas as pd
import numpy as np
from rapidfuzz import fuzz, process
from tqdm import tqdm
from multiprocessing import Pool, cpu_count, set_start_method
from google import genai
from google.genai import types
import warnings

warnings.filterwarnings('ignore')

# ==========================================
# ⚙️ 전역 설정 (Global Configuration)
# ==========================================
os.makedirs('pipeline_outputs/logs', exist_ok=True)
_log_file = f"pipeline_outputs/logs/pipeline_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler(_log_file, encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger('Pipeline')

# ==============================================================================
# 🌟🌟🌟 【이곳에서 설정 매개변수를 수정하세요 (Configuration)】 🌟🌟🌟
# ==============================================================================

# 1. API 설정 (API Configuration)
API_KEY = "[ENCRYPTION_KEY]"
MODEL_STAGE_1 = "gemini-3.1-flash-lite-preview" 
BATCH_SIZE = 100       # 대형 모델에 한 번에 보낼 항목 수 (권장: 100-200)

# 2. 매칭 임계값 (Matching Threshold)
FUZZY_THRESHOLD = 90   # 몇 점 이상의 의심되는 목록을 AI 검토로 넘길 것인가? (권장: 85)

# 3. 데이터 경로 설정 (파일 경로가 올바른지 확인하세요)
PATENT_DB_PATH = '/Users/lidachuan/Desktop/RA_work/patent_dataset/2010_2019/2019.csv'
TARGET_DB_PATH = '/Users/lidachuan/Desktop/RA_work/expected_outcome/final_outcome_fn.xlsx'
OUTPUT_DIR = '/Users/lidachuan/Desktop/RA_work/pipeline_outputs'  # 생성된 4개의 Final 테이블이 여기에 저장됩니다.

# 4. 테스트 스위치 (200만 건의 전체 데이터를 실행하려면 TESTING_ROWS를 None으로 변경하세요!!!)
# -----------------------------------------------------------
TESTING_ROWS = 500  # None은 전체 실행을 의미하며, 5000이면 처음 5000줄만 실행됨
# -----------------------------------------------------------

# ==============================================================================
# 🌟🌟🌟 【설정 매개변수 종료】 🌟🌟🌟
# ==============================================================================

client = genai.Client(api_key=API_KEY)


# ==========================================
# 📝 대형 언어 모델 프롬프트 (LLM Prompt)
# [한국어 번역 참고용 - 프롬프트 원문은 수정하지 않음]
# 역할: 당신은 수석 특허 데이터베이스 엔지니어입니다. 목표는 기업 간 법적 주체 분리(모회사/자회사, 다국적 지사, 다른 사업부 등) 가능성이 있는 모든 쌍을 제거하는 매우 정밀한 1차 필터링을 수행하는 것입니다.
# 핵심 판단 기준: "핵심 독립 원칙": "그들이 같은 회사일 수 있다"고 추측하는 것은 엄격히 금지됩니다. (중략)
# 1. MISMATCH (판단: 다른 법인) ...
# 2. UNCERTAIN (불확실, 의심되는 동일 법인) ...
# 엔지니어의 작업 지침: (1. 접미사 분리, 2. 단어 대조, 3. 차이점 찾기, 4. 오판 금지)
# 엄격히 다음 JSON 배열 형식으로 출력하세요.
# ==========================================
PROMPT_STAGE_1 = """角色定位：
你是一名专利数据库高级工程师。你的目标是执行极高精度的初步拦截，将一切在法律上可能存在主体隔离（如母子公司、跨国分部、不同事业部）的对子剔除。

核心判定准则：
“核心独立原则”：严禁推测“它们可能是一家”。只要双方名称在剥离了法定后缀（Inc, LLC等）后，存在任何具有实质业务含义、地理特征或组织性质的单词差异，一律判定为 MISMATCH。

1. MISMATCH（判定为：不同法人）
符合以下任一情况，必须判定为 MISMATCH：

【实质性业务词冲突】：一方含有具体的业务、产品或技术描述词，而另一方缺失。
例子：Dynamic Flow Computers vs Dynamic Corp（多出业务词 Flow/Computers）
例子：Titan Additive LLC vs Titan Inc（多出技术词 Additive）

【法域与分部特征词】：含有地点或特定组织层级词。
例子：Altra Industrial Motion (Shenzhen) Co. vs Altra Industrial Motion Corp（存在地理隔离）
例子：IBM China vs IBM Corp（分公司与总公司法律人格独立）

【法律组织形式冲突】：法律后缀暗示了不同的注册类型（除非在 UNCERTAIN 中定义的同义缩写）。
例子：Sutura Industries LLC vs Sutura Inc（LLC 与 Inc 注册性质不同）
例子：CME LLC vs CME LP（有限责任公司与有限合伙，主体绝对独立）

【泛用词陷阱】：共享极常见首词（如 Advanced, Systems, Global, Digital 等），但后续实词不匹配。

【极简 vs 详细】：一方是宽泛的集团名（如 X Group），另一方含有具体部门（如 X Medical Group）。

2. UNCERTAIN（不确定，疑似同一法人）
【绝对收紧】 仅在以下情况保留，进入人工复核阶段：

【纯等效后缀替换】：仅存在定冠词 "The" 的增减或法律后缀的同义缩写。
公认等效缩写：Technologies = Tech, International = Intl, Corporation = Corp, Manufacturing = Mfg, Limited = Ltd。
例子：The United States Playing Card Company vs United States Playing Card Co。

【数据库标注干扰】：双方商号完全一致，仅一方多出了数据库内部备注（通常在末尾）。
例子：Special Devices, Inc. vs Special Devices Inc-Defense。

【纯翻译对齐】：中英文准确 1:1 翻译，且不含上述 MISMATCH 中的地域或业务冲突。

工程师的操作指令：
1. 先剥离后缀：移除 Co, Inc, LLC, GmbH, Ltd, S.A., 及其公认缩写。
2. 单词比对：对剩余部分进行逐词比对。
3. 找差异项：只要发现一个多余的实词（非定冠词），立即判定为 MISMATCH。
4. 宁错杀，勿冒进：宁可将疑似项判为 MISMATCH，也不允许将含有不同业务含义的名称判为一致。

根据要求核实以下 {n} 对公司名称：
{pairs}

请严格按以下 JSON 数组格式输出，不要包含任何其他文字：
[
  {{"id": 1, "status": "MISMATCH" | "UNCERTAIN"}},
  ...
]
"""

# ==========================================
# 🛠️ 도구 모듈：자동 데이터 정제 (Step 1)
# ==========================================
def clean_company_name(name):
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
        r'\bCO\.?\b': 'COMPANY',
        r'\bLTD\.?\b': 'LIMITED',
        r'\bLLC\b': 'L.L.C.',
        r'\bL\.L\.C\.?\b': 'L.L.C.',
        r'\bPLC\.?\b': 'P.L.C.'
    }
    for abbr, full in abbreviations.items():
        name = re.sub(abbr, full, name)
        
    name = re.sub(r'[^A-Z0-9\s.]', ' ', name)
    name = re.sub(r'\s+', ' ', name).strip()
    return name

def calculate_inventor_count_vectorized(df, inventor_cols):
    num_from_column = pd.to_numeric(df['inventors'], errors='coerce').fillna(0)
    num_from_names = df[inventor_cols].notna().sum(axis=1)
    return np.maximum(num_from_column, num_from_names)

def fuzzy_match_batch(args):
    unmatched_chunk, target_list, threshold = args
    results = []
    for row in unmatched_chunk.to_dict('records'):
        assignee = row['assignee']
        clean = row['clean_name']
        match_result = process.extractOne(
            clean, target_list, scorer=fuzz.ratio, score_cutoff=threshold
        )
        if match_result:
            match_name, score, _ = match_result
            results.append({
                '특허 회사명(专利公司名)': assignee, # 컬럼 이름은 원본 또는 병용으로 유지하여 나중에 사용에 지장이 없도록 함 / Pandas Keys
                '특허 정제명(专利清洗名)': clean,
                '매칭 라이브러리 정제명(匹配库清洗名)': match_name,
                '매칭 대략적 점수(匹配粗筛分)': score
            })
    return results

# ==========================================
# 🛠️ 도구 모듈：대형 모델 API 통신 (Step 2)
# ==========================================
def safe_json_parse(text):
    if not text: raise ValueError("응답이 비어있습니다 (响应为空)")
    text = text.strip()
    text = re.sub(r'^```(?:json)?\s*', '', text)
    text = re.sub(r'\s*```$', '', text)
    try: return json.loads(text)
    except: pass
    
    arr_m = re.search(r'\[[\s\S]*?\]', text)
    if arr_m:
        try: return json.loads(arr_m.group())
        except: pass
        
    import ast
    for pat in [r'\[[\s\S]*?\]', r'\{[\s\S]*?\}']:
        m = re.search(pat, text)
        if m:
            try: return ast.literal_eval(m.group())
            except: pass
    raise ValueError(f"파싱 실패 (解析失败)：{text[:100]}")

def call_with_retry(fn, max_retries=3):
    for attempt in range(max_retries):
        try: return fn()
        except Exception as e:
            if '429' in str(e) or 'RESOURCE_EXHAUSTED' in str(e):
                time.sleep(2 ** attempt)
            else:
                if attempt == max_retries -1: raise
                
def process_batch_via_api(batch_df, sheet_type):
    pairs_text = "\n".join(
        f"{i+1}. A (특허): {row['특허 회사명(专利公司名)']} | B ({sheet_type}): {row['대조 회사명(对照公司名)']}"
        for i, row in enumerate(batch_df.to_dict('records'))
    )
    
    def _call():
        response = client.models.generate_content(
            model=MODEL_STAGE_1,
            contents=PROMPT_STAGE_1.format(n=len(batch_df), pairs=pairs_text),
            config=types.GenerateContentConfig(response_mime_type='application/json')
        )
        return safe_json_parse(response.text)
        
    try:
        result = call_with_retry(_call)
        return result
    except Exception as e:
        logger.warning(f"[API] 배치 (Batch) 예외 발생: {e}")
        return [{"id": i+1, "status": "UNCERTAIN"} for i in range(len(batch_df))]


# ==========================================
# 🌪️ 핵심 처리 로직 (단일 시트 파이프라인 실행)
# ==========================================
def process_pipeline_for_sheet(sheet_type, df_main_original, df_summary):
    logger.info(f"\n{'='*50}")
    logger.info(f" 🚀 하위 파이프라인 시작: {sheet_type.upper()}")
    logger.info(f"{'='*50}")
    
    orig_col_name = f'{sheet_type}_name'
    df_main_original['clean_name'] = df_main_original[orig_col_name].apply(clean_company_name)
    clean_to_orig = dict(zip(df_main_original['clean_name'], df_main_original[orig_col_name]))
    target_clean_list = list(clean_to_orig.keys())
    
    # ── 단계 1：로컬 알고리즘 정확한 할당 ──
    matches_auto = []
    unmatched_list = []
    
    for row in tqdm(df_summary.to_dict('records'), desc=f"[{sheet_type}] 정확히 일치 찾기(精确匹配)"):
        clean = row['clean_name']
        if clean in clean_to_orig:
            matches_auto.append({
                'Assignee_Original': row['assignee'],
                f'{sheet_type.capitalize()}_Original': clean_to_orig.get(clean, ''),
                'Match_Type': 'Strict_Local',
            })
        else:
            unmatched_list.append(row)
            
    logger.info(f"[{sheet_type}] 단계 1 (직접 승인 통과): {len(matches_auto)} 건")
    
    # ── 단계 2：로컬 알고리즘 퍼지 리콜 ──
    matches_fuzzy = []
    if unmatched_list:
        df_unmatched = pd.DataFrame(unmatched_list)
        if len(df_unmatched) > 100:
            n_cores = max(1, min(cpu_count() - 1, 4))
            chunks = np.array_split(df_unmatched, n_cores)
            with Pool(n_cores) as pool:
                args_list = [(chunk, target_clean_list, FUZZY_THRESHOLD) for chunk in chunks]
                for batch_result in tqdm(
                    pool.imap(fuzzy_match_batch, args_list),
                    total=n_cores,
                    desc=f"[{sheet_type}] 퍼지 1차 필터링(模糊初筛)"
                ):
                    for r in batch_result:
                        r['대조 회사명(对照公司名)'] = clean_to_orig.get(r['매칭 라이브러리 정제명(匹配库清洗名)'], '')
                    matches_fuzzy.extend(batch_result)
        else:
            for row in tqdm(df_unmatched.to_dict('records'), desc=f"[{sheet_type}] 퍼지 1차 필터링(模糊初筛)"):
                assignee, clean = row['assignee'], row['clean_name']
                match_result = process.extractOne(clean, target_clean_list, scorer=fuzz.ratio, score_cutoff=FUZZY_THRESHOLD)
                if match_result:
                    match_name, score, _ = match_result
                    matches_fuzzy.append({
                        '특허 회사명(专利公司名)': assignee,
                        '특허 정제명(专利清洗名)': clean,
                        '대조 회사명(对照公司名)': clean_to_orig.get(match_name, ''),
                        '매칭 라이브러리 정제명(匹配库清洗名)': match_name,
                        '매칭 대략적 점수(匹配粗筛分)': score
                    })

    logger.info(f"[{sheet_type}] 단계 2 (대형 모델 검사 전송): {len(matches_fuzzy)} 건")
    
    # ── 단계 3：대형 모델 API 엄격한 심사 ──
    final_review_list = []
    if matches_fuzzy:
        df_api_input = pd.DataFrame(matches_fuzzy)
        n_batches = math.ceil(len(df_api_input) / BATCH_SIZE)
        
        for batch_idx in tqdm(range(n_batches), desc=f"[{sheet_type}] AI 심사"):
            start = batch_idx * BATCH_SIZE
            end = min(start + BATCH_SIZE, len(df_api_input))
            batch_df = df_api_input.iloc[start:end].copy()
            
            api_res = process_batch_via_api(batch_df, sheet_type)
            
            for item in api_res:
                local_id = item.get('id', 0) - 1
                if 0 <= local_id < len(batch_df):
                    global_idx = start + local_id
                    row_data = df_api_input.iloc[global_idx].to_dict()
                    status = item.get('status', 'UNCERTAIN')
                    
                    if status == 'MISMATCH':
                        pass # 다른 법인 판단 시 바로 폐기
                    elif status == 'UNCERTAIN':
                        # 극단적으로 의심되는 경우에만 최종 인간의 검토 권한 필요
                        final_review_list.append({
                            'Assignee_Original': row_data['특허 회사명(专利公司名)'],
                            f'{sheet_type.capitalize()}_Original': row_data['대조 회사명(对照公司名)'],
                            'Match_Type': f"AI_Review (Score:{row_data['매칭 대략적 점수(匹配粗筛分)']})"
                        })
    
    # ── 마무리: 결과 저장 ──
    out_dir = OUTPUT_DIR
    
    if matches_auto:
        pd.DataFrame(matches_auto).to_excel(f"{out_dir}/Final_Auto_Matched_{sheet_type.capitalize()}.xlsx", index=False)
        logger.info(f"💾 {sheet_type} 자동 일치 목록(수용 표)이 저장되었습니다. ({len(matches_auto)} 건)")
        
    if final_review_list:
        df_review = pd.DataFrame(final_review_list)
        df_review.index += 1
        df_review.to_excel(f"{out_dir}/Final_Manual_Review_{sheet_type.capitalize()}.xlsx", index_label="번호(序号)")
        logger.info(f"💾 {sheet_type} 수동(인간) 검토 대상 파일(블랙박스)이 저장되었습니다. ({len(final_review_list)} 건)")


# ==========================================
# 🚀 메인 컨트롤 센터 (Total Control Center)
# ==========================================
def main():
    start_time = datetime.now()
    logger.info("*"*60)
    logger.info(" [Step 1] Initializing Patent Matching Pipeline (특허 매칭 파이프라인 초기화 중)")
    logger.info("*"*60)
    
    # 공통 전처리 (Common Preprocessing)
    logger.info("\n--- [1/3] 특허 특징 영역 로드 및 전처리 ---")
    if TESTING_ROWS:
        logger.warning(f"⚠️ 주의: 현재 테스트 모드가 켜져 있습니다. 전체 데이터에서 무작위로 {TESTING_ROWS} 행을 추출 중입니다!")
        df_patent_full = pd.read_csv(PATENT_DB_PATH)
        df_patent = df_patent_full.sample(n=min(TESTING_ROWS, len(df_patent_full)), random_state=42)
        del df_patent_full # 메모리 확보 (Free memory)
        
        # 후속 단계 (예: step 3)를 위해 생성된 무작위 추출 서브셋 보관
        test_db_path = f'/Users/lidachuan/Desktop/RA_work/patent_dataset/2010_2019/2019_test_{TESTING_ROWS}.csv'
        df_patent.to_csv(test_db_path, index=False)
        logger.info(f"✅ 무작위 추출된 기본 데이터가 저장되었습니다: {test_db_path}")
    else:
        logger.info(f"🚀 전체 특허 데이터를 읽어오는 중 (약 200만 행, 잠시 기다려 주십시오)...")
        df_patent = pd.read_csv(PATENT_DB_PATH)
        
    df_patent.dropna(subset=['assignee'], inplace=True)
    df_patent['clean_name'] = df_patent['assignee'].apply(clean_company_name)
    df_patent = df_patent[df_patent['clean_name'] != ""].copy()
    
    inventor_name_cols = [f'inventor_name{i}' for i in range(1, 11)]
    for col in inventor_name_cols:
        if col not in df_patent.columns: df_patent[col] = np.nan
    df_patent['final_inventor_count'] = calculate_inventor_count_vectorized(df_patent, inventor_name_cols)
    
    df_summary = df_patent.groupby(['assignee', 'clean_name']).agg({'application_year': 'count'}).reset_index()
    logger.info(f"특허 기본 설정 데이터베이스의 수분 제거 완료, 총 {len(df_summary)} 개의 독립 엔티티 확보")

    # 데이터 흐름 분배 (Distribute Data Stream)
    logger.info("\n--- [2/3] 이중 코어 파이프라인에 데이터 분배 중 ---")
    df_acquiror = pd.read_excel(TARGET_DB_PATH, sheet_name='acquiror')
    process_pipeline_for_sheet('acquiror', df_acquiror, df_summary)

    df_target = pd.read_excel(TARGET_DB_PATH, sheet_name='target')
    process_pipeline_for_sheet('target', df_target, df_summary)

    logger.info("\n" + "*"*60)
    logger.info(f" 전역 실행 종료! 총 소요 시간: {(datetime.now() - start_time).total_seconds()/60:.2f} 분")
    logger.info("*"*60)

if __name__ == "__main__":
    set_start_method('fork')
    main()
