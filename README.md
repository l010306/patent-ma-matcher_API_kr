# 특허-Compustat M&A 데이터 매칭 파이프라인 (Patent-Compustat M&A Data Matching Pipeline)

이 문서는 5단계로 이루어진 특허 및 재무(Compustat) 데이터 매칭 파이프라인의 핵심 원리와 사용법을 설명합니다. 
(本文档详细说明了由5个步骤组成的专利及Compustat数据匹配管道的核心原理及使用说明。)

## 1. 파이프라인 개요 (Pipeline Overview / 流程概述)

전체 파이프라인은 M&A(인수합병) 사건의 `Acquiror`(인수자) 및 `Target`(피인수자) 기업명을 기준으로 **특허 데이터베이스** 및 **Compustat 재무 데이터베이스**와 자동으로 매칭하고 검증하는 시스템입니다. 대규모 기업 명칭 데이터를 정제, 퍼지 매칭(Fuzzy Match) 및 대형 언어 모델(Gemini API) 기반의 엄격한 판별을 거쳐 매우 높은 정밀도의 매칭 결과를 담보합니다.

---

## 2. 각 단계별 원리 및 설명 (Principles by Step / 各步骤原理及说明)

### Step 1: 자동 매칭 및 AI 검증 (Patent Matching Pipeline)
- **파일 (File)**: `step1_patent_matching_kr.py`
- **원리 (Principle)**: 
  1. 먼저 특허 데이터에서 불필요한 공백과 법인 형태 약어(Inc, LLC 등)를 정제합니다. 
  2. M&A 주체와 **정확하게 일치(Exact Match)**하는 기업명은 바로 통과시킵니다.
  3. 일치하지 않는 기업명은 **퍼지 로직(Fuzzy)**으로 점수를 부여해 1차 필터링합니다. (기본 임계값: 90)
  4. 이후 **Gemini API**를 통해 두 이름이 본질적으로 동일한 법인인지(MISMATCH 또는 UNCERTAIN) 판단하여 심사합니다. 판단에서 살아남은 대단히 의심스러운 항목만 인간 검토를 위한 블랙박스 엑셀 파일(`Final_Manual_Review_...`)로 넘어갑니다.
- **결과 (Output)**: 자동 일치 항목(`Final_Auto_Matched`) 및 수동 검토용 항목(`Final_Manual_Review`).

### Step 2: 특허 슈퍼 딕셔너리 구축 (Patent Master Dictionary Builder)
- **파일 (File)**: `step2_patent_dictionary_kr.py`
- **원리 (Principle)**: 
  Step 1에서 얻은 엑셀 파일들(자동 매칭 명단, 수동 검토 대상 표)을 모두 통합하여 **M&A 기업명 -> 특허 기업명** 쌍을 매핑하는 대규모 사전(Super Dictionary)을 구축합니다. 충돌하는 값(Conflict)이 있으면 최초 매핑을 유지하고 경고를 로그에 남깁니다.
- **결과 (Output)**: `.pkl` 형식과 사용자가 편리하게 내용을 볼 수 있는 `_VIEW.xlsx` 형식의 마스터 딕셔너리.

### Step 3: Compustat 매칭 파이프라인 (Compustat Matching Pipeline)
- **파일 (File)**: `step3_compustat_matching_kr.py`
- **원리 (Principle)**:
  Step 1의 원리와 매우 유사하게 작동하지만, 대상이 특허가 아닌 **Compustat 전역 데이터베이스**입니다. 특허 딕셔너리 구축을 완료한 M&A 기업명 중 고유 단어를 추출해 Compustat 법인명(conm 열)과 퍼지 매칭 및 AI 비교 검토를 진행합니다.
- **결과 (Output)**: Compustat 자동 통과 명단 및 관리자 검토 명단 엑셀 파일. 

### Step 4: Compustat 슈퍼 딕셔너리 구축 (Compustat Master Dictionary Builder)
- **파일 (File)**: `step4_compustat_dictionary_kr.py`
- **원리 (Principle)**: 
  Step 3에서 생성된 파일들을 모아 다시 한 번 매핑 사전을 만듭니다. 이번에는 **M&A 기업명 -> Compustat 법인명**을 매핑하는 딕셔너리를 구축합니다.
- **결과 (Output)**: Compustat 전용 마스터 딕셔너리 (`.pkl` 및 `.xlsx`).

### Step 5: 최종 데이터 집계 엔진 (Final Data Aggregation Engine)
- **파일 (File)**: `step5_data_aggregation_kr.py`
- **원리 (Principle)**: 
  이전에 구축된 총 4개의 빅 딕셔너리(Acquiror/Target 각각 특허용과 Compustat용 쌍)와 수동 결합 템플릿(M&A 원본 표)을 하나로 융합합니다. 
  1. Compustat 딕셔너리를 통해 고유 3대 재무 ID(gvkey, cusip, cik)를 데이터프레임에 부착합니다.
  2. Patent 딕셔너리를 통해 매년 발생한 특허 수, 출원 발명자 수 등을 연도별 피벗 테이블 형태로 통계 내어 부착합니다.
- **결과 (Output)**: 특허 및 재무 정보가 완벽히 하나로 집계된 궁극의 결과 파일(`final_outcome_COMPLETE.xlsx`).

---

## 3. 사용 가이드 (User Guide / 使用说明)

### 3.1 환경 준비 (Prerequisites / 环境准备)
- 데이터 처리에 필요한 파이썬 라이브러리를 설치합니다 (请确保已安装必要的Python库):
  ```bash
  pip install pandas numpy rapidfuzz tqdm google-genai openpyxl
  ```
- **중요(Important)**: Step 1과 Step 3 소스코드 내의 `API_KEY` 부분에 본인의 Gemini API Key를 올바르게 입력(연결)해야 합니다.

### 3.2 테스트 모드 (Test Mode / 测试模式)
파이프라인의 각 파일 상단에는 프로세스 속도를 위한 `TESTING_ROWS` 파라미터가 있습니다.
```python
# 코드 내 설정 예시 (代码内设置示例):
TESTING_ROWS = 500  # 500개의 행만 스캔
```
- 전체(수백만 데이터)를 실행하려면 반드시 `TESTING_ROWS = None` 으로 코드를 수정하고 저장한 뒤 실행하세요. (若要跑全量百万数据，请务必将其改为 `None`！)

### 3.3 실행 순서 (Execution Order / 执行顺序)
터미널(Terminal) 환경에서 다음의 순서대로 스크립트를 하나씩 실행하세요. 
각 단계별 진행 과정과 각종 통계 및 경고 로그는 콘솔 및 `pipeline_outputs/logs` 폴더에 상세히 기록 및 백업됩니다.

1. **Step 1**: 특허 데이터 정제 및 AI 매칭 (清洗及AI匹配第一阶段)
   `python step1_patent_matching_kr.py`
2. **Step 2**: 특허 딕셔너리 생성 (专利字典生成)
   `python step2_patent_dictionary_kr.py`
3. **Step 3**: Compustat 데이터 정제 및 AI 매칭 (Compustat匹配阶段)
   `python step3_compustat_matching_kr.py`
4. **Step 4**: Compustat 딕셔너리 생성 (Compustat字典生成)
   `python step4_compustat_dictionary_kr.py`
5. **Step 5**: 데이터 취합 및 최종 파일 생성 (数据大一统及报表生成)
   `python step5_data_aggregation_kr.py`

### 3.4 인간 검토(Manual Review) 권장 사항 (人工审核建议)
파이프라인이 생성한 `Final_Manual_Review_...` 엑셀 파일을 작업자가 텍스트 에디터나 엑셀로 단 한 번 직접 훑어보고, **잘못된 매칭 이름인 경우 해당 행을 과감하게 삭제하거나, `Match_Type` 열을 `MISMATCH`로 수정**하세요. (오류 항목 제외)

오직 해당 변경을 완료한 이후, 삭제 처리한 파일 그대로 다음 딕셔너리 생성 스크립트(Step 2 또는 Step 4)를 실행하시면 완전히 깨끗하고 결점 없는 양질의 최종 데이터를 획득하실 수 있습니다.
