# 📊 투자 대시보드 - 자동 업데이트

매일 장 마감 후 자동으로 데이터를 수집하고 대시보드를 업데이트합니다.

## 🚀 설정 방법 (처음 1번만)

### 1단계: 이 저장소를 GitHub에 업로드

1. GitHub에서 **New repository** 클릭
2. Repository name: `investment-dashboard` 입력
3. **Public** 선택
4. **Create repository** 클릭
5. 이 폴더의 모든 파일을 드래그앤드롭으로 업로드
6. **Commit changes** 클릭

### 2단계: API 키 등록 (보안)

1. 저장소 페이지에서 **Settings** 탭 클릭
2. 왼쪽 메뉴에서 **Secrets and variables** → **Actions** 클릭
3. **New repository secret** 버튼으로 아래 3개 등록:

| Name | Value |
|------|-------|
| `KIS_APP_KEY` | 한국투자증권 앱키 |
| `KIS_APP_SECRET` | 한국투자증권 시크릿키 |
| `KRX_API_KEY` | KRX Open API 인증키 |

### 3단계: GitHub Pages 켜기

1. **Settings** → **Pages**
2. Source: **Deploy from a branch**
3. Branch: **main** / **(root)** 선택
4. **Save** 클릭
5. 몇 분 후 `https://내아이디.github.io/investment-dashboard/` 로 접속 가능!

### 4단계: 첫 데이터 수집 실행

1. **Actions** 탭 클릭
2. 왼쪽에서 **매일 데이터 수집 & 배포** 클릭
3. **Run workflow** 버튼 클릭 → **Run workflow**
4. 초록색 체크 나올 때까지 대기 (약 3-5분)
5. 대시보드 새로고침 → 데이터 확인!

## 📅 이후에는?

- **아무것도 안 해도 됩니다!**
- 매일 평일 18:35(한국시간)에 자동으로 데이터 수집
- 대시보드에 접속하면 항상 최신 데이터

## 🔧 수동으로 즉시 업데이트하고 싶을 때

1. GitHub → **Actions** 탭
2. **매일 데이터 수집 & 배포** 클릭
3. **Run workflow** → **Run workflow**

## ❓ 문제가 생기면

- Actions 탭에서 빨간색 X가 있으면 클릭해서 에러 내용 확인
- 대부분 API 키가 잘못 입력된 경우 → Secrets에서 확인
