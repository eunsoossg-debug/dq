import sys
import os
import re
import json
import platform
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.font_manager as fm

from matplotlib.backends.backend_qt5agg import FigureCanvasQTAgg as FigureCanvas
from matplotlib.figure import Figure

from PyQt5.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout, QPushButton, QLabel,
    QFileDialog, QTableWidget, QTableWidgetItem, QHeaderView, QFrame, QMessageBox,
    QToolTip, QProgressBar, QDialog, QDialogButtonBox, QGroupBox, QListWidget,
    QListWidgetItem, QComboBox, QFormLayout, QSpinBox, QLineEdit, QCheckBox
)
from PyQt5.QtCore import Qt, QThread, pyqtSignal
from PyQt5.QtGui import QFont, QFontDatabase

# ==========================================
# 0. 폰트 설정
# ==========================================
def get_font_settings():
    system_name = platform.system()
    if system_name == 'Linux':
        path = '/usr/share/fonts/truetype/nanum/NanumGothic.ttf'
        return (path, 'NanumGothic') if os.path.exists(path) else (None, 'sans-serif')
    elif system_name == 'Windows':
        return 'c:/Windows/Fonts/malgun.ttf', 'Malgun Gothic'
    else:
        return '/System/Library/Fonts/AppleGothic.ttf', 'AppleGothic'

FONT_PATH, FONT_NAME = get_font_settings()
plt.rcParams['axes.unicode_minus'] = False

# ==========================================
# 0-1. 유틸: 결측 정규화
# ==========================================
_MISSING_LIKE = {"", " ", "  ", "nan", "NaN", "NULL", "null", "N/A", "n/a", "NA", "na", "-", "--"}

def normalize_missing(df: pd.DataFrame) -> pd.DataFrame:
    # 문자열 컬럼의 공백/특정 토큰을 NaN으로 통일
    df2 = df.copy()
    for col in df2.columns:
        if df2[col].dtype == "object":
            s = df2[col].astype(str)
            s = s.str.strip()
            df2[col] = s.replace(list(_MISSING_LIKE), np.nan)
    return df2

# ==========================================
# 0-2. 룰 파일 로드 (선택)
# 규칙 파일 예시(선택):
# {
#   "pk": ["user_id"],
#   "required": ["user_id","email","created_at"],
#   "types": {"created_at":"datetime","age":"int"},
#   "allowed_values": {"status":["A","I","D"]},
#   "formats": {"email":"email","phone":"phone"},
#   "timestamp_column": "created_at",
#   "freshness_days": 30
# }
# ==========================================
def load_rules_if_exists(data_path: str) -> dict:
    base = os.path.splitext(data_path)[0]
    candidates = [
        base + ".rules.json",
        base + ".dq.json",
        os.path.join(os.path.dirname(data_path), "rules.json"),
        os.path.join(os.path.dirname(data_path), "dq_rules.json"),
    ]
    for p in candidates:
        if os.path.exists(p):
            try:
                with open(p, "r", encoding="utf-8") as f:
                    return json.load(f)
            except Exception:
                pass
    return {}

# ==========================================
# 0-3. 룰 설정 다이얼로그 (원래 틀 유지 + PK/필수컬럼만 최소로 명시)
# ==========================================
class RulesDialog(QDialog):
    def __init__(self, columns, loaded_rules=None, parent=None):
        super().__init__(parent)
        self.setWindowTitle("인증 규칙(최소 필수) 설정")
        self.setModal(True)
        self.resize(720, 520)

        self.columns = list(map(str, columns))
        self.rules = loaded_rules.copy() if loaded_rules else {}

        layout = QVBoxLayout(self)

        info = QLabel(
            "사내 '필수항목 인증' 기준으로 PK/필수컬럼/최신성만 최소 설정하세요.\n"
            "- PK 중복/NULL은 FAIL\n"
            "- 필수컬럼 결측(공백 포함)은 FAIL(또는 허용치 설정 시 Conditional)\n"
            "- 최신성은 선택(타임스탬프 컬럼 + 허용일수)"
        )
        info.setStyleSheet("color:#334155;")
        layout.addWidget(info)

        # PK 선택
        gb_pk = QGroupBox("1) PK(유일키) 선택")
        pk_form = QFormLayout(gb_pk)
        self.pk_combo = QComboBox()
        self.pk_combo.addItems(self.columns)
        # 룰에 pk가 있으면 첫 번째 사용
        if self.rules.get("pk"):
            pk = self.rules["pk"][0] if isinstance(self.rules["pk"], list) else self.rules["pk"]
            if pk in self.columns:
                self.pk_combo.setCurrentText(pk)
        pk_form.addRow("PK 컬럼:", self.pk_combo)
        layout.addWidget(gb_pk)

        # 필수 컬럼 선택
        gb_req = QGroupBox("2) 필수 컬럼(Null/공백 허용 안 함)")
        v_req = QVBoxLayout(gb_req)
        self.req_list = QListWidget()
        self.req_list.setSelectionMode(QListWidget.NoSelection)
        required_set = set(self.rules.get("required", []))
        for c in self.columns:
            item = QListWidgetItem(c)
            item.setFlags(item.flags() | Qt.ItemIsUserCheckable)
            item.setCheckState(Qt.Checked if c in required_set else Qt.Unchecked)
            self.req_list.addItem(item)
        v_req.addWidget(self.req_list)

        req_hint = QHBoxLayout()
        self.allow_missing_checkbox = QCheckBox("필수컬럼 결측 허용치 사용(%)")
        self.allow_missing_checkbox.setChecked(bool(self.rules.get("required_missing_threshold_pct", 0)))
        self.miss_threshold = QSpinBox()
        self.miss_threshold.setRange(0, 100)
        self.miss_threshold.setValue(int(self.rules.get("required_missing_threshold_pct", 0)))
        self.miss_threshold.setEnabled(self.allow_missing_checkbox.isChecked())
        self.allow_missing_checkbox.stateChanged.connect(lambda s: self.miss_threshold.setEnabled(bool(s)))
        req_hint.addWidget(self.allow_missing_checkbox)
        req_hint.addWidget(self.miss_threshold)
        req_hint.addStretch(1)
        v_req.addLayout(req_hint)

        layout.addWidget(gb_req)

        # 최신성(선택)
        gb_time = QGroupBox("3) 최신성(선택)")
        time_form = QFormLayout(gb_time)
        self.ts_combo = QComboBox()
        self.ts_combo.addItem("(사용 안 함)")
        self.ts_combo.addItems(self.columns)
        ts_col = self.rules.get("timestamp_column", "")
        if ts_col in self.columns:
            self.ts_combo.setCurrentText(ts_col)
        time_form.addRow("타임스탬프 컬럼:", self.ts_combo)

        self.fresh_days = QSpinBox()
        self.fresh_days.setRange(1, 3650)
        self.fresh_days.setValue(int(self.rules.get("freshness_days", 30)))
        time_form.addRow("허용 최신성(일):", self.fresh_days)
        layout.addWidget(gb_time)

        # 형식 검사(선택) - 이메일/전화 정도만 (원래 코드의 구문 유효성 유지하되 '필수항목' 아닌 참고)
        gb_fmt = QGroupBox("4) 형식 검사(선택/참고용)")
        fmt_form = QFormLayout(gb_fmt)
        self.email_col = QComboBox()
        self.email_col.addItem("(없음)")
        self.email_col.addItems(self.columns)
        self.phone_col = QComboBox()
        self.phone_col.addItem("(없음)")
        self.phone_col.addItems(self.columns)

        loaded_formats = self.rules.get("formats", {})
        # 자동 세팅 시도: 룰에 있거나, 컬럼명 힌트
        def pick_by_hint(hints):
            for c in self.columns:
                lc = c.lower()
                if any(h in lc for h in hints):
                    return c
            return None

        em = loaded_formats.get("email") or pick_by_hint(["email", "이메일", "mail"])
        ph = loaded_formats.get("phone") or pick_by_hint(["phone", "전화", "tel", "mobile", "휴대"])
        if em in self.columns:
            self.email_col.setCurrentText(em)
        if ph in self.columns:
            self.phone_col.setCurrentText(ph)

        fmt_form.addRow("이메일 컬럼:", self.email_col)
        fmt_form.addRow("전화번호 컬럼:", self.phone_col)
        layout.addWidget(gb_fmt)

        # 버튼
        buttons = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        buttons.accepted.connect(self.accept)
        buttons.rejected.connect(self.reject)
        layout.addWidget(buttons)

    def get_rules(self) -> dict:
        pk = self.pk_combo.currentText()

        required = []
        for i in range(self.req_list.count()):
            item = self.req_list.item(i)
            if item.checkState() == Qt.Checked:
                required.append(item.text())

        rules = {}
        rules["pk"] = [pk] if pk else []
        rules["required"] = required

        if self.allow_missing_checkbox.isChecked():
            rules["required_missing_threshold_pct"] = int(self.miss_threshold.value())
        else:
            rules["required_missing_threshold_pct"] = 0

        ts = self.ts_combo.currentText()
        if ts != "(사용 안 함)":
            rules["timestamp_column"] = ts
            rules["freshness_days"] = int(self.fresh_days.value())

        # 선택 형식
        formats = {}
        em = self.email_col.currentText()
        ph = self.phone_col.currentText()
        if em != "(없음)":
            formats["email"] = em
        if ph != "(없음)":
            formats["phone"] = ph
        if formats:
            rules["formats"] = formats

        return rules

# ==========================================
# 1. 필수항목 인증 워커 (원래 구조 유지하되 지표/판정 개선)
# ==========================================
class AnalysisWorker(QThread):
    finished_signal = pyqtSignal(dict)
    error_signal = pyqtSignal(str)

    def __init__(self, filepath, rules: dict):
        super().__init__()
        self.filepath = filepath
        self.rules = rules or {}

    def run(self):
        try:
            df = None

            # ---- 파일 로드 (인코딩 방어)
            if self.filepath.endswith('.csv'):
                encodings = ['utf-8', 'utf-8-sig', 'cp949', 'euc-kr', 'utf-16', 'latin-1']
                for enc in encodings:
                    try:
                        df = pd.read_csv(self.filepath, encoding=enc, low_memory=False)
                        break
                    except UnicodeDecodeError:
                        continue
                    except Exception:
                        continue
                if df is None:
                    df = pd.read_csv(self.filepath, encoding='utf-8', errors='ignore', low_memory=False)
            else:
                df = pd.read_excel(self.filepath)

            if df is None:
                self.error_signal.emit("파일 형식을 인식할 수 없습니다.")
                return

            if len(df) == 0:
                self.error_signal.emit("데이터가 없습니다.")
                return

            df = normalize_missing(df)

            total_rows = len(df)
            total_cells = df.size

            # ---- 규칙 준비
            pk_cols = self.rules.get("pk", [])
            if isinstance(pk_cols, str):
                pk_cols = [pk_cols]
            pk_col = pk_cols[0] if pk_cols else None

            required_cols = self.rules.get("required", [])
            if isinstance(required_cols, str):
                required_cols = [required_cols]

            required_missing_threshold_pct = float(self.rules.get("required_missing_threshold_pct", 0))

            timestamp_col = self.rules.get("timestamp_column", None)
            freshness_days = int(self.rules.get("freshness_days", 0)) if timestamp_col else 0

            formats = self.rules.get("formats", {}) or {}

            # ---- (0) 스키마 적합성: required/PK 컬럼 존재 여부
            schema_missing = []
            for c in set(required_cols + ([pk_col] if pk_col else []) + ([timestamp_col] if timestamp_col else [])):
                if c and c not in df.columns:
                    schema_missing.append(c)

            score_schema = 100.0 if not schema_missing else 0.0
            hard_fail_schema = bool(schema_missing)

            # ---- (1) 완전성(값): 전체 결측
            missing_cells = df.isnull().sum().sum()
            score_val_comp = (1 - (missing_cells / total_cells)) * 100

            # ---- (2) 완전성(필수컬럼): 필수컬럼 결측률 (인증 핵심)
            hard_fail_required = False
            req_missing_pct = 0.0
            req_missing_rows_idx = set()
            if required_cols:
                # 필수 컬럼 중 하나라도 NaN이면 해당 행을 오류로 잡음
                req_missing_mask = df[required_cols].isnull().any(axis=1)
                req_missing_rows = int(req_missing_mask.sum())
                req_missing_pct = (req_missing_rows / total_rows) * 100
                req_missing_rows_idx = set(df.index[req_missing_mask].tolist())

                # 점수는 100 - 결측행비율
                score_req_comp = max(0.0, 100.0 - req_missing_pct)

                # Hard fail: 허용치 초과
                if req_missing_pct > required_missing_threshold_pct:
                    hard_fail_required = True
            else:
                score_req_comp = 100.0

            # ---- (3) PK 무결성(유일성 + NULL)
            hard_fail_pk = False
            pk_dup_cnt = 0
            pk_null_cnt = 0
            pk_issue_idx = set()
            if pk_col and pk_col in df.columns:
                pk_null_mask = df[pk_col].isnull()
                pk_null_cnt = int(pk_null_mask.sum())
                if pk_null_cnt > 0:
                    pk_issue_idx |= set(df.index[pk_null_mask].tolist())

                pk_dup_mask = df[pk_col].duplicated(keep=False) & (~df[pk_col].isnull())
                pk_dup_cnt = int(pk_dup_mask.sum())
                if pk_dup_cnt > 0:
                    pk_issue_idx |= set(df.index[pk_dup_mask].tolist())

                # 유일성 점수: 중복/NULL 행 비율 기반
                pk_bad = len(pk_issue_idx)
                score_pk = max(0.0, 100.0 - (pk_bad / total_rows) * 100)

                # Hard fail: PK NULL 또는 중복 존재
                if pk_null_cnt > 0 or pk_dup_cnt > 0:
                    hard_fail_pk = True
            else:
                # PK가 지정되지 않으면 인증 관점에서 FAIL로는 두지 않되 점수는 낮게(운영 경고)
                score_pk = 70.0

            # ---- (4) 형식 유효성(참고): 이메일/전화 (원래 코드 유지하되 "인증 필수 아님")
            fmt_issue_idx = set()
            fmt_scores = []
            # email
            email_col = formats.get("email", None)
            if email_col and email_col in df.columns:
                pattern = r'^[\w\.-]+@[\w\.-]+\.\w+$'
                s = df[email_col].astype(str).fillna("")
                # 결측은 이 지표에선 제외(필수는 완전성에서 걸림)
                non_empty = s.str.strip() != ""
                valid = s.str.match(pattern) & non_empty
                valid_cnt = int(valid.sum())
                denom = int(non_empty.sum()) if int(non_empty.sum()) > 0 else 0
                fmt_scores.append((valid_cnt / denom) if denom else 1.0)
                fmt_issue_idx |= set(df.index[non_empty & (~valid)].tolist())

            # phone
            phone_col = formats.get("phone", None)
            if phone_col and phone_col in df.columns:
                pattern = r'^\d{2,3}[- .]?\d{3,4}[- .]?\d{4}$'
                s = df[phone_col].astype(str).fillna("")
                non_empty = s.str.strip() != ""
                valid = s.str.match(pattern) & non_empty
                valid_cnt = int(valid.sum())
                denom = int(non_empty.sum()) if int(non_empty.sum()) > 0 else 0
                fmt_scores.append((valid_cnt / denom) if denom else 1.0)
                fmt_issue_idx |= set(df.index[non_empty & (~valid)].tolist())

            score_format = (np.mean(fmt_scores) * 100) if fmt_scores else 100.0

            # ---- (5) 최신성(선택): timestamp_col 최대값이 기준일 이내인지
            hard_fail_fresh = False
            score_fresh = 100.0
            fresh_issue_idx = set()
            freshness_msg = ""
            if timestamp_col and timestamp_col in df.columns and freshness_days > 0:
                # datetime 변환 실패는 NaT로 처리
                ts = pd.to_datetime(df[timestamp_col], errors="coerce")
                max_ts = ts.max()
                if pd.isna(max_ts):
                    # 타임스탬프가 전부 깨짐 → 운영상 FAIL에 준함 (선택항목이지만, 설정했으면 책임)
                    score_fresh = 0.0
                    hard_fail_fresh = True
                    freshness_msg = f"{timestamp_col}를 날짜로 해석할 수 없습니다."
                    fresh_issue_idx |= set(df.index[ts.isna()].tolist())
                else:
                    now = pd.Timestamp.now()
                    age_days = (now - max_ts).days
                    # 점수는 0~100, 기준일 넘어가면 선형 감점(참고용)
                    if age_days <= freshness_days:
                        score_fresh = 100.0
                    else:
                        score_fresh = max(0.0, 100.0 - min(100.0, (age_days - freshness_days) * 5.0))
                        hard_fail_fresh = True
                        freshness_msg = f"최신 데이터({max_ts.date()})가 허용({freshness_days}일) 초과: {age_days}일 경과"

            # ---- 오류 프리뷰: 실제 위반 행만 모아서 보여주기
            issue_idx = set()
            issue_idx |= req_missing_rows_idx
            issue_idx |= pk_issue_idx
            issue_idx |= fmt_issue_idx
            issue_idx |= fresh_issue_idx

            # 스키마 미스는 행으로 잡을 수 없으니 별도 메시지로만
            if issue_idx:
                preview_df = df.loc[sorted(list(issue_idx))].head(50)
            else:
                preview_df = df.head(50)

            # ---- 지표 구성 (원래 "7대" 틀 유지: 의미/범위/관계 같은 논쟁성 지표 제거하고, 인증 필수/운영 보조로 재정의)
            metrics_labels = [
                "스키마 적합성",   # 필수
                "값 완전성",       # 참고(전체 결측)
                "필수 완전성",     # 필수
                "PK 무결성",       # 필수
                "형식 유효성",     # 참고
                "최신성",          # 선택/운영
                "참고(예비)"       # 틀 유지용(항상 100)
            ]
            metrics_scores = [
                score_schema,
                score_val_comp,
                score_req_comp,
                score_pk,
                score_format,
                score_fresh,
                100.0
            ]

            # ---- 인증 판정 로직 (Hard fail 중심)
            hard_fail = hard_fail_schema or hard_fail_required or hard_fail_pk or hard_fail_fresh

            # Conditional: hard fail은 아니지만 경미한 이슈(예: 형식 유효성 낮음, 전체 결측 높음)
            conditional = False
            if not hard_fail:
                if score_format < 95 or score_val_comp < 95:
                    conditional = True

            if hard_fail:
                grade = "FAIL"
            elif conditional:
                grade = "CONDITIONAL PASS"
            else:
                grade = "PASS"

            # 종합점수는 참고치로만 제공(운영/대시보드용)
            total_score = float(np.mean(metrics_scores))

            # 추가 설명 메시지
            notes = []
            if schema_missing:
                notes.append(f"스키마 누락: {', '.join(schema_missing)}")
            if required_cols:
                notes.append(f"필수 결측률: {req_missing_pct:.2f}% (허용치 {required_missing_threshold_pct:.0f}%)")
            if pk_col:
                notes.append(f"PK({pk_col}) NULL={pk_null_cnt}, 중복행수={pk_dup_cnt}")
            else:
                notes.append("PK 미지정: 점수만 감점(권장: PK 지정)")
            if freshness_msg:
                notes.append(f"최신성: {freshness_msg}")

            result = {
                "grade": grade,
                "score": round(total_score, 2),
                "metrics_labels": metrics_labels,
                "metrics_scores": metrics_scores,
                "row_count": total_rows,
                "preview": preview_df,
                "notes": "\n".join(notes) if notes else ""
            }

            self.finished_signal.emit(result)

        except Exception as e:
            self.error_signal.emit(f"분석 중 오류 발생:\n{str(e)}")

# ==========================================
# 2. GUI 클래스 (원래 틀 유지 + 규칙 설정 단계만 추가)
# ==========================================
class DQApp(QMainWindow):
    def __init__(self):
        super().__init__()
        if FONT_PATH and os.path.exists(FONT_PATH):
            QFontDatabase.addApplicationFont(FONT_PATH)
        self.setWindowTitle("DQ Pro - 필수항목 인증(정형데이터) v2.1")
        self.setGeometry(100, 100, 1300, 900)
        self.setStyleSheet(f"""
            QMainWindow {{ background-color: #f3f4f6; }}
            QLabel {{ font-family: '{FONT_NAME}', sans-serif; }}
            QPushButton {{ font-family: '{FONT_NAME}', sans-serif; }}
            QToolTip {{ background-color: #1e293b; color: #f8fafc; border: 1px solid #334155; font-family: '{FONT_NAME}'; }}
            QProgressBar {{ border: 2px solid #e2e8f0; border-radius: 5px; text-align: center; }}
            QProgressBar::chunk {{ background-color: #3b82f6; width: 10px; }}
        """)
        self.init_ui()

    def init_ui(self):
        main_widget = QWidget()
        self.setCentralWidget(main_widget)
        main_layout = QHBoxLayout(main_widget)
        main_layout.setContentsMargins(0, 0, 0, 0)
        main_layout.setSpacing(0)

        # 사이드바
        sidebar = QFrame()
        sidebar.setFixedWidth(270)
        sidebar.setStyleSheet("background-color: #0f172a; border-right: 1px solid #334155;")
        side_layout = QVBoxLayout(sidebar)
        side_layout.setContentsMargins(30, 50, 30, 50)
        side_layout.setSpacing(20)

        title_lbl = QLabel("DQ CHECKER\nPRO v2.1")
        title_lbl.setStyleSheet("color: white; font-size: 28px; font-weight: 900; line-height: 1.2;")
        side_layout.addWidget(title_lbl)

        desc_lbl = QLabel("필수항목 기반\n정형데이터 인증")
        desc_lbl.setStyleSheet("color: #94a3b8; font-size: 14px;")
        side_layout.addWidget(desc_lbl)

        self.notes_lbl = QLabel("")
        self.notes_lbl.setWordWrap(True)
        self.notes_lbl.setStyleSheet("color: #cbd5e1; font-size: 12px;")
        side_layout.addWidget(self.notes_lbl)

        side_layout.addStretch(1)

        self.btn_upload = QPushButton("📂  데이터 인증 검사")
        self.btn_upload.setCursor(Qt.PointingHandCursor)
        self.btn_upload.setStyleSheet("""
            QPushButton {
                background-color: #6366f1; color: white; font-size: 16px;
                padding: 18px; border-radius: 12px; font-weight: bold; border: none;
            }
            QPushButton:hover { background-color: #4f46e5; }
            QPushButton:pressed { background-color: #4338ca; }
            QPushButton:disabled { background-color: #94a3b8; }
        """)
        self.btn_upload.clicked.connect(self.load_file)
        side_layout.addWidget(self.btn_upload)

        self.progress_bar = QProgressBar()
        self.progress_bar.setRange(0, 0)
        self.progress_bar.setStyleSheet("QProgressBar { height: 8px; border: none; background: #334155; } QProgressBar::chunk { background: #6366f1; }")
        self.progress_bar.setVisible(False)
        side_layout.addWidget(self.progress_bar)

        side_layout.addStretch(4)
        main_layout.addWidget(sidebar)

        # 메인 콘텐츠
        content_widget = QWidget()
        content_layout = QVBoxLayout(content_widget)
        content_layout.setContentsMargins(40, 40, 40, 40)
        content_layout.setSpacing(30)

        # 상단 카드
        score_layout = QHBoxLayout()
        score_layout.setSpacing(20)

        self.card_grade = self.create_card("인증 판정", "-", "#8b5cf6", "Hard Fail(치명 결함) 중심으로 PASS/FAIL/CONDITIONAL PASS를 판정합니다.")
        self.card_score = self.create_card("참고 점수", "0", "#10b981", "대시보드용 참고 점수(평균). 인증 판정은 별도 규칙으로 결정됩니다.")
        self.card_rows = self.create_card("검사 데이터 수", "0", "#3b82f6", "검사 완료된 총 레코드 개수입니다.")

        score_layout.addWidget(self.card_grade)
        score_layout.addWidget(self.card_score)
        score_layout.addWidget(self.card_rows)
        content_layout.addLayout(score_layout)

        # 하단 (차트 + 테이블)
        bottom_layout = QHBoxLayout()
        bottom_layout.setSpacing(20)

        # 차트 영역
        chart_frame = QFrame()
        chart_frame.setStyleSheet("background-color: white; border-radius: 16px; border: 1px solid #e2e8f0;")
        chart_layout = QVBoxLayout(chart_frame)

        chart_header = QWidget()
        h_layout = QHBoxLayout(chart_header)
        chart_title = QLabel("품질 지표 분석 (Radar Chart)")
        chart_title.setStyleSheet("font-size: 16px; font-weight: bold; color: #334155;")
        h_layout.addWidget(chart_title)
        chart_layout.addWidget(chart_header)

        self.figure = Figure(figsize=(6, 6), dpi=100)
        self.figure.patch.set_facecolor('white')
        self.canvas = FigureCanvas(self.figure)
        chart_layout.addWidget(self.canvas)
        bottom_layout.addWidget(chart_frame, stretch=5)

        # 테이블 영역
        table_frame = QFrame()
        table_frame.setStyleSheet("background-color: white; border-radius: 16px; border: 1px solid #e2e8f0;")
        table_layout = QVBoxLayout(table_frame)

        table_header = QLabel("오류 데이터 미리보기(위반 행 우선)")
        table_header.setStyleSheet("font-size: 16px; font-weight: bold; color: #334155; padding-bottom: 5px;")
        table_layout.addWidget(table_header)

        self.table = QTableWidget()
        self.table.setStyleSheet("QTableWidget { border: none; gridline-color: #f1f5f9; }")
        table_layout.addWidget(self.table)
        bottom_layout.addWidget(table_frame, stretch=5)

        content_layout.addLayout(bottom_layout, stretch=1)
        main_layout.addWidget(content_widget)

    def create_card(self, title, value, color, description):
        frame = QFrame()
        frame.setToolTip(description)
        try:
            frame.setCursor(Qt.WhatsThisCursor)
        except Exception:
            frame.setCursor(Qt.PointingHandCursor)
        frame.setStyleSheet(f"""
            QFrame {{ background-color: white; border-radius: 16px; border: 1px solid #e2e8f0; border-left: 6px solid {color}; }}
            QFrame:hover {{ border: 1px solid {color}; background-color: #f8fafc; }}
        """)
        layout = QVBoxLayout(frame)
        layout.setContentsMargins(25, 25, 25, 25)

        lbl_title = QLabel(title)
        lbl_title.setStyleSheet("color: #64748b; font-size: 14px; font-weight: bold; border: none; background: transparent;")
        lbl_val = QLabel(value)
        lbl_val.setStyleSheet("color: #1e293b; font-size: 32px; font-weight: 800; border: none; margin-top: 5px; background: transparent;")
        layout.addWidget(lbl_title)
        layout.addWidget(lbl_val)
        return frame

    def update_card_value(self, card_frame, value):
        labels = card_frame.findChildren(QLabel)
        for lbl in labels:
            if "32px" in lbl.styleSheet():
                lbl.setText(str(value))
                break

    def load_file(self):
        fname, _ = QFileDialog.getOpenFileName(self, '데이터 파일 선택', '', 'Data Files (*.csv *.xlsx *.xls)')
        if not fname:
            return

        # 1) 룰 자동 로드 시도 + 컬럼 추출(다이얼로그 채우기용)
        try:
            # 빠른 컬럼 읽기: csv는 nrows=5, excel은 head
            if fname.endswith(".csv"):
                # 인코딩은 여기서 너무 집착하지 않고, 워커에서 최종 처리
                try:
                    tmp = pd.read_csv(fname, nrows=5, encoding="utf-8", low_memory=False)
                except Exception:
                    tmp = pd.read_csv(fname, nrows=5, encoding="cp949", low_memory=False, errors="ignore")
            else:
                tmp = pd.read_excel(fname, nrows=5)

            cols = tmp.columns
        except Exception as e:
            QMessageBox.critical(self, "오류", f"컬럼 정보를 읽을 수 없습니다:\n{e}")
            return

        loaded_rules = load_rules_if_exists(fname)

        dlg = RulesDialog(cols, loaded_rules=loaded_rules, parent=self)
        if dlg.exec_() != QDialog.Accepted:
            return
        rules = dlg.get_rules()

        # 2) 워커 실행
        self.btn_upload.setText("인증 분석 중... (대기)")
        self.btn_upload.setEnabled(False)
        self.progress_bar.setVisible(True)
        self.notes_lbl.setText("")

        self.worker = AnalysisWorker(fname, rules)
        self.worker.finished_signal.connect(self.on_analysis_finished)
        self.worker.error_signal.connect(self.on_analysis_error)
        self.worker.start()

    def on_analysis_finished(self, result):
        self.update_ui(result)
        self.reset_ui_state()

        msg = "필수항목 인증 분석이 완료되었습니다."
        if result.get("grade") == "FAIL":
            QMessageBox.warning(self, "분석 완료 (FAIL)", msg)
        elif result.get("grade") == "CONDITIONAL PASS":
            QMessageBox.information(self, "분석 완료 (CONDITIONAL PASS)", msg)
        else:
            QMessageBox.information(self, "분석 완료 (PASS)", msg)

    def on_analysis_error(self, err_msg):
        self.reset_ui_state()
        QMessageBox.critical(self, "오류", err_msg)

    def reset_ui_state(self):
        self.btn_upload.setText("📂  데이터 인증 검사")
        self.btn_upload.setEnabled(True)
        self.progress_bar.setVisible(False)

    def update_ui(self, result):
        self.update_card_value(self.card_grade, result['grade'])
        self.update_card_value(self.card_score, f"{result['score']}")
        self.update_card_value(self.card_rows, f"{result['row_count']:,}")
        self.notes_lbl.setText(result.get("notes", ""))

        # Radar
        self.figure.clear()
        ax = self.figure.add_subplot(111, polar=True)
        ax.set_facecolor('#f8fafc')

        labels = list(result['metrics_labels'])
        values = list(result['metrics_scores'])

        angles = np.linspace(0, 2 * np.pi, len(labels), endpoint=False).tolist()
        vals = values + values[:1]
        angs = angles + angles[:1]

        ax.plot(angs, vals, color='#6366f1', linewidth=2, linestyle='solid', marker='o')
        ax.fill(angs, vals, color='#6366f1', alpha=0.2)

        ax.set_xticks(angles)
        if FONT_PATH and os.path.exists(FONT_PATH):
            font_prop = fm.FontProperties(fname=FONT_PATH, size=9, weight='bold')
            ax.set_xticklabels(labels, fontproperties=font_prop, color='#334155')
        else:
            ax.set_xticklabels(labels, color='#334155')

        ax.set_rlabel_position(0)
        plt.yticks([20, 40, 60, 80, 100], ["20", "40", "60", "80", "100"], color="grey", size=7)
        plt.ylim(0, 100)

        ax.spines['polar'].set_visible(False)
        ax.grid(color='#e2e8f0', linestyle='--')

        self.canvas.draw()

        # Table
        df = result['preview']
        self.table.setRowCount(df.shape[0])
        self.table.setColumnCount(df.shape[1])
        self.table.setHorizontalHeaderLabels(df.columns.astype(str))
        for i in range(df.shape[0]):
            for j in range(df.shape[1]):
                val = df.iloc[i, j]
                self.table.setItem(i, j, QTableWidgetItem("" if pd.isna(val) else str(val)))
        self.table.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)

if __name__ == '__main__':
    app = QApplication(sys.argv)
    if FONT_PATH and os.path.exists(FONT_PATH):
        font_id = QFontDatabase.addApplicationFont(FONT_PATH)
        if font_id != -1:
            family = QFontDatabase.applicationFontFamilies(font_id)[0]
            app.setFont(QFont(family, 10))
        else:
            app.setFont(QFont('sans-serif', 10))
    else:
        app.setFont(QFont('sans-serif', 10))

    ex = DQApp()
    ex.show()
    sys.exit(app.exec_())
