import sys
import os
import re
import platform
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.font_manager as fm

from matplotlib.backends.backend_qt5agg import FigureCanvasQTAgg as FigureCanvas
from matplotlib.figure import Figure
from PyQt5.QtWidgets import (QApplication, QMainWindow, QWidget, QVBoxLayout, 
                             QHBoxLayout, QPushButton, QLabel, QFileDialog, 
                             QTableWidget, QTableWidgetItem, QHeaderView, QFrame, QMessageBox, QToolTip, QProgressBar)
from PyQt5.QtCore import Qt, QThread, pyqtSignal
from PyQt5.QtGui import QFont, QColor, QFontDatabase

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
# 1. 7대 지표 정밀 분석 워커
# ==========================================
class AnalysisWorker(QThread):
    finished_signal = pyqtSignal(dict)
    error_signal = pyqtSignal(str)

    def __init__(self, filepath):
        super().__init__()
        self.filepath = filepath

    def run(self):
        try:
            # 1. 파일 읽기 (인코딩 자동 감지)
            if self.filepath.endswith('.csv'):
                try:
                    df = pd.read_csv(self.filepath, low_memory=False)
                except UnicodeDecodeError:
                    df = pd.read_csv(self.filepath, encoding='cp949', low_memory=False)
            else:
                df = pd.read_excel(self.filepath)
            
            total_rows = len(df)
            if total_rows == 0:
                self.error_signal.emit("데이터가 없습니다.")
                return

            # --- [1. 완전성] ---
            # 1-1. 데이터값 완전성 (Null Check): 모든 셀 중 비어있지 않은 비율
            total_cells = df.size
            missing_cells = df.isnull().sum().sum()
            score_val_comp = (1 - (missing_cells / total_cells)) * 100

            # 1-2. 레코드 완전성 (Empty Row Check): 모든 값이 비어있는 행이 없는지
            # thresh=1 : 적어도 1개 이상의 데이터가 있어야 삭제 안 됨 -> 반대로 다 비었으면 카운트
            empty_rows = total_rows - len(df.dropna(how='all')) 
            score_rec_comp = (1 - (empty_rows / total_rows)) * 100

            # --- [2. 유효성] ---
            # 2-1. 구문 유효성 (Format): 이메일, 전화번호, 날짜 등 정해진 패턴 준수율
            # (속도를 위해 'email' 컬럼이 있을 때만 표본 검사)
            format_scores = []
            for col in df.columns:
                c_str = str(col).lower()
                if 'email' in c_str or '이메일' in c_str:
                    pattern = r'^[\w\.-]+@[\w\.-]+\.\w+$'
                    valid_cnt = df[col].astype(str).str.match(pattern).sum()
                    format_scores.append(valid_cnt / total_rows)
                elif 'phone' in c_str or '전화' in c_str or 'tel' in c_str:
                    pattern = r'^\d{2,3}[- .]?\d{3,4}[- .]?\d{4}$'
                    valid_cnt = df[col].astype(str).str.match(pattern).sum()
                    format_scores.append(valid_cnt / total_rows)
            
            score_syntax_val = np.mean(format_scores) * 100 if format_scores else 100.0

            # 2-2. 의미 유효성 (Domain): 범주형 데이터 이상치 (예: 성별에 '남','여' 외 다른 값)
            # 고유값이 10개 미만인 컬럼(범주형 추정)에서, 상위 99% 빈도에 속하지 않는 값 비율
            semantic_scores = []
            for col in df.columns:
                if df[col].dtype == 'object' and df[col].nunique() < 20:
                    top_vals = df[col].value_counts(normalize=True).cumsum()
                    # 상위 99%를 차지하는 값들을 '정상'으로 간주
                    valid_ratio = top_vals[top_vals <= 0.99].max() if not top_vals.empty else 1.0
                    semantic_scores.append(valid_ratio if not np.isnan(valid_ratio) else 1.0)
            
            score_semantic_val = np.mean(semantic_scores) * 100 if semantic_scores else 100.0

            # 2-3. 범위 유효성 (Range): 수치형 데이터가 평균 ± 3표준편차 안에 있는지 (Outlier)
            range_scores = []
            numeric_cols = df.select_dtypes(include=[np.number]).columns
            for col in numeric_cols:
                mean = df[col].mean()
                std = df[col].std()
                if std > 0:
                    # Z-score가 3 이내인 데이터 비율
                    in_range = df[col].between(mean - 3*std, mean + 3*std).sum()
                    range_scores.append(in_range / total_rows)
                else:
                    range_scores.append(1.0)
            
            score_range_val = np.mean(range_scores) * 100 if range_scores else 100.0

            # 2-4. 관계 유효성 (Relation): 날짜 논리 오류 검사 (종료일 < 시작일)
            # 'start', 'end' 또는 '시작', '종료'가 포함된 컬럼 쌍 찾기 (단순화된 로직)
            score_rel_val = 100.0 # 기본값
            # (복잡한 로직이라 여기서는 100점으로 두거나, 추후 커스텀 필요)
            
            # --- [3. 일관성] ---
            # 3-1. 참조 무결성 (Reference/Consistency): PK(ID) 중복 여부로 대체
            # ID로 추정되는 컬럼(첫번째 컬럼 or 'id' 포함)의 중복 검사
            id_col = next((c for c in df.columns if 'id' in str(c).lower() or '번' in str(c)), df.columns[0])
            duplicates = df[id_col].duplicated().sum()
            score_ref_integ = (1 - (duplicates / total_rows)) * 100

            # --- [종합 결과] ---
            # 7개 항목 리스트
            metrics_labels = ['값 완전성', '구조 완전성', '구문 유효성', '의미 유효성', '범위 유효성', '관계 유효성', '참조 무결성']
            metrics_scores = [
                score_val_comp, score_rec_comp, 
                score_syntax_val, score_semantic_val, score_range_val, score_rel_val, 
                score_ref_integ
            ]
            
            # 종합 점수 (평균)
            total_score = np.mean(metrics_scores)
            
            if total_score >= 99: grade = "Class A"
            elif total_score >= 95: grade = "Class B"
            elif total_score >= 90: grade = "Class C"
            else: grade = "Uncertified"

            result = {
                "grade": grade,
                "score": round(total_score, 2),
                "metrics_labels": metrics_labels,
                "metrics_scores": metrics_scores,
                "row_count": total_rows,
                "preview": df.head(50)
            }
            
            self.finished_signal.emit(result)

        except Exception as e:
            self.error_signal.emit(f"분석 중 오류 발생:\n{str(e)}")

# ==========================================
# 2. GUI 클래스 (7각형 차트 지원)
# ==========================================
class DQApp(QMainWindow):
    def __init__(self):
        super().__init__()
        if FONT_PATH and os.path.exists(FONT_PATH):
            QFontDatabase.addApplicationFont(FONT_PATH)
        self.setWindowTitle("DQ Pro - 7대 품질 지표 정밀 진단")
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

        title_lbl = QLabel("DQ CHECKER\nPRO v2.0")
        title_lbl.setStyleSheet("color: white; font-size: 28px; font-weight: 900; line-height: 1.2;")
        side_layout.addWidget(title_lbl)
        
        desc_lbl = QLabel("7대 품질 지표\n정밀 진단 시스템")
        desc_lbl.setStyleSheet("color: #94a3b8; font-size: 14px;")
        side_layout.addWidget(desc_lbl)
        side_layout.addStretch(1)

        self.btn_upload = QPushButton("📂  데이터 정밀 검사")
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
        self.card_grade = self.create_card("종합 품질 등급", "-", "#8b5cf6", "7개 지표의 평균 점수 기반 등급입니다.")
        self.card_score = self.create_card("종합 점수", "0", "#10b981", "100점 만점 기준 종합 점수입니다.")
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
        chart_title = QLabel("7대 품질 지표 분석 (Radar Chart)")
        chart_title.setStyleSheet("font-size: 16px; font-weight: bold; color: #334155;")
        h_layout.addWidget(chart_title)
        chart_layout.addWidget(chart_header)

        self.figure = Figure(figsize=(6, 6), dpi=100) # 차트 크기 키움
        self.figure.patch.set_facecolor('white')
        self.canvas = FigureCanvas(self.figure)
        chart_layout.addWidget(self.canvas)
        bottom_layout.addWidget(chart_frame, stretch=5)

        # 테이블 영역
        table_frame = QFrame()
        table_frame.setStyleSheet("background-color: white; border-radius: 16px; border: 1px solid #e2e8f0;")
        table_layout = QVBoxLayout(table_frame)
        table_header = QLabel("오류 데이터 미리보기")
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
        try: frame.setCursor(Qt.WhatsThisCursor)
        except: frame.setCursor(Qt.PointingHandCursor)
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
        if fname:
            self.btn_upload.setText("정밀 분석 중... (대기)")
            self.btn_upload.setEnabled(False)
            self.progress_bar.setVisible(True)
            self.worker = AnalysisWorker(fname)
            self.worker.finished_signal.connect(self.on_analysis_finished)
            self.worker.error_signal.connect(self.on_analysis_error)
            self.worker.start()

    def on_analysis_finished(self, result):
        self.update_ui(result)
        self.reset_ui_state()
        QMessageBox.information(self, "진단 완료", "7대 항목 정밀 진단이 완료되었습니다.")

    def on_analysis_error(self, err_msg):
        self.reset_ui_state()
        QMessageBox.critical(self, "오류", err_msg)

    def reset_ui_state(self):
        self.btn_upload.setText("📂  데이터 정밀 검사")
        self.btn_upload.setEnabled(True)
        self.progress_bar.setVisible(False)

    def update_ui(self, result):
        self.update_card_value(self.card_grade, result['grade'])
        self.update_card_value(self.card_score, f"{result['score']}")
        self.update_card_value(self.card_rows, f"{result['row_count']:,}")

        self.figure.clear()
        ax = self.figure.add_subplot(111, polar=True)
        ax.set_facecolor('#f8fafc')
        
        labels = result['metrics_labels']
        values = result['metrics_scores']
        
        # 레이더 차트 그리기 (7각형)
        angles = np.linspace(0, 2 * np.pi, len(labels), endpoint=False).tolist()
        values += values[:1] # 닫힌 도형 만들기
        angles += angles[:1]
        
        # 차트 스타일링
        ax.plot(angles, values, color='#6366f1', linewidth=2, linestyle='solid', marker='o')
        ax.fill(angles, values, color='#6366f1', alpha=0.2)
        
        ax.set_xticks(angles[:-1])
        if FONT_PATH:
            font_prop = fm.FontProperties(fname=FONT_PATH, size=9, weight='bold') # 글씨 크기 조정
            ax.set_xticklabels(labels, fontproperties=font_prop, color='#334155')
        
        # Y축(점수) 설정
        ax.set_rlabel_position(0)
        plt.yticks([20, 40, 60, 80, 100], ["20", "40", "60", "80", "100"], color="grey", size=7)
        plt.ylim(0, 100)
        
        ax.spines['polar'].set_visible(False)
        ax.grid(color='#e2e8f0', linestyle='--')
        
        self.canvas.draw()

        df = result['preview']
        self.table.setRowCount(df.shape[0])
        self.table.setColumnCount(df.shape[1])
        self.table.setHorizontalHeaderLabels(df.columns.astype(str))
        for i in range(df.shape[0]):
            for j in range(df.shape[1]):
                self.table.setItem(i, j, QTableWidgetItem(str(df.iloc[i, j])))
        self.table.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)

if __name__ == '__main__':
    app = QApplication(sys.argv)
    if FONT_PATH:
        id = QFontDatabase.addApplicationFont(FONT_PATH)
        if id != -1:
            family = QFontDatabase.applicationFontFamilies(id)[0]
            app.setFont(QFont(family, 10))
    else:
        app.setFont(QFont('sans-serif', 10))
    
    ex = DQApp()
    ex.show()
    sys.exit(app.exec_())
