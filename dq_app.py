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
# 1. 백그라운드 워커 (데이터 분석용)
# ==========================================
class AnalysisWorker(QThread):
    finished_signal = pyqtSignal(dict)
    error_signal = pyqtSignal(str)

    def __init__(self, filepath):
        super().__init__()
        self.filepath = filepath

    def run(self):
        try:
            # [수정된 부분] 인코딩 자동 감지 로직 추가
            if self.filepath.endswith('.csv'):
                try:
                    # 1순위: UTF-8로 시도 (일반적인 표준)
                    df = pd.read_csv(self.filepath, low_memory=False)
                except UnicodeDecodeError:
                    # 2순위: 실패하면 한국 윈도우 전용(CP949/EUC-KR)로 재시도
                    # print("UTF-8 디코딩 실패, CP949로 재시도합니다.")
                    df = pd.read_csv(self.filepath, encoding='cp949', low_memory=False)
            else:
                # 엑셀 파일은 인코딩 문제 거의 없음
                df = pd.read_excel(self.filepath)
            
            total_rows = len(df)
            if total_rows == 0:
                self.error_signal.emit("데이터가 없습니다.")
                return

            # A. 완전성
            missing_count = df.isnull().sum().sum()
            total_cells = df.size
            completeness = 1 - (missing_count / total_cells)

            # B. 유일성
            duplicates = df.duplicated().sum()
            uniqueness = 1 - (duplicates / total_rows)

            # C. 유효성
            validity_scores = []
            for col in df.columns:
                # 컬럼명도 문자열로 확실히 변환 후 체크
                col_name = str(col).lower()
                if 'email' in col_name or '이메일' in col_name:
                    pattern = r'^[\w\.-]+@[\w\.-]+\.\w+$'
                    # 벡터화 연산 최적화
                    valid_mask = df[col].astype(str).str.match(pattern)
                    valid_cnt = valid_mask.sum()
                    validity_scores.append(valid_cnt / total_rows)
                else:
                    validity_scores.append(1.0)
            
            validity = sum(validity_scores) / len(validity_scores) if validity_scores else 1.0
            total_score = (completeness * 0.4 + uniqueness * 0.3 + validity * 0.3) * 100
            
            if total_score >= 99: grade = "Class A"
            elif total_score >= 97: grade = "Class B"
            elif total_score >= 95: grade = "Class C"
            else: grade = "Uncertified"

            # 결과 전달
            result = {
                "grade": grade,
                "score": round(total_score, 2),
                "metrics": [completeness*100, uniqueness*100, validity*100],
                "row_count": total_rows,
                "col_count": len(df.columns),
                "preview": df.head(50)
            }
            
            self.finished_signal.emit(result)

        except Exception as e:
            self.error_signal.emit(f"파일을 읽는 중 오류가 발생했습니다.\n내용: {str(e)}")

# ==========================================
# 2. GUI 클래스
# ==========================================
class DQApp(QMainWindow):
    def __init__(self):
        super().__init__()
        
        if FONT_PATH and os.path.exists(FONT_PATH):
            QFontDatabase.addApplicationFont(FONT_PATH)
        
        self.setWindowTitle("DQ Pro - 대용량 데이터 품질 평가")
        self.setGeometry(100, 100, 1280, 850)
        
        self.setStyleSheet(f"""
            QMainWindow {{ background-color: #f3f4f6; }}
            QLabel {{ font-family: '{FONT_NAME}', sans-serif; }}
            QPushButton {{ font-family: '{FONT_NAME}', sans-serif; }}
            QToolTip {{ 
                background-color: #1e293b; color: #f8fafc; border: 1px solid #334155; 
                font-family: '{FONT_NAME}'; font-size: 12px; padding: 8px; border-radius: 4px;
            }}
            QProgressBar {{
                border: 2px solid #e2e8f0; border-radius: 5px; text-align: center;
            }}
            QProgressBar::chunk {{
                background-color: #3b82f6; width: 10px;
            }}
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
        sidebar.setFixedWidth(260)
        sidebar.setStyleSheet("background-color: #1e293b; border-right: 1px solid #334155;")
        side_layout = QVBoxLayout(sidebar)
        side_layout.setContentsMargins(30, 50, 30, 50)
        side_layout.setSpacing(20)

        title_lbl = QLabel("DQ CHECKER\nPRO")
        title_lbl.setStyleSheet("color: white; font-size: 28px; font-weight: 900; line-height: 1.2;")
        side_layout.addWidget(title_lbl)
        
        desc_lbl = QLabel("대용량 데이터 진단\n자동화 시스템")
        desc_lbl.setStyleSheet("color: #94a3b8; font-size: 14px;")
        side_layout.addWidget(desc_lbl)
        side_layout.addStretch(1)

        self.btn_upload = QPushButton("📂  데이터 파일 열기")
        self.btn_upload.setCursor(Qt.PointingHandCursor)
        self.btn_upload.setStyleSheet("""
            QPushButton {
                background-color: #3b82f6; color: white; font-size: 16px; 
                padding: 18px; border-radius: 12px; font-weight: bold; border: none;
            }
            QPushButton:hover { background-color: #2563eb; }
            QPushButton:pressed { background-color: #1d4ed8; }
            QPushButton:disabled { background-color: #94a3b8; }
        """)
        self.btn_upload.clicked.connect(self.load_file)
        side_layout.addWidget(self.btn_upload)
        
        self.progress_bar = QProgressBar()
        self.progress_bar.setRange(0, 0)
        self.progress_bar.setStyleSheet("QProgressBar { height: 10px; border: none; background: #334155; } QProgressBar::chunk { background: #3b82f6; }")
        self.progress_bar.setVisible(False)
        side_layout.addWidget(self.progress_bar)

        side_layout.addStretch(4)
        main_layout.addWidget(sidebar)

        # 메인 콘텐츠
        content_widget = QWidget()
        content_layout = QVBoxLayout(content_widget)
        content_layout.setContentsMargins(40, 40, 40, 40)
        content_layout.setSpacing(30)

        # 상단 스코어
        score_layout = QHBoxLayout()
        score_layout.setSpacing(20)
        self.card_grade = self.create_card("최종 등급", "-", "#8b5cf6", "종합 점수에 따른 등급입니다.")
        self.card_score = self.create_card("종합 점수", "0", "#10b981", "품질 지표 가중치 평균입니다.")
        self.card_rows = self.create_card("분석 데이터", "0", "#3b82f6", "데이터 행(Row)의 개수입니다.")
        score_layout.addWidget(self.card_grade)
        score_layout.addWidget(self.card_score)
        score_layout.addWidget(self.card_rows)
        content_layout.addLayout(score_layout)

        # 하단 차트 및 테이블
        bottom_layout = QHBoxLayout()
        bottom_layout.setSpacing(20)

        chart_frame = QFrame()
        chart_frame.setStyleSheet("background-color: white; border-radius: 16px; border: 1px solid #e2e8f0;")
        chart_layout = QVBoxLayout(chart_frame)
        chart_header = QWidget()
        chart_header_layout = QHBoxLayout(chart_header)
        chart_title = QLabel("품질 상세 분석")
        chart_title.setStyleSheet("font-size: 16px; font-weight: bold; color: #334155;")
        chart_info = QLabel("ⓘ")
        chart_info.setToolTip("점수가 높을수록 바깥쪽에 위치합니다.")
        chart_info.setStyleSheet("color: #cbd5e1; font-weight: bold; cursor: help;")
        chart_header_layout.addWidget(chart_title)
        chart_header_layout.addWidget(chart_info)
        chart_header_layout.addStretch(1)
        chart_layout.addWidget(chart_header)

        self.figure = Figure(figsize=(5, 5), dpi=100)
        self.figure.patch.set_facecolor('white')
        self.canvas = FigureCanvas(self.figure)
        chart_layout.addWidget(self.canvas)
        bottom_layout.addWidget(chart_frame, stretch=4)

        table_frame = QFrame()
        table_frame.setStyleSheet("background-color: white; border-radius: 16px; border: 1px solid #e2e8f0;")
        table_layout = QVBoxLayout(table_frame)
        table_header = QLabel("데이터 미리보기 (상위 50건)")
        table_header.setStyleSheet("font-size: 16px; font-weight: bold; color: #334155; padding-bottom: 5px;")
        table_layout.addWidget(table_header)
        self.table = QTableWidget()
        self.table.setStyleSheet("QTableWidget { border: none; gridline-color: #f1f5f9; }")
        table_layout.addWidget(self.table)
        bottom_layout.addWidget(table_frame, stretch=6)
        
        content_layout.addLayout(bottom_layout, stretch=1)
        main_layout.addWidget(content_widget)

    def create_card(self, title, value, color, description):
        frame = QFrame()
        frame.setToolTip(description)
        # 커서 설정 오류 방지
        try:
            frame.setCursor(Qt.WhatsThisCursor)
        except AttributeError:
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
        if fname:
            self.btn_upload.setText("분석 중... (잠시만 기다리세요)")
            self.btn_upload.setEnabled(False)
            self.progress_bar.setVisible(True)
            
            self.worker = AnalysisWorker(fname)
            self.worker.finished_signal.connect(self.on_analysis_finished)
            self.worker.error_signal.connect(self.on_analysis_error)
            self.worker.start()

    def on_analysis_finished(self, result):
        self.update_ui(result)
        self.reset_ui_state()
        QMessageBox.information(self, "완료", "데이터 분석이 완료되었습니다!")

    def on_analysis_error(self, err_msg):
        self.reset_ui_state()
        QMessageBox.critical(self, "Error", f"분석 오류 발생:\n{err_msg}")

    def reset_ui_state(self):
        self.btn_upload.setText("📂  데이터 파일 열기")
        self.btn_upload.setEnabled(True)
        self.progress_bar.setVisible(False)

    def update_ui(self, result):
        self.update_card_value(self.card_grade, result['grade'])
        self.update_card_value(self.card_score, f"{result['score']}")
        self.update_card_value(self.card_rows, f"{result['row_count']:,}")

        self.figure.clear()
        ax = self.figure.add_subplot(111, polar=True)
        ax.set_facecolor('#f8fafc')
        
        raw_categories = ['완전성', '유일성', '유효성']
        metrics = result['metrics']
        display_labels = [f"{cat}\n({val:.1f}%)" for cat, val in zip(raw_categories, metrics)]
        
        values = metrics + metrics[:1]
        angles = [n / float(len(raw_categories)) * 2 * np.pi for n in range(len(raw_categories))]
        angles += angles[:1]

        ax.plot(angles, values, linewidth=2, linestyle='solid', color='#3b82f6', marker='o')
        ax.fill(angles, values, '#3b82f6', alpha=0.2)
        ax.set_xticks(angles[:-1])
        
        if FONT_PATH:
            font_prop = fm.FontProperties(fname=FONT_PATH, size=11, weight='bold')
            ax.set_xticklabels(display_labels, fontproperties=font_prop, color='#334155')
        
        ax.set_rlabel_position(0)
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
