"""
Componentes HTML/CSS para UI Ultra Moderna.
Estilo: Dark Glassmorphism com Vibrant Orange.
"""

import streamlit as st
from typing import Optional, List, Dict


def load_custom_css():
    st.markdown("""
    <style>
        /* ========== IMPORT FONTS ========== */
        @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;600;700&display=swap');

        :root {
            --bg: #0b0f17;
            --bg-alt: #0f1624;
            --card: rgba(255, 255, 255, 0.04);
            --border: rgba(255, 255, 255, 0.08);
            --text: #e6e6e6;
            --muted: #9aa4b2;
            --primary: #ff6b3d; /* Laranja vibrante moderno */
            --radius: 16px;
        }

        /* ========== BASE RESET & BACKGROUND ========== */
        .stApp {
            background: radial-gradient(1200px 600px at 10% -10%, #162033 0%, transparent 60%),
                        radial-gradient(1000px 500px at 90% 10%, #1b253a 0%, transparent 60%),
                        linear-gradient(180deg, var(--bg) 0%, #0a0e15 100%) !important;
            color: var(--text);
        }

        * { font-family: 'Inter', sans-serif !important; }

        /* ========== SIDEBAR GLASSMOPHISM ========== */
        [data-testid="stSidebar"] {
            background-color: rgba(11, 15, 23, 0.8) !important;
            backdrop-filter: blur(12px) !important;
            border-right: 1px solid var(--border) !important;
        }

        [data-testid="stSidebar"] * { color: var(--text) !important; }

        .stRadio > div { gap: 8px; }
        .stRadio label {
            background: var(--card) !important;
            border: 1px solid var(--border) !important;
            border-radius: 12px !important;
            padding: 10px 16px !important;
            transition: all 0.3s ease !important;
            color: var(--muted) !important;
        }

        .stRadio label:hover {
            border-color: var(--primary) !important;
            background: rgba(255, 107, 61, 0.05) !important;
            transform: translateX(5px);
        }

        .stRadio label[data-checked="true"] {
            background: var(--primary) !important;
            color: #12171f !important;
            font-weight: 700 !important;
            border: none !important;
            box-shadow: 0 4px 15px rgba(255, 107, 61, 0.3) !important;
        }

        .stRadio label div[role="radio"] { display: none; }

        /* ========== TITLES ========== */
        h1, h2, h3 { 
            color: white !important; 
            font-weight: 700 !important;
            letter-spacing: -1px;
        }
        
        .gradient-text {
            background: linear-gradient(90deg, #ffffff 0%, var(--primary) 100%);
            -webkit-background-clip: text;
            background-clip: text;
            -webkit-text-fill-color: transparent;
        }

        /* ========== MODERN CARDS (METRICS) ========== */
        .metric-card {
            background: var(--card);
            border: 1px solid var(--border);
            border-radius: var(--radius);
            padding: 24px;
            transition: all 0.3s ease;
            position: relative;
            overflow: hidden;
        }

        .metric-card:hover {
            transform: translateY(-6px);
            border-color: rgba(255, 107, 61, 0.4);
            background: rgba(255, 255, 255, 0.06);
            box-shadow: 0 12px 32px rgba(0,0,0,0.4);
        }

        .metric-card::after {
            content: '';
            position: absolute;
            top: 0; left: -100%;
            width: 100%; height: 100%;
            background: linear-gradient(90deg, transparent, rgba(255,255,255,0.05), transparent);
            transition: 0.5s;
        }
        .metric-card:hover::after { left: 100%; }

        /* ========== INPUTS & SELECTS ========== */
        .stTextInput input, .stSelectbox [data-baseweb="select"] {
            background: var(--card) !important;
            border: 1px solid var(--border) !important;
            border-radius: 10px !important;
            color: white !important;
        }

        /* ========== BUTTONS ========== */
        .stButton > button {
            background: var(--primary) !important;
            color: #12171f !important;
            font-weight: 700 !important;
            border-radius: 10px !important;
            border: none !important;
            padding: 10px 24px !important;
            transition: all 0.3s ease !important;
            text-transform: uppercase;
            letter-spacing: 1px;
            font-size: 12px !important;
        }

        .stButton > button:hover {
            transform: scale(1.02);
            box-shadow: 0 0 20px rgba(255, 107, 61, 0.4) !important;
            filter: brightness(1.1);
        }

        /* ========== TABS ========== */
        .stTabs [data-baseweb="tab-list"] { background: transparent !important; }
        .stTabs [data-baseweb="tab"] {
            color: var(--muted) !important;
            font-weight: 600 !important;
        }
        .stTabs [aria-selected="true"] {
            color: var(--primary) !important;
            border-bottom-color: var(--primary) !important;
        }

        /* ========== DATA FRAME ========== */
        [data-testid="stDataFrame"] {
            background: var(--card) !important;
            border: 1px solid var(--border) !important;
            border-radius: var(--radius) !important;
        }

        /* ========== SCROLLBAR ========== */
        ::-webkit-scrollbar { width: 6px; }
        ::-webkit-scrollbar-thumb { background: var(--border); border-radius: 10px; }
        ::-webkit-scrollbar-thumb:hover { background: var(--primary); }

    </style>
    """, unsafe_allow_html=True)


def metric_card(label: str, value: str, icon: str = None, trend: str = None, trend_color: str = "green"):
    """Card de métrica estilo Glassmorphism."""
    trend_color_code = "#4ade80" if trend_color == "green" else "#f87171"
    icon_html = f'<div style="font-size: 24px; margin-bottom: 8px;">{icon}</div>' if icon else ""
    trend_html = f'<div style="color: {trend_color_code}; font-size: 13px; font-weight: 600; margin-top: 4px;">{trend}</div>' if trend else ""

    html = f"""
    <div class="metric-card">
        {icon_html}
        <div style="color: var(--muted); font-size: 12px; font-weight: 600; text-transform: uppercase; letter-spacing: 1px;">{label}</div>
        <div style="color: white; font-size: 32px; font-weight: 700; margin-top: 4px;">{value}</div>
        {trend_html}
    </div>
    """
    st.markdown(html, unsafe_allow_html=True)


def info_box(title: str, content: str, icon: str = "ℹ️", type: str = "info"):
    """Box de informação com glow sutil."""
    border_color = "var(--primary)" if type == "info" else "var(--border)"
    st.markdown(f"""
    <div style="background: var(--card); border: 1px solid var(--border); border-left: 4px solid {border_color}; padding: 20px; border-radius: 12px; margin-bottom: 16px;">
        <div style="display: flex; gap: 12px; align-items: flex-start;">
            <div style="font-size: 24px;">{icon}</div>
            <div>
                <div style="color: white; font-weight: 700; font-size: 16px; margin-bottom: 4px;">{title}</div>
                <div style="color: var(--muted); font-size: 14px; line-height: 1.5;">{content}</div>
            </div>
        </div>
    </div>
    """, unsafe_allow_html=True)


def progress_bar(label: str, percentage: float):
    """Barra de progresso neon."""
    st.markdown(f"""
    <div style="margin: 20px 0;">
        <div style="display: flex; justify-content: space-between; margin-bottom: 8px;">
            <span style="color: var(--muted); font-size: 12px; font-weight: 600;">{label}</span>
            <span style="color: var(--primary); font-size: 12px; font-weight: 700;">{percentage:.1f}%</span>
        </div>
        <div style="background: var(--border); height: 6px; border-radius: 10px; overflow: hidden;">
            <div style="background: var(--primary); height: 100%; width: {min(percentage, 100)}%; box-shadow: 0 0 10px var(--primary); transition: width 1s ease;"></div>
        </div>
    </div>
    """, unsafe_allow_html=True)
