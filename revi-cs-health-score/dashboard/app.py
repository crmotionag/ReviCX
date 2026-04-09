"""
ReviCX Health Score Dashboard
Streamlit application with ReviCX brand identity.
"""

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from pathlib import Path
from sqlalchemy import create_engine, text
from datetime import datetime
import hashlib

# ---------------------------------------------------------------------------
# Page config
# ---------------------------------------------------------------------------
st.set_page_config(
    page_title="ReviCX Health Score",
    page_icon="https://rfrm.io/favicon.ico",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# ---------------------------------------------------------------------------
# ReviCX — Design Tokens
# ---------------------------------------------------------------------------
BLUE_DARK = "#1A3A6B"
BLUE_PRIMARY = "#2563EB"
BLUE_LIGHT = "#DBEAFE"
BG_PAGE = "#F0F4F8"
BG_CARD = "#FFFFFF"
TEXT_DARK = "#1E293B"
TEXT_MUTED = "#64748B"
TEXT_LABEL = "#475569"
ORANGE_CTA = "#F97316"

GREEN = "#22c55e"
YELLOW = "#eab308"
RED = "#ef4444"
PURPLE = "#8B5CF6"

STATUS_COLORS = {"green": GREEN, "yellow": YELLOW, "red": RED}
STATUS_LABELS = {"green": "Campeao", "yellow": "Alerta", "red": "Em Risco"}
STATUS_ICONS = {"green": "&#9679;", "yellow": "&#9679;", "red": "&#9679;"}

# ---------------------------------------------------------------------------
# Global CSS — ReviCX
# ---------------------------------------------------------------------------
st.markdown(f"""
<style>
    /* ---- Page background ---- */
    .stApp, .main, [data-testid="stAppViewContainer"] {{
        background-color: {BG_PAGE} !important;
    }}
    .block-container {{
        padding-top: 0 !important;
        max-width: 1320px;
    }}

    /* ---- Sidebar: compact dark blue ---- */
    [data-testid="stSidebar"] {{
        background: linear-gradient(180deg, {BLUE_DARK} 0%, #0F2847 100%) !important;
        min-width: 220px !important;
        max-width: 220px !important;
    }}
    [data-testid="stSidebar"] * {{
        color: #FFFFFF !important;
    }}
    [data-testid="stSidebar"] .stRadio label {{
        font-size: 0.85rem !important;
        padding: 6px 12px !important;
        border-radius: 8px;
        transition: background 0.2s;
    }}
    [data-testid="stSidebar"] .stRadio label:hover {{
        background: rgba(255,255,255,0.1);
    }}
    [data-testid="stSidebar"] hr {{
        border-color: rgba(255,255,255,0.15) !important;
    }}

    /* ---- Header bar ---- */
    .revi-header {{
        background: linear-gradient(135deg, {BLUE_DARK} 0%, {BLUE_PRIMARY} 100%);
        border-radius: 0 0 16px 16px;
        padding: 28px 36px;
        margin: -1rem -1rem 24px -1rem;
        display: flex;
        justify-content: space-between;
        align-items: center;
    }}
    .revi-header h1 {{
        color: #FFFFFF;
        font-size: 1.5rem;
        font-weight: 700;
        margin: 0;
    }}
    .revi-header .greeting {{
        color: rgba(255,255,255,0.8);
        font-size: 0.9rem;
        margin: 4px 0 0 0;
    }}
    .revi-header .cta {{
        background: {ORANGE_CTA};
        color: #FFF;
        border: none;
        padding: 10px 24px;
        border-radius: 8px;
        font-weight: 600;
        font-size: 0.85rem;
        cursor: pointer;
        text-decoration: none;
    }}

    /* ---- KPI Metric Card ---- */
    .kpi-card {{
        background: {BG_CARD};
        border-radius: 12px;
        padding: 20px 20px 16px 20px;
        box-shadow: 0 1px 3px rgba(0,0,0,0.06), 0 1px 2px rgba(0,0,0,0.04);
        display: flex;
        align-items: center;
        gap: 16px;
        min-height: 90px;
        transition: box-shadow 0.2s;
    }}
    .kpi-card:hover {{
        box-shadow: 0 4px 12px rgba(0,0,0,0.1);
    }}
    .kpi-icon {{
        width: 48px;
        height: 48px;
        border-radius: 50%;
        display: flex;
        align-items: center;
        justify-content: center;
        font-size: 1.3rem;
        flex-shrink: 0;
    }}
    .kpi-content {{
        flex: 1;
    }}
    .kpi-label {{
        color: {TEXT_MUTED};
        font-size: 0.72rem;
        font-weight: 600;
        text-transform: uppercase;
        letter-spacing: 0.5px;
        margin: 0 0 4px 0;
    }}
    .kpi-value {{
        color: {TEXT_DARK};
        font-size: 1.65rem;
        font-weight: 700;
        margin: 0;
        line-height: 1.1;
    }}
    .kpi-sub {{
        color: {TEXT_MUTED};
        font-size: 0.75rem;
        margin: 4px 0 0 0;
    }}

    /* ---- White section card ---- */
    .section-card {{
        background: {BG_CARD};
        border-radius: 12px;
        padding: 24px;
        margin-bottom: 20px;
        box-shadow: 0 1px 3px rgba(0,0,0,0.06);
    }}
    .section-card h3 {{
        color: {TEXT_DARK};
        font-size: 1rem;
        font-weight: 600;
        margin: 0 0 16px 0;
    }}

    /* ---- Recommendation cards with colored top border ---- */
    .rec-card {{
        background: {BG_CARD};
        border-radius: 12px;
        padding: 20px;
        box-shadow: 0 1px 3px rgba(0,0,0,0.06);
        border-top: 3px solid #ccc;
    }}
    .rec-card.gold   {{ border-top-color: #F59E0B; }}
    .rec-card.blue   {{ border-top-color: {BLUE_PRIMARY}; }}
    .rec-card.red    {{ border-top-color: {RED}; }}
    .rec-card.green  {{ border-top-color: {GREEN}; }}
    .rec-card.purple {{ border-top-color: {PURPLE}; }}

    /* ---- Status badges ---- */
    .badge {{
        display: inline-block;
        padding: 3px 12px;
        border-radius: 20px;
        font-size: 0.75rem;
        font-weight: 600;
    }}
    .badge-green  {{ background: #DCFCE7; color: #166534; }}
    .badge-yellow {{ background: #FEF9C3; color: #854D0E; }}
    .badge-red    {{ background: #FEE2E2; color: #991B1B; }}

    .sev-critical {{ background: #FEE2E2; color: #991B1B; padding: 3px 12px; border-radius: 20px; font-weight: 600; font-size: 0.75rem; }}
    .sev-warning  {{ background: #FEF9C3; color: #854D0E; padding: 3px 12px; border-radius: 20px; font-weight: 600; font-size: 0.75rem; }}
    .sev-info     {{ background: {BLUE_LIGHT}; color: #1E40AF; padding: 3px 12px; border-radius: 20px; font-weight: 600; font-size: 0.75rem; }}

    /* ---- Streamlit overrides for light theme ---- */
    .stDataFrame, .stTable {{
        background: {BG_CARD} !important;
        border-radius: 10px;
    }}
    h1, h2, h3, h4, p, span, div, label {{
        color: {TEXT_DARK} !important;
    }}
    /* Header azul: todos os textos brancos */
    .revi-header h1,
    .revi-header p,
    .revi-header span,
    .revi-header div,
    .revi-header label {{
        color: #FFFFFF !important;
    }}
    .stSubheader {{
        color: {TEXT_DARK} !important;
    }}
    [data-testid="stExpander"] {{
        background: {BG_CARD};
        border-radius: 10px;
        border: 1px solid #E2E8F0;
    }}
    .stProgress > div > div > div {{
        background-color: {BLUE_PRIMARY} !important;
    }}
    [data-testid="stMetricValue"] {{
        color: {TEXT_DARK} !important;
    }}

    /* ---- Fix Streamlit default dark elements ---- */
    .stSelectbox > div > div,
    .stDateInput > div > div,
    .stTextInput > div > div {{
        background: {BG_CARD} !important;
        border: 1px solid #CBD5E1 !important;
        border-radius: 8px !important;
        color: #000000 !important;
    }}
    /* Texto dentro das caixas (select e date) — preto sobre fundo branco */
    .stSelectbox [data-baseweb="select"] *,
    .stSelectbox input,
    .stDateInput input,
    .stDateInput [data-baseweb="base-input"] *,
    .stTextInput input {{
        color: #000000 !important;
    }}
    /* Sidebar: texto DENTRO das caixas tambem preto (fundo branco) */
    [data-testid="stSidebar"] [data-baseweb="select"] *,
    [data-testid="stSidebar"] [data-baseweb="base-input"] *,
    [data-testid="stSidebar"] input {{
        color: #000000 !important;
    }}
    /* Sidebar: labels FORA das caixas ficam brancos — herdado da regra * acima */
    [data-testid="stSidebar"] button p,
    [data-testid="stSidebar"] button span {{
        color: #000000 !important;
    }}
</style>
""", unsafe_allow_html=True)


# ---------------------------------------------------------------------------
# Auth helpers
# ---------------------------------------------------------------------------
DB_PATH = Path(__file__).parent.parent / "data" / "revi_cs.db"

AREAS = ["Customer Success", "CX", "Suporte", "Produto", "Comercial", "Marketing", "Diretoria"]
ROLES = ["Gerente", "Supervisor", "Analista", "Assistente"]


def get_engine():
    return create_engine(f"sqlite:///{DB_PATH}")


def hash_pw(pw):
    return hashlib.sha256(pw.encode()).hexdigest()


def authenticate(email, password):
    engine = get_engine()
    with engine.connect() as conn:
        row = conn.execute(
            text("SELECT * FROM app_users WHERE email = :e AND is_active = 1"),
            {"e": email.strip().lower()},
        ).mappings().first()
    if row and row["password_hash"] == hash_pw(password):
        return dict(row)
    return None


def init_auth():
    if "authenticated" not in st.session_state:
        st.session_state.authenticated = False
        st.session_state.user = None


def login_page():
    """Tela de login com KV ReviCX."""
    import base64 as _b64
    _logo_login = Path(__file__).parent / "assets" / "logo-revi-white.png"
    _logo_b64 = _b64.b64encode(_logo_login.read_bytes()).decode()

    if "login_error" not in st.session_state:
        st.session_state.login_error = False

    # Espaço para centralizar verticalmente
    st.markdown("<div style='margin-top:8vh;'></div>", unsafe_allow_html=True)

    col_spacer1, col_form, col_spacer2 = st.columns([1, 1.2, 1])
    with col_form:
        # Logo + título dentro da coluna do form
        st.markdown(f"""
        <div style="background:linear-gradient(135deg,{BLUE_DARK},{BLUE_PRIMARY});
                    padding:32px 40px 24px 40px; border-radius:16px 16px 0 0;
                    text-align:center; box-shadow:0 4px 16px rgba(26,58,107,0.2);">
            <img src="data:image/png;base64,{_logo_b64}" alt="ReviCX" style="max-height:56px; margin-bottom:8px;">
            <p style="color:rgba(255,255,255,0.75); font-size:0.88rem; margin:0;">
                Health Score Dashboard
            </p>
        </div>
        """, unsafe_allow_html=True)

        with st.form("login_form"):
            st.markdown(f"<p style='text-align:center;color:{TEXT_MUTED};font-size:0.85rem;margin-top:4px;'>Entre com suas credenciais</p>", unsafe_allow_html=True)
            email = st.text_input("Email", placeholder="seu@email.com")
            password = st.text_input("Senha", type="password", placeholder="********")
            submitted = st.form_submit_button("Entrar", use_container_width=True, type="primary")

            if submitted:
                user = authenticate(email, password)
                if user:
                    st.session_state.authenticated = True
                    st.session_state.user = user
                    st.session_state.login_error = False
                    # Update last_login
                    engine = get_engine()
                    with engine.begin() as conn:
                        conn.execute(text("UPDATE app_users SET last_login = :now WHERE id = :uid"),
                                     {"now": datetime.now().isoformat(), "uid": user["id"]})
                    st.rerun()
                else:
                    st.session_state.login_error = True
                    st.rerun()

        if st.session_state.login_error:
            st.error("Email ou senha incorretos. Verifique suas credenciais e tente novamente.")


def admin_page():
    """Painel administrativo para gerenciar usuarios."""
    st.markdown(header("Painel Administrativo", "Gerencie usuarios e acessos do dashboard."), unsafe_allow_html=True)

    engine = get_engine()

    # --- Criar novo usuario ---
    st.subheader("Criar Novo Usuario")
    with st.form("create_user_form", clear_on_submit=True):
        col1, col2 = st.columns(2)
        with col1:
            new_name = st.text_input("Nome completo *")
            new_email = st.text_input("Email *", placeholder="nome@empresa.com")
            new_password = st.text_input("Senha *", type="password", placeholder="Min. 6 caracteres")
        with col2:
            new_area = st.selectbox("Area *", AREAS)
            new_role = st.selectbox("Cargo *", ROLES)
            new_admin = st.checkbox("Administrador?", help="Admins podem criar e gerenciar outros usuarios")

        create_submitted = st.form_submit_button("Criar Usuario", use_container_width=True, type="primary")

        if create_submitted:
            errors = []
            if not new_name.strip():
                errors.append("Nome e obrigatorio")
            if not new_email.strip() or "@" not in new_email:
                errors.append("Email invalido")
            if not new_password or len(new_password) < 6:
                errors.append("Senha deve ter no minimo 6 caracteres")
            if errors:
                for e in errors:
                    st.error(e)
            else:
                try:
                    with engine.begin() as conn:
                        conn.execute(text("""
                            INSERT INTO app_users (name, email, password_hash, area, role, is_active, is_admin)
                            VALUES (:name, :email, :pwd, :area, :role, 1, :admin)
                        """), {
                            "name": new_name.strip(),
                            "email": new_email.strip().lower(),
                            "pwd": hash_pw(new_password),
                            "area": new_area,
                            "role": new_role,
                            "admin": 1 if new_admin else 0,
                        })
                    st.success(f"Usuario {new_name} criado com sucesso!")
                    st.rerun()
                except Exception as ex:
                    if "UNIQUE constraint" in str(ex):
                        st.error("Ja existe um usuario com esse email.")
                    else:
                        st.error(f"Erro: {ex}")

    # --- Lista de usuarios ---
    st.subheader("Usuarios Cadastrados")
    users_df = pd.read_sql("SELECT id, name, email, area, role, is_active, is_admin, created_at, last_login FROM app_users ORDER BY name", engine)

    if len(users_df) == 0:
        st.info("Nenhum usuario cadastrado.")
        return

    # Format display
    display = users_df.copy()
    display["Status"] = display["is_active"].map({1: "Ativo", 0: "Inativo"})
    display["Admin"] = display["is_admin"].map({1: "Sim", 0: "Nao"})
    display = display.rename(columns={
        "name": "Nome", "email": "Email", "area": "Area",
        "role": "Cargo", "created_at": "Criado em", "last_login": "Ultimo Login",
    })

    st.dataframe(
        display[["Nome", "Email", "Area", "Cargo", "Status", "Admin", "Criado em", "Ultimo Login"]],
        use_container_width=True,
        hide_index=True,
    )

    # --- Gerenciar usuario ---
    st.subheader("Gerenciar Usuario")
    user_options = [f"{r['name']} ({r['email']})" for _, r in users_df.iterrows()]
    selected_idx = st.selectbox("Selecione o usuario", range(len(user_options)), format_func=lambda i: user_options[i])

    if selected_idx is not None:
        sel_user = users_df.iloc[selected_idx]
        col1, col2, col3, col4 = st.columns(4)

        with col1:
            if sel_user["is_active"]:
                if st.button("Desativar", key=f"deact_{sel_user['id']}", use_container_width=True):
                    with engine.begin() as conn:
                        conn.execute(text("UPDATE app_users SET is_active = 0 WHERE id = :uid"), {"uid": int(sel_user["id"])})
                    st.success(f"{sel_user['name']} desativado.")
                    st.rerun()
            else:
                if st.button("Reativar", key=f"react_{sel_user['id']}", use_container_width=True, type="primary"):
                    with engine.begin() as conn:
                        conn.execute(text("UPDATE app_users SET is_active = 1 WHERE id = :uid"), {"uid": int(sel_user["id"])})
                    st.success(f"{sel_user['name']} reativado.")
                    st.rerun()

        with col2:
            if not sel_user["is_admin"]:
                if st.button("Tornar Admin", key=f"admin_{sel_user['id']}", use_container_width=True):
                    with engine.begin() as conn:
                        conn.execute(text("UPDATE app_users SET is_admin = 1 WHERE id = :uid"), {"uid": int(sel_user["id"])})
                    st.success(f"{sel_user['name']} agora e admin.")
                    st.rerun()
            else:
                if st.button("Remover Admin", key=f"rmadmin_{sel_user['id']}", use_container_width=True):
                    with engine.begin() as conn:
                        conn.execute(text("UPDATE app_users SET is_admin = 0 WHERE id = :uid"), {"uid": int(sel_user["id"])})
                    st.success(f"{sel_user['name']} nao e mais admin.")
                    st.rerun()

        with col3:
            if st.button("Resetar Senha", key=f"reset_{sel_user['id']}", use_container_width=True):
                new_pw = "revi2026"
                with engine.begin() as conn:
                    conn.execute(text("UPDATE app_users SET password_hash = :pwd WHERE id = :uid"),
                                 {"pwd": hash_pw(new_pw), "uid": int(sel_user["id"])})
                st.success(f"Senha de {sel_user['name']} resetada para: {new_pw}")

        with col4:
            if int(sel_user["id"]) != st.session_state.user["id"]:
                if st.button("Excluir", key=f"del_{sel_user['id']}", use_container_width=True):
                    with engine.begin() as conn:
                        conn.execute(text("DELETE FROM app_users WHERE id = :uid"), {"uid": int(sel_user["id"])})
                    st.warning(f"{sel_user['name']} excluido.")
                    st.rerun()


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------
@st.cache_data(ttl=300)
def load_data():
    db_path = Path(__file__).parent.parent / "data" / "revi_cs.db"
    engine = create_engine(f"sqlite:///{db_path}")
    clients = pd.read_sql("SELECT * FROM dim_clients", engine)
    health = pd.read_sql("SELECT * FROM fct_health_score", engine)
    alerts = pd.read_sql("SELECT * FROM fct_alerts", engine)
    csm_activity = pd.read_sql("SELECT * FROM fct_csm_activity_weekly", engine)
    coverage = pd.read_sql("SELECT * FROM fct_coverage_monthly", engine)
    revenue = pd.read_sql("SELECT * FROM fct_revenue_retention_monthly", engine)
    nps = pd.read_sql("SELECT * FROM raw_hubspot_nps", engine)
    health["calculated_at"] = pd.to_datetime(health["calculated_at"])
    alerts["alert_date"] = pd.to_datetime(alerts["alert_date"])
    csm_activity["week_start"] = pd.to_datetime(csm_activity["week_start"])
    nps["response_date"] = pd.to_datetime(nps["response_date"])
    # Canais usados por cliente (SMS / Email)
    campaign_channels = pd.read_sql("""
        SELECT revi_client_id,
               MAX(CASE WHEN campaign_type = 'sms'   THEN 1 ELSE 0 END) AS has_sms,
               MAX(CASE WHEN campaign_type = 'email' THEN 1 ELSE 0 END) AS has_email
        FROM raw_revi_campaigns
        GROUP BY revi_client_id
    """, engine)
    return clients, health, alerts, csm_activity, coverage, revenue, nps, campaign_channels


# ---------------------------------------------------------------------------
# AUTH GATE — tudo abaixo so roda se autenticado
# ---------------------------------------------------------------------------
init_auth()

if not st.session_state.authenticated:
    login_page()
    st.stop()

# --- Usuario autenticado daqui pra baixo ---
clients, health, alerts, csm_activity, coverage, revenue, nps, campaign_channels = load_data()

latest_health = (
    health.sort_values("calculated_at")
    .groupby("client_id")
    .last()
    .reset_index()
)
client_health = clients.merge(latest_health, on="client_id", how="left")


# ---------------------------------------------------------------------------
# Helper: KPI card (icon + uppercase label + bold value)
# ---------------------------------------------------------------------------
def kpi(label, value, icon="&#128202;", color=BLUE_PRIMARY, sub=""):
    bg_light = color + "18"
    sub_part = f'<span class="kpi-sub">{sub}</span>' if sub else ""
    return (
        f'<div class="kpi-card">'
        f'<div class="kpi-icon" style="background:{bg_light};color:{color};">{icon}</div>'
        f'<div class="kpi-content">'
        f'<p class="kpi-label">{label}</p>'
        f'<p class="kpi-value">{value}</p>'
        f'{sub_part}'
        f'</div></div>'
    )


def header(title, subtitle=""):
    hour = datetime.now().hour
    if hour < 12:
        greet = "Bom dia"
    elif hour < 18:
        greet = "Boa tarde"
    else:
        greet = "Boa noite"
    sub_text = f"{greet}! {subtitle}" if subtitle else f"{greet}!"
    return (
        f'<div class="revi-header">'
        f'<div><h1>{title}</h1><p class="greeting">{sub_text}</p></div>'
        f'<a class="cta" href="#">Exportar Relatorio</a>'
        f'</div>'
    )


# ---------------------------------------------------------------------------
# Plotly light layout
# ---------------------------------------------------------------------------
PLOTLY_LAYOUT = dict(
    template="plotly_white",
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor="rgba(0,0,0,0)",
    font=dict(color=TEXT_DARK, family="Inter, sans-serif"),
    margin=dict(l=40, r=20, t=40, b=40),
)

# ---------------------------------------------------------------------------
# Sidebar
# ---------------------------------------------------------------------------
user = st.session_state.user

with st.sidebar:
    # Logo
    import base64 as _b64_sidebar
    _logo_sidebar = Path(__file__).parent / "assets" / "logo-revi-white.png"
    _logo_sidebar_b64 = _b64_sidebar.b64encode(_logo_sidebar.read_bytes()).decode()
    st.markdown(
        f'<div style="text-align:center; padding:16px 0 4px 0;">'
        f'<img src="data:image/png;base64,{_logo_sidebar_b64}" alt="ReviCX" style="max-height:40px;">'
        f'</div>',
        unsafe_allow_html=True,
    )
    st.markdown("---")

    # User info
    st.markdown(
        f'<div style="padding:4px 8px 12px 8px;">'
        f'<p style="color:#FFF !important; font-weight:600; font-size:0.9rem; margin:0;">{user["name"]}</p>'
        f'<p style="color:rgba(255,255,255,0.6) !important; font-size:0.72rem; margin:2px 0 0 0;">'
        f'{user["role"]} — {user["area"]}</p>'
        f'</div>',
        unsafe_allow_html=True,
    )
    st.markdown("---")

    # Menu — admin so aparece para admins
    menu_items = [
        "Visao Geral",
        "Carteira do CSM",
        "Upsell",
        "Alertas",
        "Performance CSM",
        "Receita (GRR/NRR)",
        "NPS",
    ]
    if user.get("is_admin"):
        menu_items.append("Admin")

    page = st.radio("Menu", menu_items, label_visibility="collapsed")

    st.markdown("---")
    if st.button("Sair", use_container_width=True):
        st.session_state.authenticated = False
        st.session_state.user = None
        st.rerun()


# =========================================================================
# PAGE 1 — Visao Geral da Base
# =========================================================================
if page == "Visao Geral":
    st.markdown(header("Health Score — Visao Geral", "Acompanhe a saude da base de clientes ReviCX."), unsafe_allow_html=True)

    # --- Filtro de data na sidebar ---
    with st.sidebar:
        st.markdown("### Filtros")
        _min_date = health["calculated_at"].min().date()
        _max_date = health["calculated_at"].max().date()
        date_range = st.date_input(
            "Periodo",
            value=(_min_date, _max_date),
            min_value=_min_date,
            max_value=_max_date,
        )

    if isinstance(date_range, (list, tuple)) and len(date_range) == 2:
        _date_start, _date_end = date_range
    else:
        _date_start, _date_end = _min_date, _max_date

    _health_filtered = health[
        (health["calculated_at"].dt.date >= _date_start) &
        (health["calculated_at"].dt.date <= _date_end)
    ]
    _latest_filtered = (
        _health_filtered.sort_values("calculated_at")
        .groupby("client_id").last().reset_index()
    )
    _ch = clients.merge(_latest_filtered, on="client_id", how="left")

    # --- session state para filtro de status ---
    if "vg_status_filter" not in st.session_state:
        st.session_state.vg_status_filter = None

    total = len(_ch)
    green_n = len(_ch[_ch["health_status"] == "green"])
    yellow_n = len(_ch[_ch["health_status"] == "yellow"])
    red_n = len(_ch[_ch["health_status"] == "red"])
    green_pct = f"{green_n/total*100:.0f}%" if total else "0%"
    yellow_pct = f"{yellow_n/total*100:.0f}%" if total else "0%"
    red_pct = f"{red_n/total*100:.0f}%" if total else "0%"

    c1, c2, c3, c4 = st.columns(4)
    with c1:
        st.markdown(kpi("TOTAL CLIENTES", total, "&#128101;", BLUE_PRIMARY), unsafe_allow_html=True)
    with c2:
        st.markdown(kpi("CAMPEOES", green_n, "&#9989;", GREEN, green_pct), unsafe_allow_html=True)
    with c3:
        st.markdown(kpi("EM ALERTA", yellow_n, "&#9888;", YELLOW, yellow_pct), unsafe_allow_html=True)
    with c4:
        st.markdown(kpi("EM RISCO", red_n, "&#128680;", RED, red_pct), unsafe_allow_html=True)

    st.markdown("<div style='height:12px'></div>", unsafe_allow_html=True)

    # Row 2 — Score, GRR, NRR
    avg_score = _ch["total_score"].mean()
    rev_sorted = revenue.sort_values("year_month")
    latest_rev = rev_sorted.iloc[-1] if len(rev_sorted) else None
    grr_val = latest_rev["grr"] * 100 if latest_rev is not None else 0
    nrr_val = latest_rev["nrr"] * 100 if latest_rev is not None else 0

    c1, c2, c3 = st.columns(3)
    with c1:
        st.markdown(kpi("SCORE MEDIO", f"{avg_score:.1f}", "&#127942;", PURPLE, "de 30 pontos"), unsafe_allow_html=True)
    with c2:
        st.markdown(kpi("GRR", f"{grr_val:.1f}%", "&#128176;", GREEN if grr_val >= 87 else RED, "Meta: 87%"), unsafe_allow_html=True)
    with c3:
        st.markdown(kpi("NRR", f"{nrr_val:.1f}%", "&#128200;", GREEN if nrr_val >= 105 else YELLOW, "Meta: 105%"), unsafe_allow_html=True)

    st.markdown("<div style='height:16px'></div>", unsafe_allow_html=True)

    # Charts
    col_left, col_right = st.columns(2)

    with col_left:
        st.subheader("Distribuicao por Status")

        _btn_all, _btn_green, _btn_yellow, _btn_red = st.columns(4)
        with _btn_all:
            if st.button("Todos", use_container_width=True,
                         type="primary" if st.session_state.vg_status_filter is None else "secondary",
                         key="vg_btn_all"):
                st.session_state.vg_status_filter = None
                st.rerun()
        with _btn_green:
            if st.button("Campeao", use_container_width=True,
                         type="primary" if st.session_state.vg_status_filter == "green" else "secondary",
                         key="vg_btn_green"):
                st.session_state.vg_status_filter = "green"
                st.rerun()
        with _btn_yellow:
            if st.button("Alerta", use_container_width=True,
                         type="primary" if st.session_state.vg_status_filter == "yellow" else "secondary",
                         key="vg_btn_yellow"):
                st.session_state.vg_status_filter = "yellow"
                st.rerun()
        with _btn_red:
            if st.button("Em Risco", use_container_width=True,
                         type="primary" if st.session_state.vg_status_filter == "red" else "secondary",
                         key="vg_btn_red"):
                st.session_state.vg_status_filter = "red"
                st.rerun()

        fig_donut = go.Figure(go.Pie(
            labels=["Campeao", "Alerta", "Em Risco"],
            values=[green_n, yellow_n, red_n],
            hole=0.6,
            marker=dict(colors=[GREEN, YELLOW, RED]),
            textinfo="label+value+percent",
            textfont=dict(size=12),
        ))
        fig_donut.update_layout(**PLOTLY_LAYOUT, showlegend=False, height=300)
        st.plotly_chart(fig_donut, use_container_width=True)

    with col_right:
        st.subheader("Evolucao do Score Medio")
        monthly_avg = (
            _health_filtered.groupby(_health_filtered["calculated_at"].dt.to_period("M"))["total_score"]
            .mean().reset_index()
        )
        monthly_avg["calculated_at"] = monthly_avg["calculated_at"].dt.to_timestamp()
        monthly_avg = monthly_avg.sort_values("calculated_at").tail(7)

        fig_line = go.Figure(go.Scatter(
            x=monthly_avg["calculated_at"],
            y=monthly_avg["total_score"],
            mode="lines+markers",
            line=dict(color=BLUE_PRIMARY, width=3),
            marker=dict(size=8, color=BLUE_PRIMARY),
            fill="tozeroy",
            fillcolor="rgba(37,99,235,0.08)",
        ))
        fig_line.update_layout(**PLOTLY_LAYOUT, height=300, yaxis_title="Score Medio")
        st.plotly_chart(fig_line, use_container_width=True)

    # Stacked bar — status evolution
    st.subheader("Evolucao de Status por Mes")
    status_monthly = (
        _health_filtered.groupby([_health_filtered["calculated_at"].dt.to_period("M"), "health_status"])
        .size().reset_index(name="count")
    )
    status_monthly["month"] = status_monthly["calculated_at"].dt.to_timestamp()
    status_monthly = status_monthly.sort_values("month")
    recent_months = sorted(status_monthly["month"].unique())[-7:]
    status_monthly = status_monthly[status_monthly["month"].isin(recent_months)]

    fig_bar = go.Figure()
    for status, color in STATUS_COLORS.items():
        subset = status_monthly[status_monthly["health_status"] == status]
        fig_bar.add_trace(go.Bar(
            x=subset["month"], y=subset["count"],
            name=STATUS_LABELS.get(status, status),
            marker_color=color,
        ))
    fig_bar.update_layout(**PLOTLY_LAYOUT, barmode="stack", height=360, yaxis_title="Clientes")
    st.plotly_chart(fig_bar, use_container_width=True)

    # Top 10 — dinamico por filtro de status
    _sf = st.session_state.vg_status_filter
    if _sf is None:
        _top_title = "Top 10 Clientes em Risco"
        _top_df = _ch[_ch["health_status"] == "red"].sort_values("total_score").head(10)
        _empty_msg = "Nenhum cliente em risco no periodo selecionado."
    else:
        _status_label = STATUS_LABELS.get(_sf, _sf)
        _top_title = f"Top 10 Clientes — {_status_label}"
        _sort_asc = _sf == "red"
        _top_df = _ch[_ch["health_status"] == _sf].sort_values("total_score", ascending=_sort_asc).head(10)
        _empty_msg = f"Nenhum cliente com status '{_status_label}' no periodo selecionado."

    st.subheader(_top_title)
    if len(_top_df) == 0:
        st.info(_empty_msg)
    else:
        _risk_table = _top_df[
            ["company_name", "segment_ipc", "total_score", "days_since_last_campaign", "campaign_roi", "days_to_renewal"]
        ].rename(columns={
            "company_name": "Empresa", "segment_ipc": "Segmento",
            "total_score": "Score", "days_since_last_campaign": "Dias s/ Campanha",
            "campaign_roi": "ROI", "days_to_renewal": "Renovacao (dias)",
        })
        _risk_table = _risk_table.reset_index(drop=True)
        _risk_table.index = _risk_table.index + 1
        st.dataframe(_risk_table, use_container_width=True, height=400)


# =========================================================================
# PAGE 2 — Carteira do CSM
# =========================================================================
elif page == "Carteira do CSM":
    st.markdown(header("Carteira do CSM", "Visualize e filtre a base de clientes por CSM."), unsafe_allow_html=True)

    with st.sidebar:
        st.markdown("### Filtros")
        csm_list = sorted(client_health["csm_owner"].dropna().unique().tolist())
        csm_filter = st.selectbox("CSM", ["Todos"] + csm_list)
        status_filter = st.selectbox("Status", ["Todos", "Campeao", "Alerta", "Em Risco"])

    status_map = {"Campeao": "green", "Alerta": "yellow", "Em Risco": "red"}

    df = client_health.copy()
    if csm_filter != "Todos":
        df = df[df["csm_owner"] == csm_filter]
    if status_filter != "Todos":
        df = df[df["health_status"] == status_map[status_filter]]

    # Summary cards
    c1, c2, c3, c4 = st.columns(4)
    with c1:
        st.markdown(kpi("CLIENTES", len(df), "&#128101;", BLUE_PRIMARY), unsafe_allow_html=True)
    with c2:
        g = len(df[df["health_status"] == "green"])
        st.markdown(kpi("CAMPEOES", g, "&#9989;", GREEN), unsafe_allow_html=True)
    with c3:
        y = len(df[df["health_status"] == "yellow"])
        st.markdown(kpi("ALERTA", y, "&#9888;", YELLOW), unsafe_allow_html=True)
    with c4:
        r = len(df[df["health_status"] == "red"])
        st.markdown(kpi("EM RISCO", r, "&#128680;", RED), unsafe_allow_html=True)

    st.markdown("<div style='height:16px'></div>", unsafe_allow_html=True)

    # ------------------------------------------------------------------
    # Busca por cliente
    # ------------------------------------------------------------------
    search_query = st.text_input(
        "Buscar cliente",
        placeholder="Digite o nome da empresa...",
        key="client_search",
    )

    filters_active = csm_filter != "Todos" or status_filter != "Todos"

    if not search_query and not filters_active:
        st.info("Use os filtros na barra lateral ou busque pelo nome de um cliente para visualizar os detalhes.")
        filtered = df.iloc[0:0]  # dataframe vazio
    elif search_query:
        filtered = df[df["company_name"].str.contains(search_query, case=False, na=False)]
        if len(filtered) == 0:
            st.warning(f"Nenhum cliente encontrado para \"{search_query}\".")
        else:
            st.caption(f"{len(filtered)} cliente(s) encontrado(s)")
    else:
        filtered = df
        st.caption(f"{len(filtered)} cliente(s) encontrado(s)")

    if len(filtered) > 0:

        score_criteria = {
            "score_recency": ("Recencia de Campanha", 5),
            "score_roi": ("ROI de Campanha", 5),
            "score_automations": ("Automacoes Ativas", 5),
            "score_integrations": ("Integracoes", 5),
            "score_chat": ("Uso do Chat", 5),
            "score_volume": ("Volume de Mensagens", 5),
        }

        # Load historical scores for sparklines
        client_ids_filtered = filtered["client_id"].tolist()
        hist = health[health["client_id"].isin(client_ids_filtered)].copy()

        for _, row in filtered.iterrows():
            status_label = STATUS_LABELS.get(row.get("health_status", ""), "")
            status_color = STATUS_COLORS.get(row.get("health_status", ""), BLUE_PRIMARY)
            score_val = row.get("total_score", 0) or 0
            expanded = len(filtered) == 1

            with st.expander(
                f"{row['company_name']}  |  Score: {score_val}  |  {status_label}",
                expanded=expanded,
            ):
                # --- Row 1: Ficha resumo ---
                r1c1, r1c2, r1c3, r1c4 = st.columns(4)
                with r1c1:
                    st.markdown(kpi("SCORE", score_val, "&#127942;", status_color), unsafe_allow_html=True)
                with r1c2:
                    st.markdown(kpi("SEGMENTO", row.get("segment_ipc", "-"), "&#127991;", BLUE_PRIMARY), unsafe_allow_html=True)
                with r1c3:
                    mrr_val = row.get("mrr", 0) or 0
                    st.markdown(kpi("MRR", f"R$ {mrr_val:,.0f}", "&#128176;", GREEN), unsafe_allow_html=True)
                with r1c4:
                    renewal = row.get("days_to_renewal", 0) or 0
                    ren_color = RED if renewal <= 30 else (YELLOW if renewal <= 90 else GREEN)
                    st.markdown(kpi("RENOVACAO", f"{renewal} dias", "&#128197;", ren_color), unsafe_allow_html=True)

                st.markdown("<div style='height:8px'></div>", unsafe_allow_html=True)

                # --- Row 2: Info basica ---
                r2c1, r2c2, r2c3, r2c4 = st.columns(4)
                with r2c1:
                    st.markdown(f"**CSM:** {row.get('csm_owner', '-')}")
                with r2c2:
                    cs_text = "Sim" if row.get("has_cs") else "Nao"
                    st.markdown(f"**CS Contratado:** {cs_text}")
                with r2c3:
                    st.markdown(f"**Plano:** {row.get('plan_type', '-')}")
                with r2c4:
                    days_client = row.get("days_as_client", 0) or 0
                    st.markdown(f"**Cliente ha:** {days_client} dias")

                st.markdown("---")

                # --- Row 3: Breakdown dos 6 criterios ---
                st.markdown("**Breakdown do Health Score**")
                for col_key, (label, max_val) in score_criteria.items():
                    val = row.get(col_key, 0) or 0
                    pct = val / max_val if max_val > 0 else 0
                    bar_color = GREEN if pct >= 0.8 else (YELLOW if pct >= 0.5 else RED)
                    c1, c2, c3 = st.columns([3, 5, 1])
                    with c1:
                        st.markdown(f"<span style='font-size:0.85rem;color:{TEXT_MUTED};'>{label}</span>", unsafe_allow_html=True)
                    with c2:
                        st.progress(min(pct, 1.0))
                    with c3:
                        st.markdown(f"**{val}** / {max_val}")

                st.markdown("---")

                # --- Row 4: Metricas brutas ---
                st.markdown("**Metricas Detalhadas**")
                m1, m2, m3, m4 = st.columns(4)
                with m1:
                    days_camp = row.get("days_since_last_campaign", 0) or 0
                    st.metric("Dias s/ Campanha", days_camp)
                with m2:
                    roi = row.get("campaign_roi", 0) or 0
                    st.metric("ROI Campanha", f"{roi:.1f}x")
                with m3:
                    auto = row.get("active_automations", 0) or 0
                    integ = row.get("integration_automations", 0) or 0
                    st.metric("Automacoes", f"{auto} std + {integ} integ")
                with m4:
                    usage = row.get("plan_usage_pct", 0) or 0
                    st.metric("Uso do Plano", f"{usage:.0f}%")

                m5, m6, m7, m8 = st.columns(4)
                with m5:
                    chat_lvl = row.get("chat_usage_level", "none") or "none"
                    chat_display = {"advanced": "Avancado", "essential": "Essencial", "none": "Nao usa"}.get(chat_lvl, chat_lvl)
                    st.metric("Chat", chat_display)
                with m6:
                    msgs = row.get("messages_sent_current", 0) or 0
                    st.metric("Msgs Mes Atual", f"{msgs:,}")
                with m7:
                    mom = row.get("messages_mom_change", 0) or 0
                    st.metric("Variacao MoM", f"{mom:+.1f}%")
                with m8:
                    cashback = "Ativo" if row.get("cashback_enabled") else "Inativo"
                    st.metric("Cashback", cashback)

                # --- Row 5: Evolucao do score (historico) ---
                client_hist = hist[hist["client_id"] == row["client_id"]].sort_values("calculated_at")
                if len(client_hist) > 1:
                    st.markdown("---")
                    st.markdown("**Evolucao do Score**")
                    fig_hist = go.Figure(go.Scatter(
                        x=client_hist["calculated_at"],
                        y=client_hist["total_score"],
                        mode="lines+markers",
                        line=dict(color=BLUE_PRIMARY, width=2),
                        marker=dict(size=6, color=BLUE_PRIMARY),
                        fill="tozeroy",
                        fillcolor="rgba(37,99,235,0.06)",
                    ))
                    # Faixas de classificacao
                    fig_hist.add_hrect(y0=26, y1=30, fillcolor=GREEN, opacity=0.07, line_width=0, annotation_text="Campeao", annotation_position="top left")
                    fig_hist.add_hrect(y0=16, y1=25, fillcolor=YELLOW, opacity=0.07, line_width=0, annotation_text="Alerta", annotation_position="top left")
                    fig_hist.add_hrect(y0=0, y1=15, fillcolor=RED, opacity=0.07, line_width=0, annotation_text="Em Risco", annotation_position="top left")
                    fig_hist.update_layout(**PLOTLY_LAYOUT, height=250, yaxis_title="Score", yaxis_range=[0, 32])
                    st.plotly_chart(fig_hist, use_container_width=True)

                # --- Row 6: Alertas ativos deste cliente ---
                client_alerts = alerts[
                    (alerts["client_id"] == row["client_id"]) & (alerts["resolved"] == 0)
                ]
                if len(client_alerts) > 0:
                    st.markdown("---")
                    st.markdown(f"**Alertas Ativos ({len(client_alerts)})**")
                    for _, a in client_alerts.iterrows():
                        sev = a.get("severity", "info")
                        sev_icon = {"critical": "&#128308;", "warning": "&#128993;", "info": "&#128309;"}.get(sev, "")
                        st.markdown(
                            f"<span style='font-size:0.85rem;'>{sev_icon} <b>{a.get('alert_name', '')}</b> — {a.get('action_suggested', '')}</span>",
                            unsafe_allow_html=True,
                        )


# =========================================================================
# PAGE 3 — Alertas
# =========================================================================
elif page == "Alertas":
    st.markdown(header("Central de Alertas", "Monitore riscos e oportunidades da base."), unsafe_allow_html=True)

    with st.sidebar:
        st.markdown("### Filtros")
        sev_filter = st.selectbox("Severidade", ["Todos", "critical", "warning", "info"])
        csm_list_a = sorted(clients["csm_owner"].dropna().unique().tolist())
        csm_alert_filter = st.selectbox("CSM", ["Todos"] + csm_list_a, key="alert_csm")

    active_alerts = alerts[alerts["resolved"] == 0].copy()
    active_alerts = active_alerts.merge(
        clients[["client_id", "csm_owner", "company_name"]], on="client_id", how="left",
    )
    if csm_alert_filter != "Todos":
        active_alerts = active_alerts[active_alerts["csm_owner"] == csm_alert_filter]
    if sev_filter != "Todos":
        active_alerts = active_alerts[active_alerts["severity"] == sev_filter]

    total_active = len(active_alerts)
    critical_n = len(active_alerts[active_alerts["severity"] == "critical"])
    warning_n = len(active_alerts[active_alerts["severity"] == "warning"])
    info_n = len(active_alerts[active_alerts["severity"] == "info"])

    c1, c2, c3, c4 = st.columns(4)
    with c1:
        st.markdown(kpi("TOTAL ALERTAS", total_active, "&#128276;", BLUE_PRIMARY), unsafe_allow_html=True)
    with c2:
        st.markdown(kpi("CRITICOS", critical_n, "&#128308;", RED), unsafe_allow_html=True)
    with c3:
        st.markdown(kpi("WARNING", warning_n, "&#128993;", YELLOW), unsafe_allow_html=True)
    with c4:
        st.markdown(kpi("INFO", info_n, "&#128309;", BLUE_PRIMARY), unsafe_allow_html=True)

    st.markdown("<div style='height:16px'></div>", unsafe_allow_html=True)

    sev_order = {"critical": 0, "warning": 1, "info": 2}
    active_alerts["sev_order"] = active_alerts["severity"].map(sev_order).fillna(3)
    active_alerts = active_alerts.sort_values(["sev_order", "alert_date"], ascending=[True, False])

    st.subheader("Alertas Ativos")
    if len(active_alerts) == 0:
        st.success("Nenhum alerta ativo.")
    else:
        alert_display = active_alerts[
            ["company_name", "alert_name", "severity", "action_suggested", "alert_date"]
        ].rename(columns={
            "company_name": "Cliente", "alert_name": "Alerta",
            "severity": "Severidade", "action_suggested": "Acao Sugerida",
            "alert_date": "Data",
        })
        alert_display = alert_display.reset_index(drop=True)
        alert_display.index = alert_display.index + 1
        alert_display["Data"] = pd.to_datetime(alert_display["Data"]).dt.strftime("%Y-%m-%d")

        sev_colors_map = {"critical": "#FEE2E2", "warning": "#FEF9C3", "info": "#DBEAFE"}

        def color_alert_row(row):
            sev = str(row.get("Severidade", "")).lower()
            color = sev_colors_map.get(sev, "")
            return [f"background-color: {color}"] * len(row)

        st.dataframe(
            alert_display.style.apply(color_alert_row, axis=1),
            use_container_width=True, height=500,
        )


# =========================================================================
# PAGE 4 — Performance do CSM
# =========================================================================
elif page == "Performance CSM":
    st.markdown(header("Performance do CSM", "Acompanhe calls, metas e cobertura de carteira."), unsafe_allow_html=True)

    with st.sidebar:
        st.markdown("### Filtros")
        csm_perf_list = sorted(csm_activity["csm_owner"].dropna().unique().tolist())
        csm_perf = st.selectbox("CSM", csm_perf_list, key="perf_csm")
        weeks = sorted(csm_activity["week_start"].unique())
        if len(weeks) > 0:
            period = st.date_input(
                "Periodo",
                value=(pd.Timestamp(weeks[-1]) - pd.Timedelta(weeks=11), pd.Timestamp(weeks[-1])),
                key="perf_period",
            )
        else:
            period = None

    csm_data = csm_activity[csm_activity["csm_owner"] == csm_perf].copy()
    if period and len(period) == 2:
        csm_data = csm_data[
            (csm_data["week_start"] >= pd.Timestamp(period[0]))
            & (csm_data["week_start"] <= pd.Timestamp(period[1]))
        ]
    csm_data = csm_data.sort_values("week_start")

    TARGET_CALLS = 20
    TARGET_SQLS = 3
    latest_week_calls = csm_data["calls_total"].iloc[-1] if len(csm_data) else 0
    calls_pct = latest_week_calls / TARGET_CALLS * 100 if TARGET_CALLS else 0

    csm_cov = coverage[coverage["csm_owner"] == csm_perf].sort_values("year_month")
    latest_sqls = csm_cov["sqls_identified"].iloc[-1] if len(csm_cov) else 0
    sqls_pct = latest_sqls / TARGET_SQLS * 100 if TARGET_SQLS else 0

    c1, c2, c3, c4 = st.columns(4)
    with c1:
        st.markdown(kpi("CALLS SEMANA", int(latest_week_calls), "&#128222;", BLUE_PRIMARY, f"Meta: {TARGET_CALLS}"), unsafe_allow_html=True)
    with c2:
        color_calls = GREEN if calls_pct >= 100 else (YELLOW if calls_pct >= 70 else RED)
        st.markdown(kpi("% META CALLS", f"{calls_pct:.0f}%", "&#127919;", color_calls), unsafe_allow_html=True)
    with c3:
        st.markdown(kpi("SQLS MES", int(latest_sqls), "&#128640;", PURPLE, f"Meta: {TARGET_SQLS}"), unsafe_allow_html=True)
    with c4:
        color_sqls = GREEN if sqls_pct >= 100 else (YELLOW if sqls_pct >= 70 else RED)
        st.markdown(kpi("% META SQLS", f"{sqls_pct:.0f}%", "&#127919;", color_sqls), unsafe_allow_html=True)

    st.markdown("<div style='height:16px'></div>", unsafe_allow_html=True)

    # Calls per week chart
    st.subheader("Calls por Semana")
    last_12 = csm_data.tail(12)
    fig_calls = go.Figure()
    fig_calls.add_trace(go.Bar(
        x=last_12["week_start"], y=last_12["calls_total"],
        marker_color=BLUE_PRIMARY, name="Calls",
    ))
    fig_calls.add_hline(y=TARGET_CALLS, line_dash="dash", line_color=RED,
                        annotation_text=f"Meta ({TARGET_CALLS})", annotation_position="top left")
    fig_calls.update_layout(**PLOTLY_LAYOUT, height=360, yaxis_title="Calls")
    st.plotly_chart(fig_calls, use_container_width=True)

    # Coverage
    st.subheader("Cobertura de Carteira")
    if len(csm_cov) > 0:
        latest_cov = csm_cov.iloc[-1]
        cov_data = {
            "Tipo": ["Com CS", "Sem CS"],
            "Total": [int(latest_cov.get("clients_with_cs_total", 0)), int(latest_cov.get("clients_without_cs_total", 0))],
            "Contactados": [int(latest_cov.get("clients_with_cs_contacted", 0)), int(latest_cov.get("clients_without_cs_contacted", 0))],
            "Cobertura %": [latest_cov.get("coverage_with_cs_pct", 0), latest_cov.get("coverage_without_cs_pct", 0)],
        }
        cov_df = pd.DataFrame(cov_data)
        st.dataframe(cov_df, use_container_width=True, hide_index=True)
        for _, r in cov_df.iterrows():
            pct = (r["Cobertura %"] or 0) / 100
            st.progress(min(pct, 1.0), text=f"{r['Tipo']}: {r['Cobertura %']:.0f}%")
    else:
        st.info("Sem dados de cobertura para este CSM.")


# =========================================================================
# PAGE 5 — Receita (GRR / NRR)
# =========================================================================
elif page == "Receita (GRR/NRR)":
    st.markdown(header("Receita — GRR & NRR", "Acompanhe retencao e expansao de receita."), unsafe_allow_html=True)

    rev = revenue.sort_values("year_month").copy()

    if len(rev) == 0:
        st.warning("Sem dados de receita.")
    else:
        latest = rev.iloc[-1]
        grr_current = latest["grr"] * 100
        nrr_current = latest["nrr"] * 100
        TARGET_GRR = 87
        TARGET_NRR = 105

        c1, c2, c3, c4 = st.columns(4)
        with c1:
            st.markdown(kpi("GRR ATUAL", f"{grr_current:.1f}%", "&#128176;", GREEN if grr_current >= TARGET_GRR else RED), unsafe_allow_html=True)
        with c2:
            st.markdown(kpi("META GRR", f"{TARGET_GRR}%", "&#127919;", BLUE_PRIMARY), unsafe_allow_html=True)
        with c3:
            st.markdown(kpi("NRR ATUAL", f"{nrr_current:.1f}%", "&#128200;", GREEN if nrr_current >= TARGET_NRR else YELLOW), unsafe_allow_html=True)
        with c4:
            st.markdown(kpi("META NRR", f"{TARGET_NRR}%", "&#127919;", BLUE_PRIMARY), unsafe_allow_html=True)

        st.markdown("<div style='height:16px'></div>", unsafe_allow_html=True)

        # GRR & NRR evolution
        st.subheader("Evolucao GRR e NRR")
        rev_6 = rev.tail(6)
        fig_rev = go.Figure()
        fig_rev.add_trace(go.Scatter(
            x=rev_6["year_month"], y=rev_6["grr"] * 100,
            mode="lines+markers", name="GRR",
            line=dict(color=GREEN, width=3), marker=dict(size=8),
        ))
        fig_rev.add_trace(go.Scatter(
            x=rev_6["year_month"], y=rev_6["nrr"] * 100,
            mode="lines+markers", name="NRR",
            line=dict(color=BLUE_PRIMARY, width=3), marker=dict(size=8),
        ))
        fig_rev.add_hline(y=TARGET_GRR, line_dash="dash", line_color="rgba(34,197,94,0.6)", annotation_text=f"Meta GRR ({TARGET_GRR}%)")
        fig_rev.add_hline(y=TARGET_NRR, line_dash="dash", line_color="rgba(37,99,235,0.6)", annotation_text=f"Meta NRR ({TARGET_NRR}%)")
        fig_rev.update_layout(**PLOTLY_LAYOUT, height=380, yaxis_title="%")
        st.plotly_chart(fig_rev, use_container_width=True)
    
        # Revenue table
        st.subheader("Movimentacoes de Receita por Mes")
        rev_table = rev[
            ["year_month", "mrr_churn", "mrr_downgrade", "mrr_upsell", "mrr_cross_sell", "mrr_new"]
        ].rename(columns={
            "year_month": "Mes", "mrr_churn": "Churn", "mrr_downgrade": "Downgrade",
            "mrr_upsell": "Upsell", "mrr_cross_sell": "Cross-sell", "mrr_new": "Novo",
        }).reset_index(drop=True)
        for col in ["Churn", "Downgrade", "Upsell", "Cross-sell", "Novo"]:
            rev_table[col] = rev_table[col].apply(lambda x: f"R$ {x:,.2f}" if pd.notna(x) else "R$ 0.00")
        st.dataframe(rev_table, use_container_width=True, hide_index=True)
    
        # Waterfall
        st.subheader("Waterfall MRR — Ultimo Mes")
        fig_wf = go.Figure(go.Waterfall(
            name="MRR", orientation="v",
            measure=["absolute", "relative", "relative", "relative", "relative", "relative", "total"],
            x=["MRR Inicio", "Novo", "Upsell", "Cross-sell", "Churn", "Downgrade", "MRR Final"],
            y=[
                latest["mrr_start"], latest["mrr_new"], latest["mrr_upsell"],
                latest["mrr_cross_sell"], -abs(latest["mrr_churn"]),
                -abs(latest["mrr_downgrade"]), latest["mrr_end"],
            ],
            connector=dict(line=dict(color="#CBD5E1")),
            increasing=dict(marker=dict(color=GREEN)),
            decreasing=dict(marker=dict(color=RED)),
            totals=dict(marker=dict(color=BLUE_PRIMARY)),
            textposition="outside",
            text=[
                f"R$ {latest['mrr_start']:,.0f}", f"+R$ {latest['mrr_new']:,.0f}",
                f"+R$ {latest['mrr_upsell']:,.0f}", f"+R$ {latest['mrr_cross_sell']:,.0f}",
                f"-R$ {abs(latest['mrr_churn']):,.0f}", f"-R$ {abs(latest['mrr_downgrade']):,.0f}",
                f"R$ {latest['mrr_end']:,.0f}",
            ],
        ))
        fig_wf.update_layout(**PLOTLY_LAYOUT, height=420, yaxis_title="MRR (R$)")
        st.plotly_chart(fig_wf, use_container_width=True)
    

# =========================================================================
# PAGE 6 — NPS
# =========================================================================
elif page == "NPS":
    st.markdown(header("NPS — Net Promoter Score", "Acompanhe a satisfacao dos clientes."), unsafe_allow_html=True)

    nps_data = nps.merge(
        clients[["hubspot_company_id", "company_name", "segment_ipc", "csm_owner"]],
        on="hubspot_company_id", how="left",
    )

    total_responses = len(nps_data)
    promoters = nps_data[nps_data["nps_category"] == "promoter"]
    passives = nps_data[nps_data["nps_category"] == "passive"]
    detractors = nps_data[nps_data["nps_category"] == "detractor"]
    promoters_n, passives_n, detractors_n = len(promoters), len(passives), len(detractors)
    nps_score_val = ((promoters_n - detractors_n) / total_responses * 100) if total_responses > 0 else 0

    c1, c2, c3, c4 = st.columns(4)
    nps_color = GREEN if nps_score_val >= 50 else (YELLOW if nps_score_val >= 0 else RED)
    with c1:
        st.markdown(kpi("NPS SCORE", f"{nps_score_val:.0f}", "&#11088;", nps_color), unsafe_allow_html=True)
    with c2:
        st.markdown(kpi("PROMOTORES", promoters_n, "&#128077;", GREEN), unsafe_allow_html=True)
    with c3:
        st.markdown(kpi("NEUTROS", passives_n, "&#128528;", YELLOW), unsafe_allow_html=True)
    with c4:
        st.markdown(kpi("DETRATORES", detractors_n, "&#128078;", RED), unsafe_allow_html=True)

    st.markdown("<div style='height:16px'></div>", unsafe_allow_html=True)

    # NPS by segment
    st.subheader("Distribuicao NPS por Segmento")
    if total_responses > 0:
        seg_nps = nps_data.groupby(["segment_ipc", "nps_category"]).size().reset_index(name="count")
        cat_colors = {"promoter": GREEN, "passive": YELLOW, "detractor": RED}
        fig_nps = go.Figure()
        for cat in ["promoter", "passive", "detractor"]:
            subset = seg_nps[seg_nps["nps_category"] == cat]
            fig_nps.add_trace(go.Bar(
                x=subset["segment_ipc"], y=subset["count"],
                name=cat.capitalize(), marker_color=cat_colors[cat],
            ))
        fig_nps.update_layout(**PLOTLY_LAYOUT, barmode="group", height=380, yaxis_title="Respostas")
        st.plotly_chart(fig_nps, use_container_width=True)
    else:
        st.info("Sem dados de NPS.")

    # Detractors table
    st.subheader("Detratores — Plano de Acao")
    if detractors_n > 0:
        det_display = (
            nps_data[nps_data["nps_category"] == "detractor"]
            .sort_values("nps_score")
            [["company_name", "segment_ipc", "nps_score", "response_date", "csm_owner"]]
            .rename(columns={
                "company_name": "Empresa", "segment_ipc": "Segmento",
                "nps_score": "NPS", "response_date": "Data Resposta", "csm_owner": "CSM",
            })
        ).reset_index(drop=True)
        det_display.index = det_display.index + 1
        det_display["Data Resposta"] = pd.to_datetime(det_display["Data Resposta"]).dt.strftime("%Y-%m-%d")

        det_clients = nps_data[nps_data["nps_category"] == "detractor"]["hubspot_company_id"].tolist()
        det_client_ids = clients[clients["hubspot_company_id"].isin(det_clients)]["client_id"].tolist()
        det_alerts = alerts[(alerts["client_id"].isin(det_client_ids)) & (alerts["resolved"] == 0)]
        st.caption(f"{len(det_alerts)} alertas ativos associados a detratores")
        st.dataframe(det_display, use_container_width=True, height=400)
    else:
        st.success("Nenhum detrator identificado.")


# =========================================================================
# PAGE 7 — Upsell
# =========================================================================
elif page == "Upsell":
    st.markdown(header("Oportunidades de Upsell", "Identifique clientes com potencial de expansao de produto."), unsafe_allow_html=True)

    # Monta base com canais de campanha
    _up = client_health.merge(
        campaign_channels.rename(columns={"revi_client_id": "client_id"}),
        on="client_id", how="left"
    )
    _up["has_sms"]   = _up["has_sms"].fillna(0).astype(bool)
    _up["has_email"] = _up["has_email"].fillna(0).astype(bool)

    _total = len(_up)

    # Definicao dos produtos e suas flags
    UPSELL_PRODUCTS = [
        ("Fluxo de Atendimento",      "has_chat_flow",          lambda r: bool(r.get("has_chat_flow"))),
        ("SMS",                        "has_sms",                lambda r: bool(r.get("has_sms"))),
        ("Email",                      "has_email",              lambda r: bool(r.get("has_email"))),
        ("Agente de IA",               "has_ai_enabled",         lambda r: bool(r.get("has_ai_enabled"))),
        ("Automacao de Integracao 2+", "integration_automations",lambda r: int(r.get("integration_automations") or 0) >= 2),
        ("Automacao de Campanhas",     "active_automations",     lambda r: int(r.get("active_automations") or 0) > 0),
        ("Cashback Ativo",             "cashback_enabled",       lambda r: bool(r.get("cashback_enabled"))),
    ]

    # Calcula quem tem e quem nao tem cada produto
    _product_stats = []
    for name, _, check_fn in UPSELL_PRODUCTS:
        has_mask = _up.apply(check_fn, axis=1)
        _product_stats.append({
            "name": name,
            "tem": has_mask.sum(),
            "nao_tem": (~has_mask).sum(),
            "mask_nao_tem": ~has_mask,
        })

    # ---- Dashboard de adocao ----
    st.markdown("#### Adocao por Produto")
    _adoption_cards = ""
    for ps in _product_stats:
        pct = ps["tem"] / _total * 100 if _total else 0
        color = GREEN if pct >= 70 else (YELLOW if pct >= 40 else RED)
        _adoption_cards += f"""
        <div style="background:#fff;border-radius:12px;padding:16px 18px;
                    box-shadow:0 1px 3px rgba(0,0,0,0.07);flex:1;min-width:0;">
            <div style="font-size:0.72rem;font-weight:600;text-transform:uppercase;
                        letter-spacing:0.4px;color:#64748B;margin-bottom:6px;
                        white-space:nowrap;overflow:hidden;text-overflow:ellipsis;"
                 title="{ps['name']}">{ps['name']}</div>
            <div style="font-size:1.6rem;font-weight:700;color:#1E293B;line-height:1;">{ps['tem']}</div>
            <div style="font-size:0.75rem;color:{color};font-weight:600;margin:4px 0 8px;">{pct:.0f}% da base</div>
            <div style="background:#E2E8F0;border-radius:4px;height:6px;">
                <div style="background:{color};width:{min(pct,100):.0f}%;height:6px;border-radius:4px;"></div>
            </div>
        </div>"""
    st.markdown(
        f'<div style="display:flex;gap:12px;margin-bottom:16px;">{_adoption_cards}</div>',
        unsafe_allow_html=True,
    )

    st.markdown("<div style='height:8px'></div>", unsafe_allow_html=True)

    # Grafico de barras: adocao vs oportunidade
    fig_up = go.Figure()
    fig_up.add_trace(go.Bar(
        name="Tem o produto",
        x=[ps["name"] for ps in _product_stats],
        y=[ps["tem"] for ps in _product_stats],
        marker_color=BLUE_PRIMARY,
        text=[ps["tem"] for ps in _product_stats],
        textposition="inside",
    ))
    fig_up.add_trace(go.Bar(
        name="Sem o produto (oportunidade)",
        x=[ps["name"] for ps in _product_stats],
        y=[ps["nao_tem"] for ps in _product_stats],
        marker_color=ORANGE_CTA,
        text=[ps["nao_tem"] for ps in _product_stats],
        textposition="inside",
    ))
    fig_up.update_layout(**PLOTLY_LAYOUT, barmode="stack", height=320,
                         yaxis_title="Clientes", showlegend=True,
                         legend=dict(orientation="h", y=-0.2))
    st.plotly_chart(fig_up, use_container_width=True)

    st.markdown("---")

    # ---- Filtro de produto + CSM ----
    with st.sidebar:
        st.markdown("### Filtros")
        _up_csm_list = sorted(_up["csm_owner"].dropna().unique().tolist())
        _up_csm = st.selectbox("CSM", ["Todos"] + _up_csm_list, key="up_csm")
        _up_status = st.selectbox("Status", ["Todos", "Campeao", "Alerta", "Em Risco"], key="up_status")

    _status_map = {"Campeao": "green", "Alerta": "yellow", "Em Risco": "red"}
    _up_filtered = _up.copy()
    if _up_csm != "Todos":
        _up_filtered = _up_filtered[_up_filtered["csm_owner"] == _up_csm]
    if _up_status != "Todos":
        _up_filtered = _up_filtered[_up_filtered["health_status"] == _status_map[_up_status]]

    # ---- Oportunidades por produto ----
    st.markdown("#### Oportunidades por Produto")
    st.caption("Clientes que ainda nao utilizam cada produto — potencial de expansao.")

    _TABLE_COLS = {
        "company_name": "Empresa",
        "csm_owner": "CSM",
        "segment_ipc": "Segmento",
        "total_score": "Score",
        "health_status": "Status",
        "mrr": "MRR (R$)",
        "days_to_renewal": "Renovacao (dias)",
    }

    for ps in _product_stats:
        _opp = _up_filtered[ps["mask_nao_tem"].reindex(_up_filtered.index, fill_value=False)]
        _count = len(_opp)
        if _count == 0:
            continue

        with st.expander(f"**{ps['name']}** — {_count} cliente(s) sem o produto", expanded=False):
            _tbl = _opp[list(_TABLE_COLS.keys())].rename(columns=_TABLE_COLS).copy()
            _tbl["Status"] = _tbl["Status"].map(STATUS_LABELS).fillna(_tbl["Status"])
            _tbl["MRR (R$)"] = _tbl["MRR (R$)"].apply(lambda x: f"R$ {x:,.0f}" if pd.notna(x) else "-")
            _tbl = _tbl.sort_values("Score").reset_index(drop=True)
            _tbl.index = _tbl.index + 1

            def _color_upsell(row):
                s = str(row.get("Status", "")).lower()
                if "campeao" in s: return ["background-color:#DCFCE7"] * len(row)
                if "alerta"  in s: return ["background-color:#FEF9C3"] * len(row)
                if "risco"   in s: return ["background-color:#FEE2E2"] * len(row)
                return [""] * len(row)

            st.dataframe(_tbl.style.apply(_color_upsell, axis=1), use_container_width=True)

# =========================================================================
# PAGE 8 — Admin (so para admins)
# =========================================================================
elif page == "Admin":
    if not user.get("is_admin"):
        st.error("Acesso restrito a administradores.")
        st.stop()
    admin_page()
