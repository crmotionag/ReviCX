"""
ReviCX Health Score Dashboard
Streamlit application with ReviCX brand identity.
"""

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from pathlib import Path
from sqlalchemy import create_engine, text
from sqlalchemy.pool import NullPool
from datetime import datetime
import hashlib
import os
import random
import json
import requests as _requests

# Carrega variaveis: st.secrets (Streamlit Cloud) > .env (local)
_SECRETS_KEYS = ["HUBSPOT_API_KEY", "JIRA_URL", "JIRA_EMAIL", "JIRA_API_TOKEN", "JIRA_PROJECT_KEY", "JIRA_CLOUD_ID"]
try:
    for _k in _SECRETS_KEYS:
        if _k in st.secrets:
            os.environ.setdefault(_k, st.secrets[_k])
except Exception:
    pass

_ENV_PATH = Path(__file__).parent.parent / ".env"
try:
    if _ENV_PATH.exists():
        for _line in _ENV_PATH.read_text(encoding="utf-8").splitlines():
            _line = _line.strip()
            if _line and not _line.startswith("#") and "=" in _line:
                _k, _v = _line.split("=", 1)
                os.environ.setdefault(_k.strip(), _v.strip())
except (PermissionError, OSError):
    pass

JIRA_URL        = os.environ.get("JIRA_URL", "https://userevi.atlassian.net")
JIRA_EMAIL      = os.environ.get("JIRA_EMAIL", "")
JIRA_API_TOKEN  = os.environ.get("JIRA_API_TOKEN", "")
JIRA_PROJECT    = os.environ.get("JIRA_PROJECT_KEY", "DEV")
JIRA_CLOUD_ID   = os.environ.get("JIRA_CLOUD_ID", "bac96881-c73c-48dc-aca3-2a3358dc7279")

# ---------------------------------------------------------------------------
# Page config
# ---------------------------------------------------------------------------
st.set_page_config(
    page_title="ReviCX Health Score",
    page_icon=str(Path(__file__).parent / "assets" / "favicon.ico"),
    layout="wide",
    initial_sidebar_state="collapsed",
)

# ---------------------------------------------------------------------------
# ReviCX — Design Tokens
# ---------------------------------------------------------------------------
BLUE_DARK = "#1A3A6B"
BLUE_PRIMARY = "#2563EB"
BLUE_LIGHT = "#DBEAFE"
REVI_GREEN = "#3DB549"
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
GREY = "#94a3b8"

STATUS_COLORS = {"green": GREEN, "yellow": YELLOW, "red": RED, "inactive": GREY}
STATUS_LABELS = {"green": "Campeao", "yellow": "Alerta", "red": "Em Risco", "inactive": "Inativo"}
STATUS_ICONS = {"green": "&#9679;", "yellow": "&#9679;", "red": "&#9679;", "inactive": "&#9679;"}

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
    .stTextInput > div > div,
    .stTextArea > div > div {{
        background: {BG_CARD} !important;
        border: 1px solid #CBD5E1 !important;
        border-radius: 8px !important;
        color: #000000 !important;
    }}
    /* Texto dentro das caixas (select, date, textarea) — preto sobre fundo branco */
    .stSelectbox [data-baseweb="select"] *,
    .stSelectbox input,
    .stDateInput input,
    .stDateInput [data-baseweb="base-input"] *,
    .stTextInput input,
    .stTextArea textarea {{
        background: {BG_CARD} !important;
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

    /* ---- Botoes primary: cor Revi ---- */
    [data-testid="stFormSubmitButton"] button,
    .stButton > button[kind="primary"],
    button[kind="primary"] {{
        background-color: {REVI_GREEN} !important;
        border-color: {REVI_GREEN} !important;
        color: #ffffff !important;
    }}
    [data-testid="stFormSubmitButton"] button:hover,
    .stButton > button[kind="primary"]:hover,
    button[kind="primary"]:hover {{
        background-color: #319f3e !important;
        border-color: #319f3e !important;
    }}
    [data-testid="stFormSubmitButton"] button p,
    [data-testid="stFormSubmitButton"] button span,
    .stButton > button[kind="primary"] p,
    .stButton > button[kind="primary"] span {{
        color: #ffffff !important;
    }}

    /* ---- Login header: textos brancos (protege contra regra global) ---- */
    .login-header p,
    .login-header span,
    .login-header div {{
        color: #ffffff !important;
    }}
</style>
""", unsafe_allow_html=True)


# ---------------------------------------------------------------------------
# Auth helpers
# ---------------------------------------------------------------------------
DB_PATH = Path(__file__).parent.parent / "data" / "revi_cs.db"

AREAS = ["Customer Success", "CX", "Suporte", "Produto", "Comercial", "Marketing", "Diretoria"]
ROLES = ["Gerente", "Supervisor", "Analista", "Assistente"]

USE_NEKT = os.environ.get("USE_NEKT", "").strip().lower() in {"1", "true", "yes"}

MODULES = [
    "Visao Geral",
    "Carteira do CSM",
    "Upsell",
    "Abrir Ticket",
    "Alertas",
    "Performance CSM",
    "Receita (GRR/NRR)",
]
# NPS module only appears on the SQLite mock (Nekt has no NPS source today).
if not USE_NEKT:
    MODULES.append("NPS")


@st.cache_resource
def get_engine():
    """
    Retorna o engine SQLAlchemy para app_users, cacheado entre reruns.

    Para persistencia no Streamlit Cloud, defina USERS_DATABASE_URL nos
    Streamlit Secrets. Sem isso, os usuarios sao perdidos a cada restart.

    Exemplo em .streamlit/secrets.toml (usar Supabase, Neon ou similar):
        USERS_DATABASE_URL = "postgresql://user:pass@host:5432/dbname"
    """
    import sys
    from urllib.parse import urlparse as _up

    _url = None
    try:
        _url = st.secrets.get("USERS_DATABASE_URL")
    except Exception:
        pass
    _url = _url or os.environ.get("USERS_DATABASE_URL") or f"sqlite:///{DB_PATH}"

    # SQLAlchemy 2.0 nao aceita "postgres://"; converte para "postgresql://"
    if isinstance(_url, str) and _url.startswith("postgres://"):
        _url = "postgresql://" + _url[len("postgres://"):]

    _is_sqlite = isinstance(_url, str) and "sqlite" in _url

    # URL segura para logs (sem senha)
    try:
        _p = _up(_url)
        _safe_url = f"{_p.scheme}://*****@{_p.hostname}:{_p.port}{_p.path}"
    except Exception:
        _safe_url = "(URL invalida)"

    _kwargs = {}
    if _is_sqlite:
        _kwargs["connect_args"] = {"check_same_thread": False}
        _kwargs["poolclass"] = NullPool
    else:
        # PostgreSQL gerenciado: timeout + SSL
        _ca: dict = {"connect_timeout": 15}
        if "sslmode" not in _url:
            _ca["sslmode"] = "require"
        _kwargs["connect_args"] = _ca

    engine = create_engine(_url, **_kwargs)

    # Retry para bancos serverless (ex: Neon dorme apos inatividade e a 1a conexao pode falhar)
    _last_exc: Exception | None = None
    for _attempt in range(3):
        try:
            with engine.begin() as conn:
                if _is_sqlite:
                    conn.execute(text("""
                        CREATE TABLE IF NOT EXISTS app_users (
                            id INTEGER PRIMARY KEY AUTOINCREMENT,
                            name TEXT NOT NULL,
                            email TEXT NOT NULL UNIQUE,
                            password_hash TEXT NOT NULL,
                            area TEXT NOT NULL,
                            role TEXT NOT NULL,
                            is_active BOOLEAN DEFAULT 1,
                            is_admin BOOLEAN DEFAULT 0,
                            must_change_password BOOLEAN DEFAULT 0,
                            modules TEXT DEFAULT 'all',
                            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                            last_login DATETIME
                        )
                    """))
                    existing = {row[1] for row in conn.execute(text("PRAGMA table_info(app_users)")).fetchall()}
                    if "must_change_password" not in existing:
                        conn.execute(text("ALTER TABLE app_users ADD COLUMN must_change_password BOOLEAN DEFAULT 0"))
                    if "modules" not in existing:
                        conn.execute(text("ALTER TABLE app_users ADD COLUMN modules TEXT DEFAULT 'all'"))
                else:
                    conn.execute(text("""
                        CREATE TABLE IF NOT EXISTS app_users (
                            id SERIAL PRIMARY KEY,
                            name TEXT NOT NULL,
                            email TEXT NOT NULL UNIQUE,
                            password_hash TEXT NOT NULL,
                            area TEXT NOT NULL,
                            role TEXT NOT NULL,
                            is_active BOOLEAN DEFAULT TRUE,
                            is_admin BOOLEAN DEFAULT FALSE,
                            must_change_password BOOLEAN DEFAULT FALSE,
                            modules TEXT DEFAULT 'all',
                            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                            last_login TIMESTAMP
                        )
                    """))
            break  # conexao ok
        except Exception as _e:
            _last_exc = _e
            print(f"[get_engine] tentativa {_attempt + 1}/3 falhou: {type(_e).__name__}: {_e}", file=sys.stderr)
            if _attempt < 2:
                import time as _t
                _t.sleep(4)
    else:
        # Todas as 3 tentativas falharam
        raise RuntimeError(
            f"Falha ao conectar ao banco de dados apos 3 tentativas.\n"
            f"Host: {_safe_url}\n"
            f"Erro: {type(_last_exc).__name__}: {_last_exc}"
        ) from _last_exc

    # Auto-bootstrap: se nao houver nenhum admin, cria o padrao automaticamente
    # (necessario em banco externo novo ou apos reset do SQLite)
    with engine.connect() as conn:
        _admin_count = conn.execute(
            text("SELECT COUNT(*) FROM app_users WHERE is_admin = TRUE")
        ).scalar()
    if _admin_count == 0:
        import hashlib as _hl
        _default_pw = _hl.sha256(b"revi2026").hexdigest()
        try:
            with engine.begin() as conn:
                conn.execute(text("""
                    INSERT INTO app_users
                        (name, email, password_hash, area, role, is_active, is_admin, must_change_password)
                    VALUES
                        ('Admin', 'admin@revi.com', :pwd,
                         'Customer Success', 'Gerente', TRUE, TRUE, TRUE)
                """), {"pwd": _default_pw})
        except Exception:
            pass  # Admin ja existe (UNIQUE constraint)

    return engine


def get_user_modules(user: dict) -> list[str]:
    """Retorna lista de modulos permitidos para o usuario."""
    if user.get("is_admin"):
        return MODULES + ["Admin"]
    raw = user.get("modules", "all")
    if raw == "all" or not raw:
        return MODULES
    try:
        return json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        return MODULES


def gen_temp_password() -> str:
    return "revi" + str(random.randint(1000, 9999))


def hash_pw(pw):
    return hashlib.sha256(pw.encode()).hexdigest()


def authenticate(email, password):
    engine = get_engine()
    with engine.connect() as conn:
        row = conn.execute(
            text("SELECT * FROM app_users WHERE email = :e AND is_active = TRUE"),
            {"e": email.strip().lower()},
        ).mappings().first()
    if row and row["password_hash"] == hash_pw(password):
        return dict(row)
    return None


def init_auth():
    if "authenticated" not in st.session_state:
        st.session_state.authenticated = False
        st.session_state.user = None
        st.session_state.must_change_pw = False


def login_page():
    """Tela de login com KV ReviCX."""
    import base64 as _b64
    _favicon_login = Path(__file__).parent / "assets" / "favicon.ico"
    _favicon_b64 = _b64.b64encode(_favicon_login.read_bytes()).decode()

    if "login_error" not in st.session_state:
        st.session_state.login_error = False

    # Espaço para centralizar verticalmente
    st.markdown("<div style='margin-top:8vh;'></div>", unsafe_allow_html=True)

    col_spacer1, col_form, col_spacer2 = st.columns([1, 1.2, 1])
    with col_form:
        # Logo + título dentro da coluna do form
        st.markdown(f"""
        <div class="login-header" style="background:linear-gradient(135deg,{BLUE_DARK},{BLUE_PRIMARY});
                    padding:32px 40px 24px 40px; border-radius:16px 16px 0 0;
                    text-align:center; box-shadow:0 4px 16px rgba(26,58,107,0.2);">
            <img src="data:image/x-icon;base64,{_favicon_b64}" alt="ReviCX" style="max-height:95px; margin-bottom:8px;">
            <p style="color:#ffffff; font-size:0.88rem; margin:0;">
                Ferramenta de gestão de clientes Revi
            </p>
        </div>
        """, unsafe_allow_html=True)

        with st.form("login_form"):
            st.markdown(f"<p style='text-align:center;color:{TEXT_MUTED};font-size:0.85rem;margin-top:4px;'>Entre com suas credenciais</p>", unsafe_allow_html=True)
            email = st.text_input("Email", placeholder="seu@email.com")
            password = st.text_input("Senha", type="password", placeholder="********")
            submitted = st.form_submit_button("Entrar", use_container_width=True, type="primary")

            if submitted:
                try:
                    user = authenticate(email, password)
                except RuntimeError as _db_err:
                    st.error(f"Erro de conexao com o banco de dados:\n\n{_db_err}")
                    st.stop()
                except Exception as _db_err:
                    st.error(f"Erro inesperado ({type(_db_err).__name__}): {_db_err}")
                    st.stop()
                if user:
                    st.session_state.authenticated = True
                    st.session_state.user = user
                    st.session_state.login_error = False
                    st.session_state.must_change_pw = bool(user.get("must_change_password", 0))
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

    # Store credentials card info outside form so it persists after submit
    if "new_user_credentials" not in st.session_state:
        st.session_state.new_user_credentials = None

    with st.form("create_user_form", clear_on_submit=True):
        col1, col2 = st.columns(2)
        with col1:
            new_name = st.text_input("Nome completo *")
            new_email = st.text_input("Email *", placeholder="nome@empresa.com")
        with col2:
            new_area = st.selectbox("Area *", AREAS)
            new_role = st.selectbox("Cargo *", ROLES)
            new_admin = st.checkbox("Administrador?", help="Admins podem criar e gerenciar outros usuarios")

        st.markdown("**Modulos permitidos** (desmarque para restringir acesso)")
        mod_cols = st.columns(4)
        selected_modules = []
        for i, mod in enumerate(MODULES):
            with mod_cols[i % 4]:
                if st.checkbox(mod, value=True, key=f"new_mod_{mod}"):
                    selected_modules.append(mod)

        create_submitted = st.form_submit_button("Criar Usuario", use_container_width=True, type="primary")

        if create_submitted:
            errors = []
            if not new_name.strip():
                errors.append("Nome e obrigatorio")
            if not new_email.strip() or "@" not in new_email:
                errors.append("Email invalido")
            if errors:
                for e in errors:
                    st.error(e)
            else:
                temp_pw = gen_temp_password()
                modules_val = "all" if len(selected_modules) == len(MODULES) else json.dumps(selected_modules, ensure_ascii=False)
                try:
                    with engine.begin() as conn:
                        conn.execute(text("""
                            INSERT INTO app_users (name, email, password_hash, area, role, is_active, is_admin, must_change_password, modules)
                            VALUES (:name, :email, :pwd, :area, :role, TRUE, :admin, TRUE, :modules)
                        """), {
                            "name": new_name.strip(),
                            "email": new_email.strip().lower(),
                            "pwd": hash_pw(temp_pw),
                            "area": new_area,
                            "role": new_role,
                            "admin": True if new_admin else False,
                            "modules": modules_val,
                        })
                    st.session_state.new_user_credentials = {
                        "name": new_name.strip(),
                        "email": new_email.strip().lower(),
                        "password": temp_pw,
                        "modules": selected_modules if selected_modules != MODULES else ["Todos os modulos"],
                    }
                    st.rerun()
                except Exception as ex:
                    if "UNIQUE constraint" in str(ex):
                        st.error("Ja existe um usuario com esse email.")
                    else:
                        st.error(f"Erro: {ex}")

    # Credentials card displayed outside the form
    if st.session_state.new_user_credentials:
        creds = st.session_state.new_user_credentials
        mods_str = ", ".join(creds["modules"])
        whatsapp_text = (
            f"Ola {creds['name']}! Seu acesso ao ReviCX Health Score foi criado.\n"
            f"Email: {creds['email']}\n"
            f"Senha temporaria: {creds['password']}\n"
            f"Modulos: {mods_str}\n"
            f"Voce sera solicitado a trocar a senha no primeiro acesso."
        )
        st.markdown(f"""
        <div style="background:#d4edda;border:1px solid #28a745;border-radius:8px;padding:20px;margin:12px 0;">
            <h4 style="color:#155724;margin:0 0 12px 0;">&#9989; Usuario criado com sucesso!</h4>
            <p style="margin:4px 0;"><strong>Nome:</strong> {creds['name']}</p>
            <p style="margin:4px 0;"><strong>Email:</strong> {creds['email']}</p>
            <p style="margin:4px 0;"><strong>Senha temporaria:</strong> <code style="background:#c3e6cb;padding:2px 6px;border-radius:4px;">{creds['password']}</code></p>
            <p style="margin:4px 0;"><strong>Modulos:</strong> {mods_str}</p>
        </div>
        """, unsafe_allow_html=True)

        col_copy, col_dismiss = st.columns([2, 1])
        with col_copy:
            st.code(whatsapp_text, language=None)
        with col_dismiss:
            if st.button("Fechar", key="dismiss_creds"):
                st.session_state.new_user_credentials = None
                st.rerun()

    # --- Lista de usuarios ---
    st.subheader("Usuarios Cadastrados")
    users_df = pd.read_sql(
        "SELECT id, name, email, area, role, is_active, is_admin, modules, must_change_password, created_at, last_login FROM app_users ORDER BY name",
        engine,
    )

    if len(users_df) == 0:
        st.info("Nenhum usuario cadastrado.")
        return

    def format_modules(raw):
        if raw == "all" or not raw:
            return "Todos"
        try:
            mods = json.loads(raw)
            return ", ".join(mods)
        except Exception:
            return str(raw)

    # Format display
    display = users_df.copy()
    display["Status"] = display["is_active"].map({1: "Ativo", 0: "Inativo"})
    display["Admin"] = display["is_admin"].map({1: "Sim", 0: "Nao"})
    display["Troca Senha"] = display["must_change_password"].map({1: "Pendente", 0: "-"})
    display["Modulos"] = display["modules"].apply(format_modules)
    display = display.rename(columns={
        "name": "Nome", "email": "Email", "area": "Area",
        "role": "Cargo", "created_at": "Criado em", "last_login": "Ultimo Login",
    })

    st.dataframe(
        display[["Nome", "Email", "Area", "Cargo", "Status", "Admin", "Modulos", "Troca Senha", "Criado em", "Ultimo Login"]],
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
                        conn.execute(text("UPDATE app_users SET is_active = FALSE WHERE id = :uid"), {"uid": int(sel_user["id"])})
                    st.success(f"{sel_user['name']} desativado.")
                    st.rerun()
            else:
                if st.button("Reativar", key=f"react_{sel_user['id']}", use_container_width=True, type="primary"):
                    with engine.begin() as conn:
                        conn.execute(text("UPDATE app_users SET is_active = TRUE WHERE id = :uid"), {"uid": int(sel_user["id"])})
                    st.success(f"{sel_user['name']} reativado.")
                    st.rerun()

        with col2:
            if not sel_user["is_admin"]:
                if st.button("Tornar Admin", key=f"admin_{sel_user['id']}", use_container_width=True):
                    with engine.begin() as conn:
                        conn.execute(text("UPDATE app_users SET is_admin = TRUE WHERE id = :uid"), {"uid": int(sel_user["id"])})
                    st.success(f"{sel_user['name']} agora e admin.")
                    st.rerun()
            else:
                if st.button("Remover Admin", key=f"rmadmin_{sel_user['id']}", use_container_width=True):
                    with engine.begin() as conn:
                        conn.execute(text("UPDATE app_users SET is_admin = FALSE WHERE id = :uid"), {"uid": int(sel_user["id"])})
                    st.success(f"{sel_user['name']} nao e mais admin.")
                    st.rerun()

        with col3:
            if st.button("Resetar Senha", key=f"reset_{sel_user['id']}", use_container_width=True):
                new_pw = gen_temp_password()
                with engine.begin() as conn:
                    conn.execute(
                        text("UPDATE app_users SET password_hash = :pwd, must_change_password = TRUE WHERE id = :uid"),
                        {"pwd": hash_pw(new_pw), "uid": int(sel_user["id"])},
                    )
                st.success(f"Senha de {sel_user['name']} resetada para: **{new_pw}** (usuario devera trocar no proximo login)")

        with col4:
            if int(sel_user["id"]) != st.session_state.user["id"]:
                if st.button("Excluir", key=f"del_{sel_user['id']}", use_container_width=True):
                    with engine.begin() as conn:
                        conn.execute(text("DELETE FROM app_users WHERE id = :uid"), {"uid": int(sel_user["id"])})
                    st.warning(f"{sel_user['name']} excluido.")
                    st.rerun()

        # --- Editar modulos ---
        st.markdown(f"**Modulos de acesso — {sel_user['name']}**")

        raw_mods = sel_user["modules"] if "modules" in sel_user.index else "all"
        if raw_mods is None or (isinstance(raw_mods, float)) or raw_mods == "all":
            current_mods = MODULES[:]
        else:
            try:
                current_mods = json.loads(raw_mods)
            except Exception:
                current_mods = MODULES[:]

        mod_edit_cols = st.columns(4)
        new_selected = []
        for i, mod in enumerate(MODULES):
            with mod_edit_cols[i % 4]:
                if st.checkbox(mod, value=(mod in current_mods), key=f"edit_mod_{sel_user['id']}_{mod}"):
                    new_selected.append(mod)

        if st.button("Salvar modulos", key=f"save_mods_{sel_user['id']}", type="primary"):
            new_modules_val = "all" if len(new_selected) == len(MODULES) else json.dumps(new_selected, ensure_ascii=False)
            with engine.begin() as conn:
                conn.execute(
                    text("UPDATE app_users SET modules = :m WHERE id = :uid"),
                    {"m": new_modules_val, "uid": int(sel_user["id"])},
                )
            st.success(f"Modulos de {sel_user['name']} atualizados.")
            st.rerun()


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------
@st.cache_data(ttl=60)
def load_data(period_days: int = 30, _cache_bust: str = "v1.1"):
    if USE_NEKT:
        return load_data_from_nekt()
    return load_data_from_sqlite(period_days)


def load_data_from_sqlite(period_days: int = 30):
    db_path = Path(__file__).parent.parent / "data" / "revi_cs.db"
    engine = create_engine(f"sqlite:///{db_path}")
    clients = pd.read_sql("SELECT * FROM dim_clients", engine)
    # fct_health_score has one row per (client, period_days) — pick the selected window.
    health = pd.read_sql(
        "SELECT * FROM fct_health_score WHERE period_days = ?",
        engine,
        params=(int(period_days),),
    )
    alerts = pd.read_sql("SELECT * FROM fct_alerts", engine)
    csm_activity = pd.read_sql("SELECT * FROM fct_csm_activity_weekly", engine)
    coverage = pd.read_sql("SELECT * FROM fct_coverage_monthly", engine)
    revenue = pd.read_sql("SELECT * FROM fct_revenue_retention_monthly", engine)
    nps = pd.read_sql("SELECT * FROM raw_hubspot_nps", engine)
    health["calculated_at"] = pd.to_datetime(health["calculated_at"])
    alerts["alert_date"] = pd.to_datetime(alerts["alert_date"])
    csm_activity["week_start"] = pd.to_datetime(csm_activity["week_start"])
    nps["response_date"] = pd.to_datetime(nps["response_date"])
    campaign_channels = pd.read_sql("""
        SELECT revi_client_id,
               MAX(CASE WHEN campaign_type = 'sms'   THEN 1 ELSE 0 END) AS has_sms,
               MAX(CASE WHEN campaign_type = 'email' THEN 1 ELSE 0 END) AS has_email
        FROM raw_revi_campaigns
        GROUP BY revi_client_id
    """, engine)
    return clients, health, alerts, csm_activity, coverage, revenue, nps, campaign_channels


def _nekt_engine():
    """Athena SQLAlchemy engine. Requires AWS creds + ATHENA_S3_STAGING_DIR env vars."""
    from urllib.parse import quote_plus
    region  = os.environ.get("AWS_REGION", "us-east-1")
    staging = os.environ["ATHENA_S3_STAGING_DIR"]
    wg      = os.environ.get("ATHENA_WORKGROUP", "primary")
    key     = os.environ.get("AWS_ACCESS_KEY_ID", "")
    secret  = os.environ.get("AWS_SECRET_ACCESS_KEY", "")
    url = (
        f"awsathena+rest://{quote_plus(key)}:{quote_plus(secret)}"
        f"@athena.{region}.amazonaws.com:443/nekt_service"
        f"?s3_staging_dir={quote_plus(staging)}&work_group={quote_plus(wg)}"
    )
    return create_engine(url)


def load_data_from_nekt():
    """Reads the 7 nekt_service views and reshapes them to the SQLite schema
    so downstream pages work unchanged."""
    engine = _nekt_engine()
    dim        = pd.read_sql("SELECT * FROM nekt_service.dim_clients", engine)
    health     = pd.read_sql("SELECT * FROM nekt_service.fct_health_score", engine)
    alerts     = pd.read_sql("SELECT * FROM nekt_service.fct_alerts", engine)
    csm_wk     = pd.read_sql("SELECT * FROM nekt_service.fct_csm_activity_weekly", engine)
    cov_raw    = pd.read_sql("SELECT * FROM nekt_service.fct_coverage_monthly", engine)
    revenue    = pd.read_sql("SELECT * FROM nekt_service.fct_revenue_retention_monthly", engine)
    upsell     = pd.read_sql("SELECT * FROM nekt_service.fct_upsell_flags", engine)

    # --- dim_clients: pad cols missing in Nekt with defaults expected by pages ---
    clients = dim.copy()
    clients["has_cs"] = clients["has_customer_service"].fillna(False).astype(bool)
    clients["segment_ipc"]          = None                           # pending (see plan)
    clients["plan_type"]            = None                           # pending
    clients["onboarding_completed"] = clients["onboarding_end_date"].notna()
    clients["contract_end_date"]    = pd.to_datetime(clients["contract_start_date"]) + pd.DateOffset(years=1)
    clients["days_as_client"]       = (pd.Timestamp.today().normalize() -
                                       pd.to_datetime(clients["contract_start_date"])).dt.days
    clients = clients.merge(
        upsell[["client_id", "cashback_enabled"]], on="client_id", how="left"
    )
    clients["cashback_enabled"] = clients["cashback_enabled"].fillna(False).astype(bool)

    # --- dates & bool normalization ---
    health["calculated_at"] = pd.to_datetime(health["calculated_at"])
    alerts["alert_date"]    = pd.to_datetime(alerts["alert_date"])
    csm_wk["week_start"]    = pd.to_datetime(csm_wk["week_start"])
    # Athena returns bools; pages compare `resolved == 0` — map to int for compatibility.
    alerts["resolved"] = alerts["resolved"].fillna(False).astype(bool).astype(int)

    # --- csm_activity: pad call-type breakdown with 0 (pending — see plan) ---
    for col in ("calls_consultoria", "calls_onboarding", "calls_urgencia", "calls_follow_up"):
        csm_wk[col] = 0
    csm_wk["unique_clients_contacted"] = csm_wk["meetings_total"]  # best-effort proxy

    # --- coverage: aggregate granular (csm, month, slug) → (csm, year_month) buckets ---
    coverage = _reshape_coverage(cov_raw, clients)

    # --- nps: not in Nekt; return empty shape so legacy refs don't break ---
    nps = pd.DataFrame(columns=[
        "response_id", "hubspot_company_id", "contact_email",
        "nps_score", "nps_category", "response_date",
    ])
    nps["response_date"] = pd.to_datetime(nps["response_date"])

    # --- campaign_channels: derive from fct_upsell_flags ---
    campaign_channels = upsell[["client_id", "has_sms", "has_email"]].rename(
        columns={"client_id": "revi_client_id"}
    )
    campaign_channels["has_sms"]   = campaign_channels["has_sms"].astype(int)
    campaign_channels["has_email"] = campaign_channels["has_email"].astype(int)

    return clients, health, alerts, csm_wk, coverage, revenue, nps, campaign_channels


def _reshape_coverage(cov_raw: pd.DataFrame, clients: pd.DataFrame) -> pd.DataFrame:
    """Turn granular (csm, month, slug, meetings) into per-(csm, year_month) shape
    with with_cs / without_cs totals + contacted counts expected by Performance page."""
    if cov_raw.empty:
        return pd.DataFrame(columns=[
            "csm_owner", "year_month",
            "clients_with_cs_total", "clients_with_cs_contacted",
            "clients_without_cs_total", "clients_without_cs_contacted",
            "coverage_with_cs_pct", "coverage_without_cs_pct",
            "sqls_identified",
        ])

    clients_slim = clients[["csm_owner", "company_name", "has_cs"]].copy()
    clients_slim["slug"] = clients_slim["company_name"].str.lower().str.strip()

    # Join contacted slugs back to clients → has_cs flag
    contacted = cov_raw.merge(
        clients_slim, left_on=["csm_owner", "client_name_slug"],
        right_on=["csm_owner", "slug"], how="left",
    )
    contacted["has_cs"] = contacted["has_cs"].fillna(True)  # unknown → assume with_cs

    contacted_agg = contacted.groupby(["csm_owner", "year_month"]).agg(
        clients_with_cs_contacted   =("has_cs", lambda s: int(s.sum())),
        clients_without_cs_contacted=("has_cs", lambda s: int((~s.astype(bool)).sum())),
    ).reset_index()

    totals = clients_slim.groupby("csm_owner").agg(
        clients_with_cs_total   =("has_cs", lambda s: int(s.sum())),
        clients_without_cs_total=("has_cs", lambda s: int((~s.astype(bool)).sum())),
    ).reset_index()

    out = contacted_agg.merge(totals, on="csm_owner", how="left")
    out["coverage_with_cs_pct"]    = (out["clients_with_cs_contacted"]    / out["clients_with_cs_total"].replace(0, pd.NA)    * 100).round(1).fillna(0)
    out["coverage_without_cs_pct"] = (out["clients_without_cs_contacted"] / out["clients_without_cs_total"].replace(0, pd.NA) * 100).round(1).fillna(0)
    out["sqls_identified"] = 0  # pending — see plan
    return out


# ---------------------------------------------------------------------------
# Change password page
# ---------------------------------------------------------------------------
def change_password_page():
    """Tela de troca de senha obrigatoria no primeiro login."""
    st.markdown("<div style='margin-top:8vh;'></div>", unsafe_allow_html=True)
    col_spacer1, col_form, col_spacer2 = st.columns([1, 1.2, 1])
    with col_form:
        st.markdown(f"""
        <div style="background:linear-gradient(135deg,{BLUE_DARK},{BLUE_PRIMARY});
                    padding:32px 40px 24px 40px; border-radius:16px 16px 0 0;
                    text-align:center; box-shadow:0 4px 16px rgba(26,58,107,0.2);">
            <p style="color:#FFF; font-size:1.1rem; font-weight:600; margin:0;">Defina sua nova senha</p>
            <p style="color:rgba(255,255,255,0.75); font-size:0.85rem; margin:8px 0 0 0;">
                Por seguranca, troque a senha temporaria antes de continuar.
            </p>
        </div>
        """, unsafe_allow_html=True)

        with st.form("change_pw_form"):
            new_pw = st.text_input("Nova senha", type="password", placeholder="Min. 6 caracteres")
            confirm_pw = st.text_input("Confirmar nova senha", type="password")
            save = st.form_submit_button("Salvar senha", use_container_width=True, type="primary")

            if save:
                if len(new_pw) < 6:
                    st.error("A senha deve ter no minimo 6 caracteres.")
                elif new_pw != confirm_pw:
                    st.error("As senhas nao coincidem.")
                else:
                    engine = get_engine()
                    uid = st.session_state.user["id"]
                    with engine.begin() as conn:
                        conn.execute(
                            text("UPDATE app_users SET password_hash = :pwd, must_change_password = FALSE WHERE id = :uid"),
                            {"pwd": hash_pw(new_pw), "uid": uid},
                        )
                    st.session_state.must_change_pw = False
                    # Refresh user in session
                    with engine.connect() as conn:
                        row = conn.execute(text("SELECT * FROM app_users WHERE id = :uid"), {"uid": uid}).mappings().first()
                    if row:
                        st.session_state.user = dict(row)
                    st.rerun()


# ---------------------------------------------------------------------------
# AUTH GATE — tudo abaixo so roda se autenticado
# ---------------------------------------------------------------------------
init_auth()

if not st.session_state.authenticated:
    login_page()
    st.stop()

# Forcar troca de senha se necessario
if st.session_state.get("must_change_pw"):
    change_password_page()
    st.stop()

# --- Usuario autenticado daqui pra baixo ---
# Janela de analise de campanhas. Definida no sidebar via session_state.
period_days = st.session_state.get("period_days", 30)
clients, health, alerts, csm_activity, coverage, revenue, nps, campaign_channels = load_data(period_days)

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
def kpi(label, value, icon="&#128202;", color=BLUE_PRIMARY, sub="", tooltip=""):
    bg_light = color + "18"
    sub_part = f'<span class="kpi-sub">{sub}</span>' if sub else ""
    title_attr = f' title="{tooltip}"' if tooltip else ""
    cursor = ' style="cursor:help;"' if tooltip else ""
    return (
        f'<div class="kpi-card"{title_attr}{cursor}>'
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
    _logo_sidebar = Path(__file__).parent / "assets" / "revi-logo-white.png"
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

    # Menu — filtrado pelos modulos permitidos do usuario
    allowed = get_user_modules(user)
    menu_items = [m for m in MODULES if m in allowed]
    if user.get("is_admin") and "Admin" not in menu_items:
        menu_items.append("Admin")

    page = st.radio("Menu", menu_items, label_visibility="collapsed")

    st.markdown("---")
    st.selectbox(
        "Janela de analise (ROI)",
        options=[7, 30, 60, 90],
        index=1,
        key="period_days",
        format_func=lambda d: f"Ultimos {d} dias",
        help="Janela para o calculo de ROI (SUM receita / SUM custo) das campanhas.",
    )

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
    inactive_n = len(_ch[_ch["health_status"] == "inactive"])
    green_pct = f"{green_n/total*100:.0f}%" if total else "0%"
    yellow_pct = f"{yellow_n/total*100:.0f}%" if total else "0%"
    red_pct = f"{red_n/total*100:.0f}%" if total else "0%"
    inactive_pct = f"{inactive_n/total*100:.0f}%" if total else "0%"

    c1, c2, c3, c4, c5 = st.columns(5)
    with c1:
        st.markdown(kpi("TOTAL CLIENTES", total, "&#128101;", BLUE_PRIMARY), unsafe_allow_html=True)
    with c2:
        st.markdown(kpi(
            "CAMPEOES", green_n, "&#9989;", GREEN, green_pct,
            tooltip="Score 19–30 pts. Power User: campanhas frequentes, automacoes ativas, boa cobertura da base. Prioridade para upsell e expansao.",
        ), unsafe_allow_html=True)
    with c3:
        st.markdown(kpi(
            "EM ALERTA", yellow_n, "&#9888;", YELLOW, yellow_pct,
            tooltip="Score 11–18 pts. Usa a plataforma mas subutiliza ao menos um criterio. Acompanhamento preventivo — risco de churn se nao evoluir.",
        ), unsafe_allow_html=True)
    with c4:
        st.markdown(kpi(
            "EM RISCO", red_n, "&#128680;", RED, red_pct,
            tooltip="Score 4–10 pts. Baixo uso ou campanhas inativas. Acao de resgate imediata — contato do CSM prioritario.",
        ), unsafe_allow_html=True)
    with c5:
        st.markdown(kpi(
            "INATIVOS", inactive_n, "&#9898;", GREY, inactive_pct,
            tooltip="Score 0–3 pts. Sem atividade relevante na plataforma. Pode ser onboarding abandonado ou conta parada. Verificar status junto ao CS.",
        ), unsafe_allow_html=True)

    st.markdown("<div style='height:12px'></div>", unsafe_allow_html=True)

    # Row 2 — Score, GRR, NRR
    avg_score = _ch["total_score"].mean()
    rev_sorted = revenue.sort_values("year_month")
    latest_rev = rev_sorted.iloc[-1] if len(rev_sorted) else None
    grr_val = latest_rev["grr"] if latest_rev is not None else 0
    nrr_val = latest_rev["nrr"] if latest_rev is not None else 0

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

        _btn_all, _btn_green, _btn_yellow, _btn_red, _btn_inactive = st.columns(5)
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
        with _btn_inactive:
            if st.button("Inativo", use_container_width=True,
                         type="primary" if st.session_state.vg_status_filter == "inactive" else "secondary",
                         key="vg_btn_inactive"):
                st.session_state.vg_status_filter = "inactive"
                st.rerun()

        fig_donut = go.Figure(go.Pie(
            labels=["Campeao", "Alerta", "Em Risco", "Inativo"],
            values=[green_n, yellow_n, red_n, inactive_n],
            hole=0.6,
            marker=dict(colors=[GREEN, YELLOW, RED, GREY]),
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
        status_filter = st.selectbox("Status", ["Todos", "Campeao", "Alerta", "Em Risco", "Inativo"])

    status_map = {"Campeao": "green", "Alerta": "yellow", "Em Risco": "red", "Inativo": "inactive"}

    df = client_health.copy()
    if csm_filter != "Todos":
        df = df[df["csm_owner"] == csm_filter]
    if status_filter != "Todos":
        df = df[df["health_status"] == status_map[status_filter]]

    # Summary cards
    c1, c2, c3, c4, c5 = st.columns(5)
    with c1:
        st.markdown(kpi("CLIENTES", len(df), "&#128101;", BLUE_PRIMARY), unsafe_allow_html=True)
    with c2:
        g = len(df[df["health_status"] == "green"])
        st.markdown(kpi(
            "CAMPEOES", g, "&#9989;", GREEN,
            tooltip="Score 19–30 pts. Extrai o maximo da plataforma. Foco em expansao e indicacao.",
        ), unsafe_allow_html=True)
    with c3:
        y = len(df[df["health_status"] == "yellow"])
        st.markdown(kpi(
            "ALERTA", y, "&#9888;", YELLOW,
            tooltip="Score 11–18 pts. Usa mas subutiliza. Agendar consultoria para identificar gaps.",
        ), unsafe_allow_html=True)
    with c4:
        r = len(df[df["health_status"] == "red"])
        st.markdown(kpi(
            "EM RISCO", r, "&#128680;", RED,
            tooltip="Score 4–10 pts. Baixo engajamento. Acao imediata do CSM para resgatar o cliente.",
        ), unsafe_allow_html=True)
    with c5:
        i = len(df[df["health_status"] == "inactive"])
        st.markdown(kpi(
            "INATIVOS", i, "&#9898;", GREY,
            tooltip="Score 0–3 pts. Sem atividade. Verificar se e onboarding incompleto ou churn silencioso.",
        ), unsafe_allow_html=True)

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
            "score_recency": (
                "Recencia de Campanha", 5,
                "Ha quantos dias foi a ultima campanha enviada.\n"
                "≤7 dias → 5 pts  |  8–20 dias → 3 pts  |  >20 dias → 0 pts\n"
                "Indica se o cliente esta ativo e usando a plataforma.",
            ),
            "score_automations": (
                "Automacoes Ativas", 5,
                "Numero de automacoes personalizadas ativas\n"
                "(carrinho abandonado, pos-venda, reativacao, etc.).\n"
                "3 ou mais → 5 pts  |  1–2 → 3 pts  |  nenhuma → 0 pts",
            ),
            "score_integrations": (
                "Integracoes Conectadas", 5,
                "Numero de integracoes de e-commerce/ERP ativas\n"
                "(ex: Shopify, VTEX, Nuvem Shop, Bling).\n"
                "2 ou mais → 5 pts  |  1 → 3 pts  |  nenhuma → 0 pts\n"
                "Mais integracoes = maior lock-in e valor percebido.",
            ),
            "score_chat": (
                "Uso do Atendimento", 5,
                "Nivel de adocao do modulo de atendimento.\n"
                "Avancado (humano + flow ou AI) → 5 pts\n"
                "Essencial (apenas humano ou apenas flow) → 4 pts\n"
                "Sem uso → 0 pts",
            ),
            "score_volume": (
                "Volume de Mensagens", 5,
                "Volume e tendencia de mensagens enviadas no mes.\n"
                "Crescente ou ≥80% do plano contratado → 5 pts\n"
                "Estavel (variacao entre -10% e +10%) → 3 pts\n"
                "Em queda ou uso irrelevante → 0 pts",
            ),
            "score_coverage": (
                "Cobertura da Base", 5,
                "% da base de clientes cadastrada que recebeu\n"
                "ao menos uma mensagem no mes atual.\n"
                "≥15% da base impactada → 5 pts\n"
                "5–14% → 3 pts  |  <5% → 0 pts\n"
                "Mede o alcance real das acoes de marketing.",
            ),
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
                for col_key, (label, max_val, tooltip) in score_criteria.items():
                    val = row.get(col_key, 0) or 0
                    pct = val / max_val if max_val > 0 else 0
                    bar_color = GREEN if pct >= 0.8 else (YELLOW if pct >= 0.5 else RED)
                    c1, c2, c3 = st.columns([3, 5, 1])
                    with c1:
                        st.markdown(
                            f"<span title='{tooltip}' style='font-size:0.85rem;color:{TEXT_MUTED};cursor:help;'>"
                            f"{label} <span style='font-size:0.7rem;'>&#8505;</span></span>",
                            unsafe_allow_html=True,
                        )
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
                    st.metric(
                        "Dias s/ Campanha", days_camp,
                        help="Dias desde a ultima campanha enviada.\n"
                             "Ativo: ≤7 dias | Moderado: 8–20 dias | Inativo: >20 dias.\n"
                             "Acima de 14 dias gera alerta automatico para o CSM.",
                    )
                with m2:
                    auto = row.get("active_automations", 0) or 0
                    integ = row.get("active_integrations_count", 0) or 0
                    st.metric(
                        "Automacoes / Integracoes", f"{auto} aut | {integ} int",
                        help="Automacoes personalizadas ativas (carrinho abandonado, pos-venda, etc.)\n"
                             "e integracoes de e-commerce/ERP conectadas (Shopify, VTEX, Bling, etc.).\n"
                             "Score: automacoes 3+ → 5 pts | integracoes 2+ → 5 pts.",
                    )
                with m3:
                    usage = row.get("plan_usage_pct", 0) or 0
                    st.metric(
                        "Uso do Plano", f"{usage:.0f}%",
                        help="% das mensagens do pacote contratado utilizadas no mes atual.\n"
                             "≥80% indica uso intenso — pode qualificar para upsell de plano.\n"
                             "Abaixo de 20% com score baixo sugere cliente desengajado.",
                    )
                with m4:
                    coverage = row.get("coverage_pct", 0) or 0
                    st.metric(
                        "Cobertura da Base", f"{coverage:.1f}%",
                        help="% dos clientes cadastrados que receberam ao menos uma mensagem\n"
                             "no mes atual (campanhas + automacoes).\n"
                             "≥15% → 5 pts | 5–14% → 3 pts | <5% → 0 pts.\n"
                             "Mede o alcance real das acoes de marketing do cliente.",
                    )

                m5, m6, m7, m8 = st.columns(4)
                with m5:
                    chat_lvl = row.get("chat_usage_level", "none") or "none"
                    chat_display = {"advanced": "Avancado", "essential": "Essencial", "none": "Nao usa"}.get(chat_lvl, chat_lvl)
                    st.metric(
                        "Atendimento", chat_display,
                        help="Nivel de uso do modulo de atendimento:\n"
                             "Avancado = humano + flow de chatbot ou AI ativo (5 pts)\n"
                             "Essencial = apenas humano OU apenas flow (4 pts)\n"
                             "Nao usa = sem atendimento configurado (0 pts)",
                    )
                with m6:
                    msgs = row.get("messages_sent_current", 0) or 0
                    st.metric(
                        "Msgs Mes Atual", f"{msgs:,}",
                        help="Total de mensagens enviadas no mes corrente,\n"
                             "incluindo campanhas de marketing e automacoes.",
                    )
                with m7:
                    mom = row.get("messages_mom_change", 0) or 0
                    st.metric(
                        "Variacao MoM", f"{mom:+.1f}%",
                        help="Variacao % de mensagens enviadas em relacao ao mes anterior.\n"
                             "Positivo = crescimento | Negativo = queda.\n"
                             "Queda acima de 30% gera alerta automatico.",
                    )
                with m8:
                    flows = row.get("active_flows", 0) or 0
                    st.metric(
                        "Flows Ativos", flows,
                        help="Numero de fluxos de chatbot ativos na conta.\n"
                             "Flows ativos contribuem para o criterio de Atendimento\n"
                             "e indicam sofisticacao no uso da plataforma.",
                    )

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
                    # Faixas de classificacao (v1.1: max 30 pts)
                    fig_hist.add_hrect(y0=19, y1=30, fillcolor=GREEN, opacity=0.07, line_width=0, annotation_text="Campeao", annotation_position="top left")
                    fig_hist.add_hrect(y0=11, y1=18, fillcolor=YELLOW, opacity=0.07, line_width=0, annotation_text="Alerta", annotation_position="top left")
                    fig_hist.add_hrect(y0=4,  y1=10, fillcolor=RED,    opacity=0.07, line_width=0, annotation_text="Em Risco", annotation_position="top left")
                    fig_hist.add_hrect(y0=0,  y1=3,  fillcolor=GREY,   opacity=0.07, line_width=0, annotation_text="Inativo", annotation_position="top left")
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
        grr_current = latest["grr"]
        nrr_current = latest["nrr"]
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
            x=rev_6["year_month"], y=rev_6["grr"],
            mode="lines+markers", name="GRR",
            line=dict(color=GREEN, width=3), marker=dict(size=8),
        ))
        fig_rev.add_trace(go.Scatter(
            x=rev_6["year_month"], y=rev_6["nrr"],
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
        _up_status = st.selectbox("Status", ["Todos", "Campeao", "Alerta", "Em Risco", "Inativo"], key="up_status")

    _status_map = {"Campeao": "green", "Alerta": "yellow", "Em Risco": "red", "Inativo": "inactive"}
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
# PAGE 8 — Abrir Ticket (Jira)
# =========================================================================
elif page == "Abrir Ticket":
    st.markdown(header("Abrir Ticket", "Registre um bug ou feature request para o time de produto."), unsafe_allow_html=True)

    # Verifica configuracao Jira
    _jira_ok = bool(JIRA_URL and JIRA_EMAIL and JIRA_API_TOKEN)
    if not _jira_ok:
        st.warning("Integracao com Jira nao configurada. Adicione JIRA_URL, JIRA_EMAIL, JIRA_API_TOKEN e JIRA_PROJECT_KEY no arquivo .env")

    _MODULO_OPTIONS = {
        "Integração":        "10126",
        "Automação":         "10127",
        "Conversa":          "10128",
        "Segmentação":       "10129",
        "WhatsApp (Envio)":  "10130",
        "E-mail (Envio)":    "10131",
        "Captação de Lead":  "10132",
        "Resultados":        "10133",
        "Cashback":          "10134",
        "IA":                "10135",
        "Home":              "10170",
        "Configuração":      "10203",
    }

    def _create_jira_ticket(summary, description, issue_type, priority, labels=None):
        """Cria um issue no Jira via REST API."""
        url = f"{JIRA_URL}/rest/api/3/issue"
        payload = {
            "fields": {
                "project": {"key": JIRA_PROJECT},
                "summary": summary,
                "description": {
                    "type": "doc",
                    "version": 1,
                    "content": [{"type": "paragraph", "content": [{"type": "text", "text": description}]}],
                },
                "issuetype": {"name": issue_type},
                "priority": {"name": priority},
                **({"labels": labels} if labels else {}),
            }
        }
        resp = _requests.post(
            url,
            json=payload,
            auth=(JIRA_EMAIL, JIRA_API_TOKEN),
            headers={"Content-Type": "application/json"},
            timeout=10,
        )
        return resp

    def _set_jira_modulo(issue_key, modulo_id):
        """Atualiza o campo Módulo via PUT (edit screen aceita campos que create screen não aceita)."""
        _requests.put(
            f"{JIRA_URL}/rest/api/3/issue/{issue_key}",
            auth=(JIRA_EMAIL, JIRA_API_TOKEN),
            headers={"Content-Type": "application/json"},
            json={"fields": {"customfield_10342": {"id": modulo_id}}},
            timeout=10,
        )

    def _transition_jira_ticket(issue_key, transition_id):
        """Move o issue para a coluna correta via transicao."""
        url = f"{JIRA_URL}/rest/api/3/issue/{issue_key}/transitions"
        _requests.post(
            url,
            json={"transition": {"id": transition_id}},
            auth=(JIRA_EMAIL, JIRA_API_TOKEN),
            headers={"Content-Type": "application/json"},
            timeout=10,
        )

    def _log_hubspot_activity(app_name_value, jira_key, jira_link, ticket_summary, tipo):
        """Busca empresa pelo gupshupapp e cria Ticket no HubSpot com o link do Jira."""
        hs_key = os.environ.get("HUBSPOT_API_KEY", "")
        if not hs_key:
            return
        hs_headers = {"Authorization": f"Bearer {hs_key}", "Content-Type": "application/json"}

        # 1. Encontrar empresa pelo gupshupapp
        search_resp = _requests.post(
            "https://api.hubapi.com/crm/v3/objects/companies/search",
            headers=hs_headers,
            json={
                "filterGroups": [{"filters": [
                    {"propertyName": "gupshupapp", "operator": "EQ", "value": app_name_value}
                ]}],
                "properties": ["name", "gupshupapp"],
                "limit": 1,
            },
            timeout=10,
        )
        if search_resp.status_code != 200:
            return
        results = search_resp.json().get("results", [])
        if not results:
            return
        company_id = results[0]["id"]

        # 2. Criar Ticket no HubSpot associado à empresa
        content = (
            f"Ticket Jira: {jira_key}\n"
            f"Link: {jira_link}\n\n"
            f"{ticket_summary}"
        )
        # Stage por tipo: Bug → Pendente (Bug), Feature/Demanda → Pendente Feature
        stage_map = {"Bug": "1049833765", "Feature Request": "1049833764", "Demanda Técnica": "1049833764"}
        priority_map = {"Bug": "HIGH", "Feature Request": "MEDIUM", "Demanda Técnica": "MEDIUM"}
        ticket_resp = _requests.post(
            "https://api.hubapi.com/crm/v3/objects/tickets",
            headers=hs_headers,
            json={
                "properties": {
                    "subject": ticket_summary,
                    "content": content,
                    "hs_ticket_priority": priority_map.get(tipo, "MEDIUM"),
                    "hs_pipeline": "719982980",
                    "hs_pipeline_stage": stage_map.get(tipo, "1049833764"),
                },
                "associations": [{
                    "to": {"id": company_id},
                    "types": [{"associationCategory": "HUBSPOT_DEFINED", "associationTypeId": 26}],
                }],
            },
            timeout=10,
        )
        return ticket_resp

    def _upload_jira_attachments(issue_key, files):
        """Faz upload de imagens e vídeos como anexos no ticket Jira."""
        url = f"{JIRA_URL}/rest/api/3/issue/{issue_key}/attachments"
        for f in files:
            mime = f.type or "application/octet-stream"
            _requests.post(
                url,
                headers={"X-Atlassian-Token": "no-check"},
                files={"file": (f.name, f.getvalue(), mime)},
                auth=(JIRA_EMAIL, JIRA_API_TOKEN),
                timeout=120,  # vídeos podem ser maiores
            )

    # --- Tipo fora do form para permitir disclaimer reativo ---
    st.markdown("### Tipo de Solicitação")
    tipo = st.selectbox("Tipo de solicitação *", ["Bug", "Feature Request", "Demanda Técnica"], key="ticket_tipo")

    if tipo == "Bug":
        st.markdown(
            """
            <div style="background:#fff3cd;border:1px solid #ffc107;border-radius:8px;padding:14px 18px;margin:8px 0 16px 0;">
                <strong>⚠️ Antes de abrir um Bug, você já passou pelo prompt do Marcelo?</strong><br>
                <span style="font-size:0.9rem;">Use o Gem do Marcelo no Gemini para diagnosticar o problema antes de registrar.
                O resultado deve ser colado na descrição abaixo.</span><br><br>
                <a href="https://gemini.google.com/gem/1h9ivK-TDClkBr37H7mNTxNy--BG55TTO?usp=sharing"
                   target="_blank"
                   style="background:#ffc107;color:#212529;padding:6px 14px;border-radius:6px;text-decoration:none;font-weight:600;font-size:0.88rem;">
                   Abrir Prompt do Marcelo no Gemini →
                </a>
            </div>
            """,
            unsafe_allow_html=True,
        )

    # --- Formulario ---
    with st.form("ticket_form", clear_on_submit=True):
        st.markdown("### Informações do Cliente")
        fc1, fc2 = st.columns(2)
        with fc1:
            app_id = st.text_input("AppName do cliente *", placeholder="ex: minhaloja")
        with fc2:
            nome_cliente = st.text_input("Nome do cliente *", placeholder="ex: Mury The Brand")
        fc3, fc4 = st.columns(2)
        with fc3:
            titulo_demanda = st.text_input("Título da demanda *", placeholder="ex: Botão de envio não funciona")
        with fc4:
            relacionamento = st.selectbox(
                "Como está o relacionamento do cliente com a plataforma? *",
                ["Bom", "Neutro", "Ruim"],
            )

        urgencia = st.select_slider(
            "Você entende que é urgente para o cliente?",
            options=[1, 2, 3, 4, 5],
            value=3,
            format_func=lambda x: {1: "1 — Baixa", 2: "2", 3: "3 — Média", 4: "4", 5: "5 — Crítica"}[x],
        )

        modulo = st.selectbox("Módulo *", [""] + list(_MODULO_OPTIONS.keys()), format_func=lambda x: "Selecione o módulo..." if x == "" else x)

        _desc_labels = {
            "Bug": ("Descreva o problema *", "Cole aqui o resultado do prompt do Marcelo no Gemini..."),
            "Feature Request": ("Descreva a funcionalidade desejada *", "Descreva o que precisa ser desenvolvido..."),
            "Demanda Técnica": ("Descreva a demanda técnica *", "Descreva a integração, configuração ou ajuste técnico necessário..."),
        }
        descricao = st.text_area(
            _desc_labels[tipo][0],
            placeholder=_desc_labels[tipo][1],
            height=180,
            key="descricao_input",
        )

        anexos = st.file_uploader(
            "Imagens e vídeos (opcional)",
            type=["png", "jpg", "jpeg", "gif", "webp", "mp4", "mov", "avi", "webm", "mkv"],
            accept_multiple_files=True,
            key="ticket_anexos",
        )

        impactos = []
        if tipo == "Bug":
            st.markdown("**Está afetando as áreas abaixo?**")
            ci1, ci2 = st.columns(2)
            with ci1:
                if st.checkbox("Automação / Integração"):
                    impactos.append("Automacao/Integracao")
                if st.checkbox("Volumes grandes de clientes afetados"):
                    impactos.append("Volumes-grandes")
            with ci2:
                if st.checkbox("Utilização do produto / campanha"):
                    impactos.append("Produto/Campanha")
                if st.checkbox("Inbox instável"):
                    impactos.append("Inbox-instavel")

        submitted = st.form_submit_button("Abrir Ticket no Jira", use_container_width=True, type="primary")

    if submitted:
        # Validacao
        _erros = []
        if not app_id.strip():
            _erros.append("AppName do cliente é obrigatório.")
        if not nome_cliente.strip():
            _erros.append("Nome do cliente é obrigatório.")
        if not titulo_demanda.strip():
            _erros.append("Título da demanda é obrigatório.")
        if not descricao.strip():
            _erros.append("Descrição é obrigatória.")
        if not modulo:
            _erros.append("Módulo é obrigatório.")

        if _erros:
            for e in _erros:
                st.error(e)
        else:
            # Monta summary e descricao
            _tipo_label = {"Bug": "BUG", "Feature Request": "FEATURE", "Demanda Técnica": "DEMANDA"}[tipo]
            _summary = f"[{_tipo_label}] {app_id.strip()} - {nome_cliente.strip()} - {titulo_demanda.strip()}"

            _priority_map = {1: "Lowest", 2: "Low", 3: "Medium", 4: "High", 5: "Highest"}
            _priority = _priority_map[urgencia]
            _issue_type = "Bug" if tipo == "Bug" else "Tarefa"

            _impactos_txt = "\n".join(f"  - {i}" for i in impactos) if impactos else "  Nenhum informado"
            _desc_full = (
                f"Solicitado por: {user.get('name', 'N/A')} ({user.get('area', '')} / {user.get('role', '')})\n\n"
                f"AppName: {app_id.strip()}\n"
                f"Cliente: {nome_cliente.strip()}\n"
                f"Relacionamento com a plataforma: {relacionamento}\n"
                f"Urgência (1-5): {urgencia}\n"
                f"Tipo: {tipo}\n\n"
                f"Descrição:\n{descricao.strip()}\n\n"
                + (f"Áreas afetadas:\n{_impactos_txt}" if tipo == "Bug" else "")
            )

            _labels = impactos if impactos else []

            if _jira_ok:
                try:
                    with st.spinner("Criando ticket no Jira..."):
                        _resp = _create_jira_ticket(_summary, _desc_full, _issue_type, _priority, _labels)
                    if _resp.status_code in (200, 201):
                        _data = _resp.json()
                        _key = _data.get("key", "")
                        _link = f"{JIRA_URL}/browse/{_key}"
                        # Define módulo via PUT separado (não está na tela de criação)
                        if modulo:
                            _set_jira_modulo(_key, _MODULO_OPTIONS.get(modulo))
                        # Routing: areas afetadas → Prioridade; Bug → Bugs; Feature → Feature request; Demanda → Customer Tasks
                        if impactos:
                            _transition_jira_ticket(_key, "12")   # Prioridade
                        elif tipo == "Bug":
                            _transition_jira_ticket(_key, "15")   # Bugs
                        elif tipo == "Feature Request":
                            _transition_jira_ticket(_key, "16")   # Feature request
                        else:
                            _transition_jira_ticket(_key, "8")    # Customer Tasks (Demanda Técnica)
                        if anexos:
                            _upload_jira_attachments(_key, anexos)
                        _hs_resp = _log_hubspot_activity(app_id.strip(), _key, _link, _summary, tipo)
                        st.success(f"Ticket criado com sucesso! [{_key}]({_link})")
                        if _hs_resp is None:
                            st.warning("HubSpot: empresa nao encontrada para esse AppName.")
                        elif _hs_resp.status_code not in (200, 201):
                            st.warning(f"HubSpot: ticket nao criado ({_hs_resp.status_code}) — {_hs_resp.text[:200]}")
                        else:
                            # Adiciona label hs-{id} no Jira para Automations conseguirem fazer o sync
                            _hs_ticket_id = _hs_resp.json().get("id", "")
                            if _hs_ticket_id:
                                _requests.put(
                                    f"{JIRA_URL}/rest/api/3/issue/{_key}",
                                    auth=(JIRA_EMAIL, JIRA_API_TOKEN),
                                    headers={"Content-Type": "application/json"},
                                    json={"update": {"labels": [{"add": f"hs-{_hs_ticket_id}"}]}},
                                    timeout=10,
                                )
                    else:
                        st.error(f"Erro ao criar ticket no Jira ({_resp.status_code}): {_resp.text[:300]}")
                except Exception as _ex:
                    st.error(f"Falha na conexao com o Jira: {_ex}")
            else:
                # Modo preview sem Jira configurado
                st.info("Jira nao configurado — preview do ticket que seria criado:")
                st.json({
                    "summary": _summary,
                    "type": _issue_type,
                    "priority": _priority,
                    "reporter": user.get("name", "N/A"),
                    "description": _desc_full,
                    "labels": _labels,
                })


# =========================================================================
# PAGE 9 — Admin (so para admins)
# =========================================================================
elif page == "Admin":
    if not user.get("is_admin"):
        st.error("Acesso restrito a administradores.")
        st.stop()
    admin_page()
