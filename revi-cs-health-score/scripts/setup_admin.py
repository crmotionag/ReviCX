"""
Cria a tabela de usuarios e o admin inicial.
Rode uma vez: python scripts/setup_admin.py
"""

import hashlib
from pathlib import Path
from sqlalchemy import create_engine, text

DB_PATH = Path(__file__).parent.parent / "data" / "revi_cs.db"


def hash_password(password):
    return hashlib.sha256(password.encode()).hexdigest()


def main():
    engine = create_engine(f"sqlite:///{DB_PATH}")

    with engine.begin() as conn:
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
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                last_login DATETIME
            )
        """))

        # Verificar se ja existe admin
        result = conn.execute(text("SELECT COUNT(*) FROM app_users WHERE is_admin = 1"))
        admin_count = result.scalar()

        if admin_count > 0:
            print(f"Ja existem {admin_count} admin(s). Nenhuma acao necessaria.")
            return

        # Criar admin padrao
        conn.execute(text("""
            INSERT INTO app_users (name, email, password_hash, area, role, is_active, is_admin)
            VALUES (:name, :email, :pwd, :area, :role, 1, 1)
        """), {
            "name": "Admin",
            "email": "admin@revi.com",
            "pwd": hash_password("revi2026"),
            "area": "Customer Success",
            "role": "Gerente",
        })

    print("=" * 50)
    print("Admin criado com sucesso!")
    print("  Email: admin@revi.com")
    print("  Senha: revi2026")
    print("  TROQUE A SENHA NO PRIMEIRO ACESSO!")
    print("=" * 50)


if __name__ == "__main__":
    main()
