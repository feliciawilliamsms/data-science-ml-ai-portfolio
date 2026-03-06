# connection.py (drop-in)
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL

load_dotenv(override=True)

def _strip_env(name):
    v = os.getenv(name)
    if v is None:
        return None
    # remove accidental surrounding quotes / whitespace
    return v.strip().strip('"').strip("'")

ADB_USER = _strip_env("ADB_USER")
ADB_PWD = _strip_env("ADB_PWD")
ADB_DSN = _strip_env("ADB_DSN")            # e.g. "mydb_high" or "dbhost:1521/service"
ADB_WALLET_DIR = _strip_env("ADB_WALLET_DIR")  # path to wallet (TNS_ADMIN)

# sanity checks
missing = [k for k,v in [("ADB_USER",ADB_USER),("ADB_PWD",ADB_PWD),("ADB_DSN",ADB_DSN),("ADB_WALLET_DIR",ADB_WALLET_DIR)] if v in (None, "")]
if missing:
    raise ValueError(f"Missing required env vars: {missing}. Populate .env or environment before running.")

# ensure wallet path exists
if not os.path.isdir(ADB_WALLET_DIR):
    raise ValueError(f"ADB_WALLET_DIR not found or not a directory: {ADB_WALLET_DIR}")

# Tell the Oracle client where to find tnsnames/wallet files
os.environ["TNS_ADMIN"] = ADB_WALLET_DIR

def get_engine(echo: bool = False, pool_pre_ping: bool = True):
    """
    Create and return a SQLAlchemy engine using the python-oracledb dialect.

    Notes:
    - Uses thin mode by default (no client library required).
    - To use thick mode you must install Oracle Client libraries and call oracledb.init_oracle_client()
      before creating the engine; we avoid that here to prevent DPI-1047.
    - ADB_DSN should be either a tns alias (e.g. 'mydb_high') found via tnsnames.ora in the wallet dir,
      or a host:port/service string like 'dbhost:1521/service'.
    """
    # Build URL safely
    # For oracle+oracledb, the host field can be used to carry the DSN when DSN is not a host:port/service.
    url = URL.create(
        "oracle+oracledb",
        username=ADB_USER,
        password=ADB_PWD,
        host=ADB_DSN
    )

    # connect_args instruct driver to use the wallet config dir for TNS/wallet resolution
    connect_args = {
        "config_dir": ADB_WALLET_DIR,  # supports wallet/TNS resolution
        # note: do not pass 'wallet_location' here; config_dir is sufficient for TNS_ADMIN resolution
    }

    engine = create_engine(
        url,
        echo=echo,
        future=True,
        pool_pre_ping=pool_pre_ping,
        connect_args=connect_args
    )
    return engine

def test_connection(engine):
    """
    Run a quick test query to validate connectivity.
    Returns True on success, raises exception on failure with helpful hints.
    """
    try:
        with engine.connect() as conn:
            # small, safe test
            result = conn.execute(text("SELECT 1 FROM DUAL"))
            one = result.scalar()
            if one == 1:
                print("Connection test successful (SELECT 1 FROM DUAL).")
                return True
            else:
                raise RuntimeError("SELECT 1 FROM DUAL returned unexpected result.")
    except Exception as e:
        # provide actionable hints for the two most common problems seen earlier
        msg = (
            f"Connection test failed: {e}\n\n"
            "Hints:\n"
            " - Check ADB_WALLET_DIR contains wallet files (cwallet.sso, tnsnames.ora, sqlnet.ora).\n"
            " - If your ADB_DSN is a tns alias (e.g. 'mydb_high'), ensure tnsnames.ora in the wallet dir contains it.\n"
            " - If you instead have a host:port/service, set ADB_DSN to 'host:port/service'.\n"
            " - If you previously attempted Thick mode (oracledb.init_oracle_client) and got DPI-1047, you likely don't have the Oracle Client installed.\n"
            "   We use Thin mode here (no client lib required). If you need Thick mode, install the Oracle Instant Client and call\n"
            "   oracledb.init_oracle_client(lib_dir=...) before get_engine().\n"
        )
        # re-raise with combined message
        raise RuntimeError(msg) from e

# convenience: build engine and test on import (optional)
if __name__ == "__main__":
    eng = get_engine()
    test_connection(eng)
