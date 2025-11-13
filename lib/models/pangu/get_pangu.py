import os
import subprocess
import sys
import shutil
from pathlib import Path

REPO_URL = "https://github.com/198808xc/Pangu-Weather.git"
REPO_DIR = Path("Pangu-Weather")   # recomendado usar Path

def clone_if_missing(repo_url: str, repo_dir: Path):
    """
    Clona un repositorio Git solo si la carpeta no existe.

    Parameters
    ----------
    repo_url : str
        URL del repositorio Git.
    repo_dir : pathlib.Path
        Carpeta de destino.
    """
    if not repo_dir.exists():
        print(f"Clonando repositorio desde {repo_url} ...")
        result = subprocess.run(
            ["git", "clone", "--quiet", repo_url, str(repo_dir)],
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            print("Error al clonar:", result.stderr)
            sys.exit(1)
        print("Repositorio clonado correctamente.")
    else:
        print(f"Repositorio ya existe en {repo_dir.resolve()}")

def ensure_dirs():
    for d in ["input_data", "output_data"]:
        path = Path(d)
        path.mkdir(parents=True, exist_ok=True)
        print(f"Carpeta lista: {path.resolve()}")

if __name__ == "__main__":
    clone_if_missing(REPO_URL, REPO_DIR)
    ensure_dirs()
