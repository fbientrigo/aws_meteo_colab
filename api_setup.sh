#!/usr/bin/env bash
set -e

echo "=== AWS EC2 – Instalación y arranque de API ==="

# Detectar ruta actual
PROJECT_DIR="$(pwd)"
echo "Directorio actual: $PROJECT_DIR"

echo "=== 1. Actualizando paquetes del sistema ==="
sudo apt update && sudo apt upgrade -y

echo "=== 2. Instalando dependencias del sistema ==="
sudo apt install -y git python3 python3-venv python3-pip tmux

echo "=== 3. Creando entorno virtual .venv si no existe ==="
if [ ! -d ".venv" ]; then
    python3 -m venv .venv
    echo "Entorno virtual creado."
else
    echo "Entorno virtual ya existe."
fi

echo "=== 4. Activando entorno virtual ==="
source .venv/bin/activate

echo "Python utilizado:"
which python

echo "=== 5. Instalando dependencias desde api_requirements.txt ==="
pip install --upgrade pip
pip install -r api_requirements.txt

echo "=== 6. Iniciando API con uvicorn ==="
echo "Puedes acceder en: http://<IP_DE_TU_EC2>:8000/docs"
echo "Presiona CTRL+C para detener."

uvicorn main:app --host 0.0.0.0 --port 8000
