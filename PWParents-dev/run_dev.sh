#!/usr/bin/env bash
set -Eeuo pipefail

# очистити консоль перед стартом
clear

# перейти в корінь проекту (з scripts/ .. -> /PWParents)
cd "$(dirname "$0")/.."

# шлях до venv (за замовчуванням ../venv, можна перевизначити VENV=/path/to/venv)
VENV="${VENV:-../venv}"
if [[ ! -d "$VENV" ]]; then
  echo "❌ VENV not found at: $VENV"
  echo "   Create it:  python3 -m venv ../venv && source ../venv/bin/activate && pip install -r requirements.txt"
  exit 1
fi

# активувати середовище
# shellcheck disable=SC1090
source "$VENV/bin/activate"

# змінні оточення для dev-запуску
export ENV=dev
export PYTHONPATH="$PWD"

# запуск бота
python -m app.main
