#!/usr/bin/env bash
set -euo pipefail

# Базові шляхи
BASE_DIR="/root/Python"
PROD_DIR="$BASE_DIR/PWParents"
DEV_DIR="$BASE_DIR/PWParents-dev"
BACKUP_DIR="$BASE_DIR/PWParents-backups"

mkdir -p "$BACKUP_DIR"

usage() {
  echo "Використання:"
  echo "  $0 prod2dev   # оновити DEV з PROD"
  echo "  $0 dev2prod   # оновити PROD з DEV"
  exit 1
}

if [[ $# -ne 1 ]]; then
  usage
fi

case "$1" in
  prod2dev)
    SRC="$PROD_DIR"
    DST="$DEV_DIR"
    DIRECTION="PROD → DEV"
    ;;
  dev2prod)
    SRC="$DEV_DIR"
    DST="$PROD_DIR"
    DIRECTION="DEV → PROD"
    ;;
  *)
    usage
    ;;
esac

if [[ ! -d "$SRC" ]]; then
  echo "❌ Джерело не існує: $SRC"
  exit 1
fi

if [[ ! -d "$DST" ]]; then
  echo "❌ Цільова папка не існує: $DST"
  exit 1
fi

echo "👉 Напрямок синку: $DIRECTION"
echo "   SRC: $SRC"
echo "   DST: $DST"
read -rp "Продовжити? [y/N] " ans
if [[ ! "${ans:-}" =~ ^[yY]$ ]]; then
  echo "Скасовано."
  exit 0
fi

# 1) Бекап цільової папки (включно з .env*)
TS="$(date +%Y%m%d-%H%M%S)"
BACKUP_NAME="$(basename "$DST")-$TS.tar.gz"
BACKUP_PATH="$BACKUP_DIR/$BACKUP_NAME"

echo "📦 Роблю бекап цільової папки: $BACKUP_PATH"
tar \
  --exclude='.venv' \
  --exclude='.git' \
  --exclude='__pycache__' \
  --exclude='.mypy_cache' \
  --exclude='.pytest_cache' \
  --exclude='*.log' \
  -czf "$BACKUP_PATH" -C "$DST" .

echo "✅ Бекап готовий."

# 2) Синхронізація SRC → DST (без виключення .env*)
echo "🔁 Копіюю файли з $SRC в $DST ..."

rsync -av --delete \
  --exclude='.venv' \
  --exclude='.git' \
  --exclude='__pycache__' \
  --exclude='.mypy_cache' \
  --exclude='.pytest_cache' \
  --exclude='.idea' \
  --exclude='.vscode' \
  --exclude='*.log' \
  "$SRC"/ "$DST"/

echo "✅ Синхронізація завершена."
echo "ℹ️ Бекап цілі: $BACKUP_PATH"
