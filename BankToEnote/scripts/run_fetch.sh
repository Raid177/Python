#!/usr/bin/env bash
set -euo pipefail
cd /root/Python/BankToEnote
source /root/Python/venv/bin/activate
python -m app.fetch_bank_data
