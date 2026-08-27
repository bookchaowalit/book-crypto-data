#!/usr/bin/env bash
set -euo pipefail
PROJECT_DIR="$(cd "$(dirname "$0")" && pwd)"
CRON_TAG="# book-crypto-data-feeds"
LOCK_FILE="${PROJECT_DIR}/data/crypto.lock"
LOG_FILE="${PROJECT_DIR}/data/logs/cron.log"

ensure_venv() {
    if [ ! -x "${PROJECT_DIR}/.venv/bin/python" ]; then
        python3 -m venv "${PROJECT_DIR}/.venv"
        "${PROJECT_DIR}/.venv/bin/pip" install -r "${PROJECT_DIR}/requirements-collect.txt"
    fi
}

install_cron() {
    ensure_venv
    mkdir -p "${PROJECT_DIR}/data/logs" "${PROJECT_DIR}/data/exported"
    crontab -l 2>/dev/null | grep -v "${CRON_TAG}" | crontab - 2>/dev/null || true
    PYTHON="${PROJECT_DIR}/.venv/bin/python"
    (crontab -l 2>/dev/null || true; echo "0 */4 * * * cd ${PROJECT_DIR} && flock -n ${LOCK_FILE} ${PYTHON} scripts/run_crypto.py >> ${LOG_FILE} 2>&1 ${CRON_TAG}") | crontab -
    echo "Cron installed every 4 hours"
}

remove_cron() {
    crontab -l 2>/dev/null | grep -v "${CRON_TAG}" | crontab - 2>/dev/null || true
    echo "Cron removed"
}

show_status() {
    if crontab -l 2>/dev/null | grep -q "${CRON_TAG}"; then
        crontab -l | grep "${CRON_TAG}"
        echo "Status: ACTIVE"
    else
        echo "Status: NOT INSTALLED"
    fi
}

case "${1:-}" in
    install) install_cron ;;
    remove) remove_cron ;;
    status) show_status ;;
    *) echo "Usage: $0 {install|remove|status}"; exit 1 ;;
esac
