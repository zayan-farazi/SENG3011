#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ARTIFACT_DIR="$ROOT_DIR/build/lambdas"
WORK_DIR="$ARTIFACT_DIR/work"
BUILD_VENV_DIR="${BUILD_VENV_DIR:-$ROOT_DIR/build/.lambda-build-venv}"
PYTHON_BIN="${PYTHON_BIN:-python3.12}"

if ! command -v "$PYTHON_BIN" >/dev/null 2>&1; then
  PYTHON_BIN="${PYTHON_BIN/python3.12/python3}"
fi

rm -rf "$WORK_DIR"
mkdir -p "$ARTIFACT_DIR" "$WORK_DIR"

if [ ! -d "$BUILD_VENV_DIR" ]; then
  "$PYTHON_BIN" -m venv "$BUILD_VENV_DIR"
fi

VENV_PYTHON="$BUILD_VENV_DIR/bin/python"
VENV_PIP="$BUILD_VENV_DIR/bin/pip"

"$VENV_PIP" install --upgrade pip >/dev/null

build_lambda() {
  local service="$1"
  local zip_name="$2"
  local service_dir="$ROOT_DIR/lambdas/$service"
  local stage_dir="$WORK_DIR/$service"
  local zip_path="$ARTIFACT_DIR/$zip_name"

  rm -rf "$stage_dir" "$zip_path"
  mkdir -p "$stage_dir/lambdas/$service"

  if [ -f "$service_dir/requirements.txt" ]; then
    if [ "$service" = "analytics" ]; then
      "$VENV_PIP" install --upgrade \
        --only-binary=:all: \
        --platform manylinux2014_x86_64 \
        --implementation cp \
        --python-version 3.12 \
        -r "$service_dir/requirements.txt" \
        --target "$stage_dir" >/dev/null
    else
      "$VENV_PIP" install --upgrade -r "$service_dir/requirements.txt" --target "$stage_dir" >/dev/null
    fi
  fi

  cp "$ROOT_DIR/constants.py" "$stage_dir/constants.py"
  cp "$ROOT_DIR/lambdas/__init__.py" "$stage_dir/lambdas/__init__.py"
  cp "$ROOT_DIR/lambdas/metrics.py" "$stage_dir/lambdas/metrics.py"
  cp -R "$service_dir/." "$stage_dir/lambdas/$service/"
  find "$stage_dir" -name '__pycache__' -type d -prune -exec rm -rf {} +

  (
    cd "$stage_dir"
    zip -qr "$zip_path" .
  )
}

build_lambda "location" "location.zip"
build_lambda "retrieval" "retrieval.zip"
build_lambda "ingestion" "ingestion.zip"
build_lambda "processing" "processing.zip"
build_lambda "analytics" "analytics.zip"
build_lambda "watchlist" "watchlist.zip"

rm -rf "$WORK_DIR"
