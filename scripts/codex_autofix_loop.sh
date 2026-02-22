#!/usr/bin/env bash
set -euo pipefail

# Repeatedly invoke Codex CLI to review/fix the repository until the duration expires.
#
# Quick examples:
#   1) 기본 30분 실행
#      scripts/codex_autofix_loop.sh
#   2) 45분 동안 실행
#      scripts/codex_autofix_loop.sh 45
#   3) 모델 지정
#      CODEX_MODEL='gpt-5.2-codex' scripts/codex_autofix_loop.sh 60
#   4) 자동 커밋 활성화
#      AUTO_COMMIT=1 scripts/codex_autofix_loop.sh 60

print_usage() {
  cat <<'USAGE'
Usage:
  scripts/codex_autofix_loop.sh [duration_minutes]

Environment variables:
  CODEX_CMD      Codex CLI executable (default: codex)
  CODEX_MODEL    Model name passed to Codex (optional)
  AUTO_COMMIT    Set to 1 to auto-commit each Codex patch
  COMMIT_PREFIX  Commit prefix when AUTO_COMMIT=1 (default: "fix: auto-fix")
USAGE
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  print_usage
  exit 0
fi

DURATION_MINUTES="${1:-30}"
START_TS="$(date +%s)"
END_TS="$((START_TS + DURATION_MINUTES * 60))"

CODEX_CMD="${CODEX_CMD:-codex}"
CODEX_MODEL="${CODEX_MODEL:-}"
AUTO_COMMIT="${AUTO_COMMIT:-0}"
COMMIT_PREFIX="${COMMIT_PREFIX:-fix: auto-fix}"

run_codex_fix() {
  local prompt
  prompt="저장소 전체를 오류 관점에서 검토하고, 발견된 문제를 최소 수정으로 고쳐주세요.\
수정 후에는 포맷/린트/테스트를 실행해 상태를 확인하고 결과를 요약하세요.\
제약: panic 유발 코드(unwrap/expect/panic) 도입 금지, UTF-8 경계 안전성 유지."

  if [[ -n "$CODEX_MODEL" ]]; then
    "$CODEX_CMD" --model "$CODEX_MODEL" run "$prompt"
  else
    "$CODEX_CMD" run "$prompt"
  fi
}

maybe_commit() {
  if [[ "$AUTO_COMMIT" != "1" ]]; then
    return 0
  fi

  if ! git diff --quiet; then
    git add -A
    git commit -m "$COMMIT_PREFIX iteration $(date '+%Y-%m-%d %H:%M:%S')"
  fi
}

iteration=1
while [[ "$(date +%s)" -lt "$END_TS" ]]; do
  echo "=== Iteration $iteration ==="

  if ! run_codex_fix; then
    echo "[WARN] Codex invocation failed in iteration $iteration"
  fi

  maybe_commit
  iteration="$((iteration + 1))"
done

echo "[TIMEOUT] Reached ${DURATION_MINUTES} minutes."
exit 0
