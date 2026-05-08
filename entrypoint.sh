#!/bin/bash

set -e

FORMAT="${1:?FORMAT argument is required}"
INPUT="${2:?INPUT argument is required}"
OUTPUT="${3:?OUTPUT argument is required}"
BRANCH="${4:?BRANCH argument is required}"

echo "Configuring git..."
git config --global user.email "github-actions[bot]@users.noreply.github.com"
git config --global user.name "github-actions[bot]"
git config --global --add safe.directory /github/workspace

echo "Preparing branch..."
git fetch origin

if [[ ! "$BRANCH" =~ ^[a-zA-Z0-9/_.-]+$ ]]; then
  echo "Error: Invalid branch name '$BRANCH'"
  exit 1
fi
if git ls-remote --exit-code --heads origin "$BRANCH"; then
  git checkout "$BRANCH"
  git pull origin "$BRANCH"
else
  echo "Warning: Branch '$BRANCH' does not exist. Creating as orphan (empty history)."
  git checkout --orphan "$BRANCH"
  git rm -rf .
fi

echo "Running processor..."
python3 /app/batchProcessor.py append "$FORMAT" "$INPUT" "$OUTPUT"

echo "Committing changes..."
git add "$OUTPUT"
if git diff --quiet && git diff --staged --quiet; then
  echo "No changes to commit."
else
  git commit -m "chore: update metrics [skip ci]"
  git push origin "$BRANCH"
fi
