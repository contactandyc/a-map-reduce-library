#!/usr/bin/env bash
# SPDX-FileCopyrightText: 2025-2026 Andy Curtis <contactandyc@gmail.com>
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail

# Run this script from examples/data or anywhere; it resolves its own directory.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

BASE_2018="https://jmcauley.ucsd.edu/data/amazon_v2"
BASE_2023="https://huggingface.co/datasets/McAuley-Lab/Amazon-Reviews-2023/resolve/main/raw"

DATASETS=(
  "amazon_2018"
  "amazon_2023"
  "quit"
)

VARIANTS=(
  "Gift_Cards"
  "Software"
  "Video_Games"
  "Electronics"
)

menu_choose() {
  local prompt="$1"
  shift
  local options=("$@")

  if command -v gum >/dev/null 2>&1; then
    printf "%s\n" "${options[@]}" | gum choose --header "$prompt"
    return
  fi

  if command -v fzf >/dev/null 2>&1; then
    printf "%s\n" "${options[@]}" | fzf --prompt="$prompt > " --height=10 --reverse
    return
  fi

  if command -v whiptail >/dev/null 2>&1; then
    local menu_items=()
    local i=1
    for opt in "${options[@]}"; do
      menu_items+=("$i" "$opt")
      ((i++))
    done

    local choice
    choice=$(
      whiptail --title "Amazon Data Downloader" \
               --menu "$prompt" 20 78 10 \
               "${menu_items[@]}" \
               3>&1 1>&2 2>&3
    )
    printf "%s\n" "${options[$((choice - 1))]}"
    return
  fi

  printf "%s\n" "$prompt" >&2
  local PS3="#? "
  select opt in "${options[@]}"; do
    if [[ -n "${opt:-}" ]]; then
      printf "%s\n" "$opt"
      return
    fi
    printf "Invalid option\n" >&2
  done
}

download_if_needed() {
  local url="$1"
  local out="$2"
  local label="$3"

  mkdir -p "$(dirname "$out")"

  if [[ -f "$out" && -s "$out" ]]; then
    echo "✅ Already exists: $out"
    return
  fi

  echo "Downloading $label..."
  curl -fL --retry 3 --retry-delay 2 -o "$out" "$url"
}

update_dataset_links() {
  local dataset_dir="$1"
  local meta_target="$2"
  local reviews_target="$3"
  local meta_link_name="$4"
  local reviews_link_name="$5"

  mkdir -p "$dataset_dir"

  ln -sfn "$(basename "$(dirname "$meta_target")")/$(basename "$meta_target")" \
    "$dataset_dir/$meta_link_name"

  ln -sfn "$(basename "$(dirname "$reviews_target")")/$(basename "$reviews_target")" \
    "$dataset_dir/$reviews_link_name"
}

echo "Choose dataset"
dataset="$(menu_choose "Choose dataset" "${DATASETS[@]}")"
[[ "$dataset" == "quit" ]] && exit 0

echo "Choose variant"
variant="$(menu_choose "Choose variant" "${VARIANTS[@]}")"

echo
echo "Dataset: $dataset"
echo "Variant: $variant"
echo

case "$dataset" in
  amazon_2018)
    DATASET_DIR="amazon_2018"
    OUT_DIR="${DATASET_DIR}/${variant}"

    META_URL="${BASE_2018}/metaFiles2/meta_${variant}.json.gz"
    REVIEWS_URL="${BASE_2018}/categoryFilesSmall/${variant}_5.json.gz"

    META_FILE="${OUT_DIR}/meta_${variant}.json.gz"
    REVIEWS_FILE="${OUT_DIR}/${variant}_5.json.gz"

    download_if_needed "$META_URL" "$META_FILE" "2018 metadata"
    download_if_needed "$REVIEWS_URL" "$REVIEWS_FILE" "2018 reviews"

    update_dataset_links \
      "$DATASET_DIR" \
      "$META_FILE" \
      "$REVIEWS_FILE" \
      "items.jsonl.gz" \
      "events.jsonl.gz"
    ;;

  amazon_2023)
    DATASET_DIR="amazon_2023"
    OUT_DIR="${DATASET_DIR}/${variant}"

    META_URL="${BASE_2023}/meta_categories/meta_${variant}.jsonl"
    REVIEWS_URL="${BASE_2023}/review_categories/${variant}.jsonl"

    META_FILE="${OUT_DIR}/meta.jsonl"
    REVIEWS_FILE="${OUT_DIR}/reviews.jsonl"

    download_if_needed "$META_URL" "$META_FILE" "2023 metadata"
    download_if_needed "$REVIEWS_URL" "$REVIEWS_FILE" "2023 reviews"

    update_dataset_links \
      "$DATASET_DIR" \
      "$META_FILE" \
      "$REVIEWS_FILE" \
      "meta.jsonl" \
      "reviews.jsonl"
    ;;

  *)
    echo "Unknown dataset: $dataset" >&2
    exit 1
    ;;
esac

echo
echo "✅ Done."
echo "  Variant directory: $SCRIPT_DIR/$OUT_DIR"
echo
echo "Updated dataset links:"
if [[ "$dataset" == "amazon_2018" ]]; then
  echo "  $SCRIPT_DIR/$DATASET_DIR/items.jsonl.gz -> $(readlink "$DATASET_DIR/items.jsonl.gz")"
  echo "  $SCRIPT_DIR/$DATASET_DIR/events.jsonl.gz -> $(readlink "$DATASET_DIR/events.jsonl.gz")"
else
  echo "  $SCRIPT_DIR/$DATASET_DIR/meta.jsonl -> $(readlink "$DATASET_DIR/meta.jsonl")"
  echo "  $SCRIPT_DIR/$DATASET_DIR/reviews.jsonl -> $(readlink "$DATASET_DIR/reviews.jsonl")"
fi
