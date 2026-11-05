#!/usr/bin/env bash
# Download and unpack the Enron Email 2015 dataset (assignment URL) into ./data.
#
# The tarball does not name its top-level directory after the dataset — this
# release unpacks into "maildir/" (150 user dirs, ~517k mail files). The
# tree is adopted to ./data/<NAME> the same way the producer does, so it
# always sits at the <data_dir>/<dataset_name> contract the services use.
#
# Idempotent: skips the download if the tree is already unpacked.
set -euo pipefail

DEST_DIR="${DATA_DIR:-data}"
BASE_URL="https://www.cs.cmu.edu/~enron"
TARBALL="enron_mail_20150507.tar.gz"
NAME="enron_mail_20150507"

# adopt [dir] — if `dir` holds exactly one top-level directory under a
# foreign name, rename it to $NAME (the producer's adoptTree, in shell).
adopt() {
  local d="$1" tree
  tree="$(cd "$d" && ls -d */ 2>/dev/null | head -1 | tr -d /)"
  if [ -n "$tree" ] && [ "$tree" != "$NAME" ]; then
    mv "$d/$tree" "$d/$NAME"
    echo "adopted ./$tree -> ./$NAME"
  fi
}

mkdir -p "$DEST_DIR"
cd "$DEST_DIR"

if [ -d "$NAME" ]; then
  echo "dataset already unpacked: ./$NAME"
else
  # A prior extraction may have left the tree under the tarball's name.
  adopt .
  if [ -d "$NAME" ]; then
    echo "dataset already unpacked: ./$NAME"
  else
    echo "==> downloading $BASE_URL/$TARBALL"
    curl --fail --location --progress-bar --retry 3 -o "$TARBALL" "$BASE_URL/$TARBALL"
    echo "==> extracting"
    tar -xzf "$TARBALL"
    rm -f "$TARBALL"
    adopt .
    if [ ! -d "$NAME" ]; then
      echo "error: ./$NAME not found after extraction" >&2
      ls -la >&2
      exit 1
    fi
  fi
fi

echo "==> summary:"
du -sh "$NAME"
echo "users: $(find "$NAME" -mindepth 1 -maxdepth 1 -type d | wc -l)"
echo "mail files: $(find "$NAME" -type f | wc -l)"
echo "sample:"
find "$NAME" -type f | head -3
