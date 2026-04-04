#!/bin/sh
# Install git hooks from the hooks/ directory into .git/hooks/
# Run once after cloning: ./hooks/install.sh

HOOK_DIR="$(git rev-parse --show-toplevel)/hooks"
GIT_HOOK_DIR="$(git rev-parse --git-dir)/hooks"

for hook in "$HOOK_DIR"/*; do
    name="$(basename "$hook")"
    [ "$name" = "install.sh" ] && continue

    cp "$hook" "$GIT_HOOK_DIR/$name"
    chmod +x "$GIT_HOOK_DIR/$name"
    echo "installed: $name"
done

echo "done."
