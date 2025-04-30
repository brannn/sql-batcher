#!/bin/bash

# Get the directory of this script
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# Create .git/hooks directory if it doesn't exist
mkdir -p .git/hooks

# Create symlinks for each hook in .git-hooks
for hook in "$SCRIPT_DIR/.git-hooks"/*; do
    if [ -f "$hook" ]; then
        hook_name=$(basename "$hook")
        ln -sf "$hook" ".git/hooks/$hook_name"
        echo "Linked $hook_name hook"
    fi
done

echo "Git hooks have been set up successfully!" 