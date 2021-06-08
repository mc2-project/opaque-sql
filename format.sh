#!/bin/bash

# Scalafmt + Clang formatter. This script formats all changed files from the last mergebase (usually master).
# Please run this locally before pushing changes for review, or install the pre-commit hook with ./hooks/install-pre-commit.

check_command_exist() {
    if ! [ -x "$(command -v "$1")" ]; then
        echo "Error: $1 not installed. Follow the instructions at https://mc2-project.github.io/opaque/contributing/contributing.html to install."
        exit 1
    fi
}

format_from_master() {
    echo "* Checking changed Scala files for formatting..."
    (cd ${OPAQUE_HOME}/; scalafmt --diff-branch PR-repo/master >/dev/null)
    git diff --quiet -- '*.scala'
    scala_formatted=$?
    echo "* Done!"
    if [ $scala_formatted -ne 0 ];
    then
        echo "* The following files needed formatting:"
        git diff --name-only -- '*.scala'
        echo "* Adding newly formatted Scala files to the most recent commit."
        git add -u
    fi

    # Run git-clang-format and check format
    echo "* Checking changed C/C++ files for formatting..."
    (cd ${OPAQUE_HOME}/; git-clang-format PR-repo/master >/dev/null)
    git diff --quiet -- '*.h' '*.c' '*.cpp'
    c_formatted=$?
    echo "* Done!"
    if [ $c_formatted -ne 0 ];
    then
        echo "* The following files needed formatting:"
        git diff --name-only -- '*.h' '*.c' '*.cpp'
        echo "* Adding newly formatted C/C++ files to the most recent commit."
        git add -u
    fi

    if [ $c_formatted -ne 0 ] || [ $scala_formatted -ne 0 ];
    then
        echo "* Creating new commit with linted files."
        files_changed=$(git diff --name-only '*.scala' '*.h' '*.c' '*.cpp')
        git commit -m "Lint with format.sh"

    fi
}

echo "Linting files..."

check_command_exist scalafmt
check_command_exist git-clang-format

if git remote -v | grep -q PR-repo; then
    git remote rm 'PR-repo'
fi
git remote add 'PR-repo' 'git@github.com:mc2-project/opaque.git' &>/dev/null
git fetch PR-repo master &>/dev/null

# Lint files from PR-repo/master.
format_from_master

git remote rm 'PR-repo'
