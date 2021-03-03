cd ${OPAQUE_HOME}
touch .git/hooks/pre-commit
rm .git/hooks/pre-commit
ln -s .hooks/pre-commit-hook.sh .git/hooks/pre-commit
