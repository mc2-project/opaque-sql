cd ${OPAQUE_HOME}
touch .git/hooks/pre-push
rm .git/hooks/pre-push
ln -s .hooks/pre-push-hook.sh .git/hooks/pre-push
