remote="$1"
url="$2"

echo "AHHHHHHHHHHHH"

# Run scalafmt and check format
(cd $DIR/; scalafmt)
git diff --quiet
formatted=$?
echo "* Scala properly formatted?"
if [ $formatted -eq 0 ]
then
   echo "* Yes"
else
    echo "* No"
    echo "The following files need formatting (in stage or commited):"
    git diff --name-only
    echo ""
    echo "Running 'scalafmt' to format the code."
    scalafmt
    exit 1
fi

exit 0

