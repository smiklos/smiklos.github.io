#!/usr/bin/env sh

# Run this script to deploy the app to Github Pages.

# Exit if any subcommand fails.
set -e

echo "Started deploying"

git checkout master

# Build site.

bundle exec jekyll build

# Delete and move files.
find . -maxdepth 1 ! -name '_site' ! -name '.git' ! -name '.gitignore' -exec rm -rf {} \;
mv _site/* .
rm -R _site/

# Push to master.
git add -fA
git commit --allow-empty -m "$(git log -1 --pretty=%B) [ci skip]"
git push -f -q origin master

# Move back to previous branch.
git checkout -

echo "Deployed Successfully!"

exit 0