#!/bin/bash

TEMP_DIR=$(mktemp -d)
cd $TEMP_DIR

echo ">> DOWNLOADING BADGES"
wget https://archive.org/download/stackexchange/stackoverflow.com-Badges.7z

echo ">> EXTRACTING BADGES"
dtrx -o -n stackoverflow.com-Badges.7z

# -- remove last and 1st two lines
cd stackoverflow.com-Badges
echo ">> CLEAN BADGES"
sed '$ d' Badges.xml > TempBadges.xml
sed '1,2d' TempBadges.xml > Badges.xml

# -- split into manageable chunks
echo ">> SPLIT BADGES"
split --lines 100000 --additional-suffix=.xml Badges.xml

# -- remove temporary stuff
echo ">> CLEAN ARTEFACTS"
cd ..
rm stackoverflow.com-Badges/TempBadges.xml stackoverflow.com-Badges/Badges.xml stackoverflow.com-Badges.7z

echo "files saved in ${TEMP_DIR}"
