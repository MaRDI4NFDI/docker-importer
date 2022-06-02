#!/bin/bash

# Entrypopint of the Dockerfile.
# Sets up the crontab to call import.sh on a regular basis

set +e

# Adjust timezone.
# TIMEZONE is set in the Dockerfile
cp /usr/share/zoneinfo/${TIMEZONE} /etc/localtime
echo ${TIMEZONE} > /etc/timezone
echo "Date: `date`."

# Set up import group.
# IMPORT_GID is set in the Dockerfile
: ${IMPORT_GID:="$IMPORT_DEFAULT_GID"}
if [ "$(id -g import)" != "$IMPORT_GID" ]; then
	groupmod -o -g "$IMPORT_GID" import
fi
echo "Using group ID $(id -g import)."

# Set up import user.
# IMPORT_UID is set in the Dockerfile
: ${IMPORT_UID:="$IMPORT_DEFAULT_UID"}
if [ "$(id -u import)" != "$IMPORT_UID" ]; then
	usermod -o -u "$IMPORT_UID" import
fi
echo "Using user ID $(id -u import)."

# Make sure the files are owned by the user executing import, as we
# will need to add/delete files.
chown import:import /app/import.sh

# Set up crontab.
# CRONTAB and DEFAULT_SCHEDULE are set in the Dockerfile
# IMPORT_SCHEDULE is set in docker-compose.yml
if [ "$IMPORTER_CRON_ENABLE" = true ]; then
	echo "" > $CRONTAB
	echo "${IMPORT_SCHEDULE:-$DEFAULT_SCHEDULE} /app/import.sh" >> $CRONTAB
else
    echo "No cronjob set for importer"
fi

crontab -u import - < $CRONTAB

#echo "Starting cron."
exec cron -l 8 -f
