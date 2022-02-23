#FROM python:3.8
FROM php:8.0-cli

##############################
#    Setup import cronjob    #
##############################

# cron defaults
ENV IMPORT_DEFAULT_GID="9001" \
	IMPORT_DEFAULT_UID="9001" \
	TIMEZONE="CET" \
	DEFAULT_SCHEDULE="00 01 * * *" \
	CRONTAB="/var/spool/cron/crontabs/import"

# install cron and utilities
RUN apt-get update && apt-get install --yes --no-install-recommends \
        cron \
		bash \
		gzip \
		tzdata \
		nano \
	&& rm -rf /var/cache/apk/*

# Set up non-root user.
RUN addgroup --gid "$IMPORT_DEFAULT_GID" import
RUN adduser --no-create-home --disabled-password --disabled-login --ingroup import --shell /bin/bash --uid $IMPORT_DEFAULT_UID --gecos "" import

# Copy cron files.
RUN mkdir /app
COPY import.sh /app/
COPY start.sh /app/

# Make sure scripts are executable
RUN chown import:import /app/*.sh && chmod 774 /app/*.sh


##############################
# Setup import Python script #
##############################



# upgrade Python package manager to latest version
#RUN /usr/local/bin/python -m pip install --upgrade pip

# Install Python requirements
#COPY requirements.txt ./
#RUN pip install --no-cache-dir -r requirements.txt

# Copy the Python source code to the image
#COPY src /src

# Copy the unit tests to the image
#COPY tests /tests

# Copy configurations to the image
#COPY config /config

# entry point start cronjob
#WORKDIR /app
#ENTRYPOINT ["/bin/bash","/app/start.sh"]