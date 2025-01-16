FROM python:3.11-slim

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
		gcc \
        cron \
		bash \
		gzip \
		tzdata \
		nano \
		git \
		libmariadb-dev \
	&& rm -rf /var/cache/apk/*

# Set up non-root user.
RUN addgroup --gid "$IMPORT_DEFAULT_GID" import \
	&& adduser --no-create-home --disabled-password --disabled-login --ingroup import --shell /bin/bash --uid $IMPORT_DEFAULT_UID --gecos "" import

##############################
# Setup import script        #
##############################

# Copy cron files.
RUN mkdir /app
COPY import.sh /app/
COPY start.sh /app/

# Make sure scripts are executable
RUN chown import:import /app/*.sh && chmod 774 /app/*.sh

##############################
# Setup Python packages      #
##############################

RUN python3 -m pip install --no-cache-dir --upgrade pip setuptools

# Install wikibaseintegrator from source
RUN git clone https://github.com/LeMyst/WikibaseIntegrator.git \
    && pip install ./WikibaseIntegrator

# Install MaRDI client
RUN git clone https://github.com/MaRDI4NFDI/mardiclient.git \
    && pip install ./mardiclient

# Install MaRDI importer
COPY /mardi_importer /mardi_importer
RUN pip install --no-cache-dir -v --no-build-isolation -e /mardi_importer

# Add contentmath datatype to WikibaseIntegrator
COPY config/contentmath.py /usr/local/lib/python3.11/site-packages/wikibaseintegrator/datatypes/
RUN echo "from .contentmath import ContentMath" \
    >> /usr/local/lib/python3.11/site-packages/wikibaseintegrator/datatypes/__init__.py

# Copy the unit tests to the image
# COPY tests /tests

# Copy configurations to the image
COPY config /config

# entry point start cronjob
WORKDIR /app
ENTRYPOINT ["/bin/bash","/app/start.sh"]
