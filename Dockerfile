FROM php:7.4-apache

# add mysql driver
RUN docker-php-ext-install mysqli && docker-php-ext-enable mysqli

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
		libicu-dev \
		git \
	&& rm -rf /var/cache/apk/*

# add missing php library, requires libicu-dev
RUN docker-php-ext-install intl

# Set up non-root user.
RUN addgroup --gid "$IMPORT_DEFAULT_GID" import \
	&& adduser --no-create-home --disabled-password --disabled-login --ingroup import --shell /bin/bash --uid $IMPORT_DEFAULT_UID --gecos "" import

# Copy cron files.
RUN mkdir /app
COPY import.sh /app/
COPY start.sh /app/

# Make sure scripts are executable
RUN chown import:import /app/*.sh && chmod 774 /app/*.sh

##############################
# Setup import script        #
##############################

# add Python and upgrade Python package manager to latest version
# Note: this might be considered un-docker-like, but what would be an alternative? Running 2 containers that communicate over http? 
RUN apt-get update && apt-get install --yes --no-install-recommends python3 pip \
	&& rm -rf /var/cache/apk/*
RUN python3 -m pip install --no-cache-dir --upgrade pip setuptools

# Install Sphinx
# RUN pip install --no-cache-dir sphinx sphinx-argparse sphinx-rtd-theme

# Install MaRDI client
RUN git clone https://github.com/MaRDI4NFDI/mardiclient.git
RUN pip install ./mardiclient

# Install MaRDI importer
COPY /mardi_importer /mardi_importer
RUN pip install --no-cache-dir -v --no-build-isolation -e /mardi_importer

# Add contentmath datatype to WikibaseIntegrator
COPY config/contentmath.py /usr/local/lib/python3.9/dist-packages/wikibaseintegrator/datatypes/
RUN echo "from .contentmath import ContentMath" \
    >> /usr/local/lib/python3.9/dist-packages/wikibaseintegrator/datatypes/__init__.py

# Copy the unit tests to the image
# COPY tests /tests

# Copy configurations to the image
COPY config /config

# entry point start cronjob
WORKDIR /app
ENTRYPOINT ["/bin/bash","/app/start.sh"]
