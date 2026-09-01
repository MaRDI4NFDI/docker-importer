FROM python:3.14-slim

##############################
#    Setup import cronjob    #
##############################

# cron defaults
ENV IMPORT_DEFAULT_GID="9001" \
  IMPORT_DEFAULT_UID="9001" 

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

##############################
# Setup Python packages      #
##############################

RUN python3 -m pip install --no-cache-dir --upgrade pip setuptools

# Install the project. Dependencies -- including prefect, wikibaseintegrator
# and mardiclient -- are declared in pyproject.toml.
#
# The build context has no .git, so setuptools-scm cannot derive the version
# from tags; CI passes the value it computed from the repository.
ARG SETUPTOOLS_SCM_PRETEND_VERSION=0.0.0

# mardiclient is tracked from its main branch, which Docker cannot see when
# deciding whether this layer is stale. CI passes the resolved commit so the
# layer is rebuilt exactly when that branch moves. The value is not read by
# pip; it only participates in the cache key.
ARG MARDICLIENT_REF=main

COPY pyproject.toml README.md /src/
COPY src /src/src
RUN MARDICLIENT_REF="${MARDICLIENT_REF}" pip install --no-cache-dir /src

# Add contentmath datatype to WikibaseIntegrator
COPY config/contentmath.py /usr/local/lib/python3.11/site-packages/wikibaseintegrator/datatypes/
RUN echo "from .contentmath import ContentMath" \
    >> /usr/local/lib/python3.11/site-packages/wikibaseintegrator/datatypes/__init__.py

# Copy configurations to the image
COPY config /config

# entry point
WORKDIR /app
CMD ["gunicorn", "-w", "2", "--timeout", "300", "-b", "0.0.0.0:8000", "mardi_portal.api.app:app"]
