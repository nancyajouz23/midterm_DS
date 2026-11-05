#!/bin/sh
# Called by Patroni once, after the first leader is initialised.
# $1 is a connection string for the superuser.
set -e

psql "$1" -v ON_ERROR_STOP=1 -v pw="$APP_DB_PASSWORD" <<'SQL'
CREATE ROLE ds_user LOGIN PASSWORD :'pw';
CREATE DATABASE ds_storage OWNER ds_user;
SQL
