#!/usr/bin/env bash
docker exec -i $1 psql broker fabric < psql.upgrade
