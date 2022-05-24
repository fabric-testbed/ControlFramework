#!/usr/bin/env bash
docker exec -i $1 psql am fabric < psql.upgrade
