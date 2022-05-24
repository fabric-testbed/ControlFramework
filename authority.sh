#!/usr/bin/env bash
docker exec -i $1-am-db psql am fabric < psql.upgrade
