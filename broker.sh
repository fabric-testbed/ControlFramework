#!/usr/bin/env bash
docker exec -i broker-db psql broker fabric < psql.upgrade
