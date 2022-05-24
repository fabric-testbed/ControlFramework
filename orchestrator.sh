#!/usr/bin/env bash
docker exec -i $1 psql orchestrator fabric < psql.upgrade
