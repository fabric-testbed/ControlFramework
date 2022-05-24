#!/usr/bin/env bash
docker exec -i orchestrator-db psql orchestrator fabric < psql.upgrade
