# Deployment

This directory will initiate the docker container deployments for [app-api](../app-api/), [app-predictions](../app-predictions/) and [app-monitoring](../app-monitoring/).

```
docker compose -f deploy/app-docker-compose.yml --project-directory . up --build
```
