# FastAPI

## [Schemas](schemas)

These schemas serves to:
- encode or decode data from JSON to a python object, vice versa. 
- validate the type and structure of your JSON object

## Local Development

Take a look at the [FastAPI docs](https://fastapi.tiangolo.com/advanced/settings/).

There are a few entry points into the local API server.

1. Using `__main__` entry point

The [`__main__.py`](api_src/__main__.py) provides an automatic entrypoint into initiating the FastAPI web server.

```
python3 api_src/__main__.py
```

2. Manual entry point:
  
cd into `api_src` folder
```
APP_API_GCP_PROJECT="<project-name>" APP_API_GCP_BUCKET="<bucket-name>" _API_GCP_SERVICE_ACCOUNT_JSON_PATH="json-path" uvicorn application:get_app
```

This will initialise the server on the defined port.
```
INFO:     Started server process [4621]
INFO:     Waiting for application startup.
INFO:     Application startup complete.
INFO:     Uvicorn running on http://127.0.0.1:8000 (Press CTRL+C to quit)
```


Access [127.0.0.1:8000](http://127.0.0.1:8000/api/v1/docs) from the browser to see the docs.

```
INFO:     127.0.0.1:50939 - "GET /api/v1/docs HTTP/1.1" 200 OK
INFO:     127.0.0.1:50939 - "GET /api/v1/openapi.json HTTP/1.1" 200 OK
```

![FastApi Docs](../assets/img/fastapi.png)

