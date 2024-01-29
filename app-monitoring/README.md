# Monitoring Web App

## Local Development

To start the app, run the following:
```shell
streamlit run monitoring_src/main.py --server.port 8502
```


The streamlit web server can be debugged locally from two entry points. The entry point can be configured using [`settings.py`](monitoring_src/settings.py)

1. Local Machine

For local testing, update the API_URL to `http://localhost:8000/api/v1`.
Access http://127.0.0.1:8502/ to see the app.

2. Local Docker container

For local docker testing, update the API_URL to `http://api:8000/api/v1`.
Access http://localhost:8502/ to see the app.



