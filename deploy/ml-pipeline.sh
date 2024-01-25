cd feature_pipelines
poetry build
poetry publish -r my-pypi

cd ../training)pipelines
poetry build
poetry publish -r my-pypi

cd ../batch_prediction_pipelines
poetry build
poetry publish -r my-pypi