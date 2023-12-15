import fire
import hopsworks

import settings
from utility import get_logger

# set up logging
logger = get_logger(__name__)

def clean():
    """
    Utiliy function used during development to clean all the data from the feature store.
    """

    project = hopsworks.login(
        api_key_value=settings.SETTINGS["FS_API_KEY"],
        project=settings.SETTINGS["FS_PROJECT_NAME"],
    )
    fs = project.get_feature_store()

    logger.info("Deleting feature views and training datasets...")
    try:
        feature_views = fs.get_feature_views(name="pm25_singapore_view")

        for feature_view in feature_views:
            try:
                feature_view.delete()
            except Exception as e:
                logger.error(e)
    except Exception as e:
        logger.error(e)

    logger.info("Deleting feature groups...")

    try:
        feature_groups = fs.get_feature_groups(name="pm25_singapore")
        for feature_group in feature_groups:
            try:
                feature_group.delete()
            except Exception as e:
                logger.error(e)
    except Exception as e:
        logger.error(e)

if __name__ == "__main__":
    fire.Fire(clean)