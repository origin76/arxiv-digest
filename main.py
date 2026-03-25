from digest_pipeline import main
from digest_runtime import LOGGER


if __name__ == "__main__":
    try:
        main()
    except Exception:
        if LOGGER.handlers:
            LOGGER.exception("Pipeline failed")
        raise
