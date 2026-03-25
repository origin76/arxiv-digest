from digest_runtime import LOGGER
from macro_pipeline import main


if __name__ == "__main__":
    try:
        main()
    except Exception:
        if LOGGER.handlers:
            LOGGER.exception("Macro pipeline failed")
        raise
