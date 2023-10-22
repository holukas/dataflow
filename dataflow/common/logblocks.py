def log_start(logger, class_id):
    logger.info(f"")
    logger.info(f"")
    logger.info(f"")
    _spacer = "-" * 50
    logger.info(f"{class_id} {_spacer} {class_id} start -->")


def log_end(logger, class_id):
    _spacer = "-" * 50
    logger.info(f"{class_id} {_spacer} <-- {class_id} end")
    logger.info(f"")
