def log_start(logger, class_id):
    logger.info(f"")
    logger.info(f"")
    logger.info(f"")
    _spacer = "-" * 50
    logger.info(f"{class_id} {_spacer} {class_id} start -->")


def log_varscanner_start(log, run_id, filescanner_df_outfilepath, logfile_name) -> None:
    log.info(f"")
    log.info(f"Calling varscanner for run {run_id} ...")
    log.info(f"")
    log.info(f"")
    log.info(f"Preparing VarScanner: found required files from FileScanner run:")
    log.info(f"  * {filescanner_df_outfilepath}")
    log.info(f"  * {logfile_name}")
    log.info("")
    log.info("NOTE:")
    log.info("  (i) Data will be uploaded file-by-file.")
    log.info("")


def log_end(logger, class_id):
    _spacer = "-" * 50
    logger.info(f"{class_id} {_spacer} <-- {class_id} end")
    logger.info(f"")
