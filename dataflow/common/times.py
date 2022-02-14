import datetime as dt


def _make_run_id(prefix: str = None) -> str:
    """Make run identifier based on current datetime"""
    now_time_dt = dt.datetime.now()
    now_time_str = now_time_dt.strftime("%Y%m%d-%H%M%S")
    prefix = prefix if prefix else "RUN"
    run_id = f"{prefix}-{now_time_str}"
    return run_id
