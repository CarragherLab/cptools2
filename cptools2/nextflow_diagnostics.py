"""Shared Nextflow diagnostics mode helpers."""


def normalise_nextflow_diagnostics_mode(mode):
    """Return a validated Nextflow diagnostics mode string."""
    if mode is None:
        mode = "full"
    elif isinstance(mode, bool):
        mode = "off" if mode is False else "full"
    mode = str(mode).strip().lower()
    if mode not in {"full", "minimal", "off"}:
        raise ValueError(
            "nextflow_diagnostics must be one of full, minimal, or off; got {}".format(
                mode
            )
        )
    return mode


def diagnostics_flags_for_mode(mode):
    """Return the Nextflow observer booleans for one diagnostics mode."""
    mode = normalise_nextflow_diagnostics_mode(mode)
    if mode == "full":
        return {
            "enable_trace": True,
            "enable_report": True,
            "enable_timeline": True,
        }
    if mode == "minimal":
        return {
            "enable_trace": True,
            "enable_report": False,
            "enable_timeline": False,
        }
    return {
        "enable_trace": False,
        "enable_report": False,
        "enable_timeline": False,
    }
