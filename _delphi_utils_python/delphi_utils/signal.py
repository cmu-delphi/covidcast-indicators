"""Functions for understanding and creating signal names."""


def add_prefix(signal_names, wip_signal, prefix="wip_"):
    """Add prefix to signal if there is a WIP signal.

    Parameters
    ----------
    signal_names: List[str]
        Names of signals to be exported
    prefix : "wip_"
        prefix for new/non public signals
    wip_signal : List[str] or bool
        a list of wip signals: [], OR
        all signals in the registry: True OR
        only signals that have never been published: False
    Returns
    -------
    List of signal names
        wip/non wip signals for further computation
    """
    if wip_signal is True:
        return [prefix + signal for signal in signal_names]
    if isinstance(wip_signal, list):
        make_wip = set(wip_signal)
        return [
            (prefix if signal in make_wip else "") + signal
            for signal in signal_names
        ]
    if wip_signal in {False, ""}:
        return signal_names
    raise ValueError("Supply True | False or '' or [] | list()")
