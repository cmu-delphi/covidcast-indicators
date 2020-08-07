"""
Handle signal names.

Author: Vishakha
Created: 2020-08-07
"""

from delphi_epidata import Epidata


def add_prefix(signal_names, wip_signal, prefix: str):
    """Adds prefix to signal if there is a WIP signal
    Parameters
    ----------
    signal_names: List[str]
        Names of signals to be exported
    prefix : 'wip_'
        prefix for new/non public signals
    wip_signal : List[str] or bool
        Either takes a list of wip signals: [], OR
        incorporated all signals in the registry: True OR
        no signals: False
    Returns
    -------
    List of signal names
        wip/non wip signals for further computation
    """
    if wip_signal in ("", False):
        return signal_names
    elif wip_signal and isinstance(wip_signal, bool):
        return [
            (prefix + signal) if public_signal(signal)
            else signal
            for signal in signal_names
        ]
    elif isinstance(wip_signal, list):
        for signal in wip_signal:
            if public_signal(signal):
                signal_names.append(prefix + signal)
                signal_names.remove(signal)
        return signal_names
    else:
        raise ValueError("Supply True | False or '' or [] | list()")


def public_signal(signal_):
    """Checks if the signal name is already public using Epidata
    Parameters
    ----------
    signal_ : str
        Name of the signal
    Returns
    -------
    bool
        True if the signal is not present
        False if the signal is present
    """
    epidata_df = Epidata.covidcast_meta()
    for index in range(len(epidata_df['epidata'])):
        if 'signal' in epidata_df['epidata'][index]:
            if epidata_df['epidata'][index]['signal'] == signal_:
                return False
    return True
