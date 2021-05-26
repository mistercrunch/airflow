import platform

def is_windows() -> bool:
    """
    Returns true if executing system is Windows
    """
    return platform.system() == 'Windows'

def choose_by_platform(posix_option, windows_option):
    """
    Returns either POSIX or Windows option based on executing platform
    :param posix_option: Option for POSIX system (e.g. path to executable)
    :param posix_option: Option for Windows system (e.g. path to executable)
    :returns: Choice based on platform
    """
    return windows_option if is_windows() else posix_option