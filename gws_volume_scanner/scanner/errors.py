"""GWS Scanner Exception and Warning Subclasses."""


class ScannerConfigError(TypeError):
    """Base class for all configuration errors."""


class ScannerMainConfigError(ScannerConfigError):
    """Raised if the main scanner configuration is unparseable.

    This is likely to result in the scanner failing to start.
    """


class ScannerGWSConfigError(ScannerConfigError):
    """Raised in configuration for a specific GWS is unparseable.

    This is likely to result in the scanner falling back on defaults.
    """


class FileNotFoundWarning(Warning):
    """Warning which catches a FileNotFoundError and warns about it instead.

    This can happen if a file was moved under the scanner.
    """
