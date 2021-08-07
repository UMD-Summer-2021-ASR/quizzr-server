class QuizzrDFError(Exception):
    """Base exception class for the data flow server"""


class UsernameTakenError(QuizzrDFError):
    """Raised when a user under a specific username already exists"""


class BadProfileError(QuizzrDFError):
    """Raised when a profile is invalid"""


class MalformedProfileError(BadProfileError):
    """Raised when a profile has one or more missing or invalid fields"""


class ProfileNotFoundError(BadProfileError):
    """Raised when a profile is not found"""
