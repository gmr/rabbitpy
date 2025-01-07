"""Parse URLS"""
import logging
import ssl
import typing
import urllib.parse

from pamqp import constants

LOGGER = logging.getLogger(__name__)

AMQP = 'amqp'
AMQPS = 'amqps'
DEFAULT_CHANNEL_MAX = 65535
DEFAULT_TIMEOUT = 3
DEFAULT_HEARTBEAT_INTERVAL = 60
DEFAULT_LOCALE = 'en_US'
DEFAULT_URL = 'amqp://guest:guest@localhost:5672/%2F'
DEFAULT_VHOST = '%2F'
GUEST = 'guest'
PORTS = {'amqp': 5672, 'amqps': 5671}
SSL_CERT_MAP = {
    'ignore': ssl.CERT_NONE,
    'optional': ssl.CERT_OPTIONAL,
    'required': ssl.CERT_REQUIRED
}


def parse(url: str = DEFAULT_URL) -> dict:
    """Parse the AMQP URL passed in and return the configuration
    information in a dictionary of values.

    The URL format is as follows:

        amqp[s]://username:password@host:port/virtual_host[?query string]

    Values in the URL such as the virtual_host should be URL encoded or
    quoted just as a URL would be in a web browser. The default virtual
    host / in RabbitMQ should be passed as %2F.

    Default values:

        - If port is omitted, port 5762 is used for AMQP and port 5671 is
          used for AMQPS
        - If username or password is omitted, the default value is guest
        - If the virtual host is omitted, the default value of %2F is used

    Query string options:

        - heartbeat
        - channel_max
        - frame_max
        - locale
        - cacertfile - Path to CA certificate file
        - capath - Path to directory containing CA certificates
        - certfile - Path to client certificate file
        - keyfile - Path to client certificate key
        - verify - Server certificate validation requirements (1)
        - ssl_check_hostname - Whether to validate the server hostname (2)

        (1) Should be one of three values:

           - ignore - Ignore the cert if provided (default)
           - optional - Cert is validated if provided
           - required - Cert is required and validated

        (2) Should be one of the following values:

            - 0, false, no - Do not validate the server hostname
            - 1, true, yes - Validate the server hostname


    :param url: The AMQP url passed in
    :raises: ValueError

    """
    parsed = urllib.parse.urlparse(url)

    _validate_uri_scheme(parsed.scheme)

    # Toggle the SSL flag based upon the URL scheme and if SSL is enabled
    use_ssl = True if parsed.scheme == AMQPS and ssl else False

    # Ensure that SSL is available if SSL is requested
    if parsed.scheme == 'amqps' and not ssl:
        LOGGER.warning('SSL requested but not available, disabling')

    # Figure out the port as specified by the scheme
    scheme_port = PORTS[AMQPS] if parsed.scheme == AMQPS else PORTS[AMQP]

    # Set the vhost to be after the base slash if it was specified
    vhost = DEFAULT_VHOST
    if parsed.path:
        vhost = parsed.path[1:] or DEFAULT_VHOST

    # Parse the query string
    query_args = urllib.parse.parse_qs(parsed.query)

    # Return the configuration dictionary to use when connecting
    return {
        'host': parsed.hostname,
        'port': parsed.port or scheme_port,
        'virtual_host': urllib.parse.unquote(vhost),
        'username': urllib.parse.unquote(parsed.username or GUEST),
        'password': urllib.parse.unquote(parsed.password or GUEST),
        'timeout': _query_args_int('timeout', query_args, DEFAULT_TIMEOUT),
        'heartbeat': _query_args_int(
            'heartbeat', query_args, DEFAULT_HEARTBEAT_INTERVAL),
        'frame_max': _query_args_int(
            'frame_max', query_args, constants.FRAME_MAX_SIZE),
        'channel_max': _query_args_int(
            'channel_max', query_args, DEFAULT_CHANNEL_MAX),
        'locale': _query_args_value('locale', query_args),
        'ssl': use_ssl,
        'ssl_options': {
            'check_hostname': _query_args_value(
                'ssl_check_hostname', query_args, '').lower()
                              not in ('0', 'false', 'no'),
            'cafile': _query_args_mk_value(
                ['cacertfile', 'ssl_cacert', 'cafile'], query_args),
            'capath': _query_args_value('capath', query_args),
            'certfile': _query_args_mk_value(
                ['certfile', 'ssl_cert'], query_args),
            'keyfile': _query_args_mk_value(
                ['keyfile', 'ssl_key'], query_args),
            'verify': _query_args_ssl_validation(query_args)
        }
    }


def _query_args_int(key: str, values: dict, default: int) -> int:
    """Return the query arg value as an integer for the specified key or
    return the specified default value.

    :param key: The key to return the value for
    :param values: The query value dict returned by urlparse
    :param default: The default return value

    """
    return int(values.get(key, [default])[0])


def _query_args_float(key: str, values: dict, default: float) -> float:
    """Return the query arg value as a float for the specified key or
    return the specified default value.

    :param key: The key to return the value for
    :param values: The query value dict returned by urlparse
    :param default: The default return value

    """
    return float(values.get(key, [default])[0])


def _query_args_value(
        key: str, values: dict,
        default: typing.Union[int, float, str, None] = None) \
        -> typing.Union[int, float, str, None]:
    """Return the value from the query arguments for the specified key
    or the default value.

    :param key: The key to get the value for
    :param values: The query value dict returned by urlparse

    """
    return values.get(key, [default])[0]


def _query_args_mk_value(keys: list[str], values: dict) \
        -> typing.Union[int, float, str, None]:
    """Try and find the query string value where the value can be specified
    with different keys.

    :param keys: The keys to check
    :param values: The query value dict returned by urlparse

    """
    for key in keys:
        value = _query_args_value(key, values)
        if value is not None:
            return value
    return None


def _query_args_ssl_validation(values: dict) -> typing.Union[int, None]:
    """Return the value mapped from the string value in the query string
    for the AMQP URL specifying which level of server certificate
    validation is required, if any.

    :param values: The dict of query values from the AMQP URI

    """
    validation = _query_args_mk_value(['verify', 'ssl_validation'], values)
    if not validation:
        return None
    elif validation not in SSL_CERT_MAP:
        raise ValueError(
            f'Unsupported server cert validation option: {validation}')
    return SSL_CERT_MAP[validation]


def _validate_uri_scheme(scheme: str) -> None:
    """Ensure that the specified URI scheme is supported by rabbitpy

    :param scheme: The value to validate
    :raises: ValueError

    """
    if scheme not in list(PORTS.keys()):
        raise ValueError(f'Unsupported URI scheme: {scheme}')
