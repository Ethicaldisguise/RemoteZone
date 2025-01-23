import configparser
import ipaddress
import json
import logging
import os
import socket
import subprocess
import urllib.request
import webbrowser
from ipaddress import IPv4Address, IPv6Address
from pathlib import Path

from kademlia.utils import digest

import src.core.eventloop  # noqa
import src.managers.logmanager as log_manager
from src.avails import RemotePeer, const, use
from src.configurations.configure import set_constants, validate_ports
from src.core import Dock, set_current_remote_peer_object
from src.managers import get_current_profile

_logger = logging.getLogger(__name__)


def initiate_bootup():
    clear_logs() if const.CLEAR_LOGS else None
    config_map = configparser.ConfigParser(allow_no_value=True)
    try:
        config_map.read(const.PATH_CONFIG)
    except KeyError:
        write_default_configurations(const.PATH_CONFIG)
        config_map.read(const.PATH_CONFIG)

    if not Path(const.PATH_PROFILES, const.DEFAULT_PROFILE_NAME).exists():
        write_default_profile()

    if const.DEFAULT_PROFILE_NAME not in config_map['USER_PROFILES']:
        config_map.set('USER_PROFILES', const.DEFAULT_PROFILE_NAME)

    with open(const.PATH_CONFIG, 'w+') as fp:
        config_map.write(fp)

    set_constants(config_map)

    Dock.exit_stack.enter_context(log_manager.initiate())
    ip_addr = get_ip(const.IP_VERSION)

    if ip_addr.version == 6:
        const.USING_IP_V4 = False
        const.USING_IP_V6 = True
        # if we cannot work with ip-v6, program will fall back to ip-v4
        const.IP_VERSION = socket.AF_INET6

    const.THIS_IP = str(ip_addr)
    # const.THIS_IP = "172.16.210.0"
    # const.WEBSOCKET_BIND_IP = const.THIS_IP
    _logger.info(f"{const.THIS_IP=}")
    validate_ports(const.THIS_IP)


def get_ip(ip_version) -> IPv4Address | IPv6Address:
    """
    Retrieves the local IP address of the machine.
    If unsuccessful, falls back to using gethostbyname().

    Returns:
        str: The local IP address as a string.

    Raises:
        socket.error: If a socket error occurs during connection.
    """
    if ip_version == socket.AF_INET:
        return get_v4() or ipaddress.IPv4Address('127.0.0.1')

    if not socket.has_ipv6:
        return get_ip(socket.AF_INET)

    primary_ip = get_v6()
    if not primary_ip.is_loopback:
        return primary_ip

    # if got a loopback addr then we try to get v4 address
    secondary_ip = get_v4()

    # if got a loopback addr again then we return ip v6 loopback address
    return primary_ip if secondary_ip.is_loopback else secondary_ip


def get_v4():
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as config_socket:
        try:
            config_socket.connect(('1.1.1.1', 80))
            config_ip = config_socket.getsockname()[0]
        except (OSError, socket.error) as e:
            # error_log(f"Error getting local ip: {e} from get_local_ip() at line 40 in core/constants.py")

            config_ip = socket.gethostbyname(socket.gethostname())
            if const.IS_DARWIN or const.IS_LINUX:
                config_ip = subprocess.getoutput("hostname -I")

    return ipaddress.IPv4Address(config_ip)


"""
    import commands
    ips = commands.getoutput("/sbin/ifconfig | grep -i \"inet\" | grep -iv \"inet6\" | " +
                         "awk {'print $2'} | sed -ne 's/addr: / /p'")
    print ips
"""


def get_v6():
    if const.IS_WINDOWS:
        back_up = IPv6Address("::1")
        for sock_tuple in socket.getaddrinfo("", None, socket.AF_INET6):
            ip, _, _, _ = sock_tuple[4]
            ipaddr = IPv6Address(ip)
            if ipaddr.is_link_local:
                back_up = ipaddr
            elif not ipaddr.is_link_local:
                return ipaddr
        return back_up
    elif const.IS_DARWIN or const.IS_LINUX:
        return get_v6_from_shell() or get_v6_from_api64()


def get_v6_from_shell():
    try:
        result = subprocess.run(['ip', '-6', 'addr', 'show'], capture_output=True, text=True)
        output_lines = result.stdout.split('\n')

        ip_v6 = []
        for line in output_lines:
            if 'inet6' in line and 'fe80' not in line and '::1' not in line:
                ip_v6.append(line.split()[1].split('/')[0])
    except Exception as exp:
        _logger.critical(f"Error occurred at ip lookup", exc_info=exp)
        return
    return ip_v6[0]


def get_v6_from_api64():
    config_ip = "::1"  # Default IPv6 address

    try:
        with urllib.request.urlopen('https://api64.ipify.org?format=json') as response:
            data = response.read().decode('utf-8')
            data_dict = json.loads(data)
            config_ip = data_dict.get('ip', config_ip)
    except (urllib.request.HTTPError, json.JSONDecodeError) as e:
        # error_log(f"Error getting local ip: {e} from get_local_ip() at line inspect.currentframe().f_lineno in core/constants.py")
        config_ip = socket.getaddrinfo(socket.gethostname(), None, socket.AF_INET6)[0][4][0]

    return config_ip


def clear_logs():
    pass


def write_default_configurations(path):
    default_config_file = (
        '[NERD_OPTIONS]\n'
        'this_port = 48221\n'
        'page_port = 12260\n'
        'ip_version = 4\n'
        'protocol = tcp\n'
        'req_port = 35623\n'
        'page_serve_port = 40000\n'
        '\n'
        '[USER_PROFILES]\n'
        'admin\n'
        '[SELECTED_PROFILE]\n'
        'default_profile.ini\n'
    )
    with open(path, 'w+') as config_file:
        config_file.write(default_config_file)


def write_default_profile():
    default_profile_file = (
        '[USER]\n'
        'name = new user\n'
        'id = 1234'
        '\n'
        '[SERVER]\n'
        'port = 45000\n'
        'ip = 0.0.0.0\n'
        'id = 0\n'
    )
    with open(os.path.join(const.PATH_PROFILES, const.DEFAULT_PROFILE_NAME), 'w+') as profile_file:
        profile_file.write(default_profile_file)
    parser = configparser.ConfigParser(allow_no_value=True)
    parser.read(const.PATH_CONFIG)
    parser.set('USER_PROFILES', const.DEFAULT_PROFILE_NAME)

    with open(const.PATH_CONFIG, 'w+') as fp:
        parser.write(fp)


def configure_this_remote_peer():
    rp = make_this_remote_peer()
    set_current_remote_peer_object(rp)
    const.USERNAME = rp.username


def make_this_remote_peer():
    profile = get_current_profile()
    rp = RemotePeer(
        byte_id=digest(profile.id),
        username=profile.username,
        ip=const.THIS_IP,
        conn_port=const.PORT_THIS,
        req_port=const.PORT_REQ,
        status=1,
    )
    return rp


@use.NotInUse
def retrace_browser_path():
    if const.IS_WINDOWS:
        import winreg
        key = winreg.OpenKey(winreg.HKEY_CURRENT_USER,
                             r"Software\Microsoft\Windows\Shell\Associations\UrlAssociations\http\UserChoice")
        prog_id, _ = winreg.QueryValueEx(key, 'ProgId')
        key.Close()

        key = winreg.OpenKey(winreg.HKEY_CLASSES_ROOT, rf"\\{prog_id}\shell\open\command")
        path, _ = winreg.QueryValueEx(key, '')
        key.Close()

        return path.strip().split('"')[1]

    if const.IS_DARWIN:
        return subprocess.check_output(["osascript",
                                        "-e",
                                        'tell application "System Events" to get POSIX path of (file of process "Safari" as alias)'
                                        ]).decode().strip()

    if const.IS_LINUX:
        command_output = subprocess.check_output(["xdg-settings", "get", "default-web-browser"]).decode().strip()

        if command_output.startswith('userapp-'):
            command_output = subprocess.check_output(["xdg-mime", "query", "default", "text/html"]).decode().strip()

        return command_output


def launch_web_page():
    page_url = os.path.join(const.PATH_PAGE, "index.html")
    try:
        webbrowser.open(page_url)
    except webbrowser.Error:
        if const.IS_WINDOWS:
            os.system(f"start {page_url}")

        elif const.IS_LINUX or const.IS_DARWIN:
            subprocess.Popen(['xdg-open', page_url])

    except FileNotFoundError:
        _logger.critical("::webpage not found, look's like the what you downloaded is corrupted")
