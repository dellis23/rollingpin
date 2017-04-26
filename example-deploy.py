#!/usr/bin/python

from __future__ import print_function

import json
import os
import socket
import subprocess
import sys


def run(*argv):
    """Run a command and send all of its output to stderr."""
    print(*argv, file=sys.stderr)
    subprocess.check_call(argv, stdout=sys.stderr)


def run_and_capture(*argv):
    """Run a command and return its stdout while allowing stderr to pass on."""
    print(*argv, file=sys.stderr)
    return subprocess.check_output(argv)


def synchronize(*components):
    """Synchronize the code repositories with upstreams.

    This is called once on the code host with a list of all components to
    synchronize. The script should fetch from upstream remotes, sync with a SCM
    server, or whatever your system does. The command should return a mapping
    of each component to an object containing a unique identifier for this
    version of the code and optionally the hostname of a buildhost.

    If a buildhost is provided for a component, rollingpin will run the build
    command on the build host with a list of all components destined for it.

    If no buildhost is provided, the token returned by synchronize will be
    passed straight through to the deploy command on each host.

    This command is required if you want to run "-d" commands in rollingpin.

    """

    return {
        # a component that needs to be built
        "buildable": {
            "token": "7be0db612ea365e1d9410763198bb79a9e28dfd6",
            "buildhost": "build-01",
        },
        # a component that just gets deployed without build
        "simple": {
            "token": "d8b97272f2f71658be46d5320761ed487a913529",
            "buildhost": None,
        },
    }



def build(*components_with_tokens):
    """Build a component in preparation for deploy.

    :param components_with_tokens: a list of deploy targets and SHAs to be
        deployed, each in the format "foo@12345".

    This will be called once on each build host with a list of all components
    to build.  The script should prepare the components for deploy in whatever
    way is necessary (build a .deb package, fetch down from an SCM system,
    etc.) and return a mapping of components to tokens that identify the
    relevant build artifacts, in the format:

        {
            'foo@012345': '012345',
            'bar@abcdef': 'abcdef',
        }

    This command is required if you want to run "-d" commands in rollingpin.

    """

    res = {}
    for component_with_token in components_with_tokens:
        component, sep, build_token = component_with_token.partition("@")
        assert sep == "@"
        # TODO: put your build logic in here!
        res[component] = "example"
    return res


def deploy(*components_with_tokens):
    """Deploy a component on a host.

    This is executed on each host to deploy.  Rollingpin will pass a list of
    components with their build tokens as generated by the `build` command.

    This command is required if you want to run "-d" commands in rollingpin.

    """

    for component_with_token in components_with_tokens:
        component, sep, build_token = component_with_token.partition("@")
        assert sep == "@"

        # TODO: put your deploy logic here!


def components():
    """Collect information about the SHA of each running process.

    This can be used to identify hanging processes.  A summary report will be
    a printed to stdout, in the format:

        *** component report
        COMPONENT      SHA     COUNT
        foo         012345      1
        bar         abcdef      1

    To support this functionality, the `components` command should
    return a result containing all running SHAs of all components on the
    host, in the format:

        {
            'components': {
                'foo': '012345',
                'bar': 'abcdef',
            }
        }

    The `components` command should also print more detailed
    information to stderr to allow an operator to dig in further if a
    problem is found.  The suggested format of this output:

        component: app-123 foo@012345
        component: app-123 bar@abcdef

    """
    components = {
        'components': {
            'foo': '012345',
        },
    }
    hostname = socket.gethostname()
    for component, commit_hash in components['components'].iteritems():
        print("component: %s %s %s" % (hostname, component, commit_hash),
              file=sys.stderr)
    return components


def restart(service):
    """Restart a named service.

    This is executed on each host to restart a service.

    This command is required if you want to run "-r" commands in rollingpin.

    """
    # TODO: replace this with your relevant restart logic
    assert service.isalpha()
    run("service", service, "restart")


def custom():
    """Your command here!

    Any custom commands that you desire can be added to these deploy scripts
    and rollingpin can run them by running "-c" commands.

    """
    run("example")


def main(commands):
    """Do basic setup and dispatch commands to their handlers.

    Rollingpin executes commands that take the format of "command [args...]".
    With the SSH transport, this program is executed with sudo and the command
    is passed as the first argument. e.g.

        deploy project@abcdefg

    maps to

        sudo /usr/local/bin/deploy deploy project@abcdefg

    Additionally, the SSH transport expects all commands to output a JSON blob
    on stdout and will display anything printed to stderr as a log message.

    This main function uses the first command line argument to select a
    function to execute, then JSON encodes the return value of the function (or
    {} if None) for response to rollingpin.

    """
    progname = os.path.basename(sys.argv[0])

    if len(sys.argv) < 2:
        print("USAGE: {} COMMAND [ARG...]".format(progname), file=sys.stderr)
        sys.exit(1)

    command_name = sys.argv[1]
    args = sys.argv[2:]

    def fatal_error(message_fmt, *args):
        message = message_fmt.format(args)
        formatted = "{}: {}: {}".format(progname, command_name, message)
        print(formatted, file=sys.stderr)
        sys.exit(1)

    command_fn = commands.get(command_name)
    if not command_fn:
        fatal_error("unknown command")

    try:
        result = command_fn(*args)
    except Exception as e:
        fatal_error(str(e))

    print(json.dumps(result or {}, indent=2))


if __name__ == "__main__":
    main({
        "synchronize": synchronize,
        "build": build,
        "components": components,
        "deploy": deploy,
        "restart": restart,
        "custom": custom,
    })
