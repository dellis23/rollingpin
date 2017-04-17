import collections
import logging
import signal
import traceback

from twisted.internet import reactor
from twisted.internet.defer import (
    DeferredList,
    DeferredSemaphore,
    inlineCallbacks,
    returnValue,
)

from .hostsources import Host
from .transports import TransportError, ExecutionTimeout
from .utils import sleep


SIGNAL_MESSAGES = {
    signal.SIGINT: "received SIGINT",
    signal.SIGHUP: "received SIGHUP. tsk tsk.",
}


class AbortDeploy(Exception):
    pass


class DeployError(Exception):
    pass


class HostDeployError(DeployError):

    def __init__(self, host, error):
        self.host = host
        self.error = error
        super(HostDeployError, self).__init__()

    def __str__(self):
        return "{}: {}".format(self.host, self.error)


class ComponentNotBuiltError(DeployError):

    def __init__(self, component):
        self.component = component
        super(ComponentNotBuiltError, self).__init__()

    def __str__(self):
        return "{}: build token not generated".format(self.component)


class Deployer(object):

    def __init__(self, config, event_bus, parallel, sleeptime, timeout):
        """
        :param timeout: command execution timeout
        """
        self.log = logging.getLogger(__name__)
        self.host_source = config["hostsource"]
        self.transport = config["transport"]
        self.event_bus = event_bus
        self.parallel = parallel
        self.code_host = config["deploy"]["code-host"]
        self.execution_timeout = timeout
        self.sleeptime = sleeptime

    @inlineCallbacks
    def process_host(self, host, commands, timeout=0):
        log = logging.LoggerAdapter(self.log, {"host": host.name})

        yield self.event_bus.trigger("host.begin", host=host)

        results = []

        try:
            log.info("connecting")
            connection = yield self.transport.connect_to(host.address)
            for command in commands:
                log.info(" ".join(command))
                yield self.event_bus.trigger(
                    "host.command", host=host, command=command)
                result = yield connection.execute(log, command, timeout)
                results.append(result)
            yield connection.disconnect()
        except TransportError as e:
            should_be_alive = yield self.host_source.should_be_alive(host)
            if should_be_alive:
                log.error("error: %s", e)
            else:
                log.warning("error on possibly terminated host: %s", e)

            yield self.event_bus.trigger(
                "host.abort", host=host, error=e,
                should_be_alive=should_be_alive)
            raise HostDeployError(host, e)
        else:
            log.info("success! all done")
            yield self.event_bus.trigger("host.end", host=host)

        returnValue(results)

    @inlineCallbacks
    def on_host_error(self, reason):
        if not reason.check(DeployError):
            reason.printTraceback()
            yield self.abort(reason.getErrorMessage())

    @inlineCallbacks
    def run_deploy(self, hosts, components, commands):
        try:
            self.transport.initialize()
        except TransportError as e:
            raise DeployError("could not initialize transport: %s" % e)

        def signal_handler(sig, _):
            reason = SIGNAL_MESSAGES[sig]
            reactor.callFromThread(self.abort, reason)
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGHUP, signal_handler)

        yield self.event_bus.trigger("deploy.begin")

        try:
            if components:
                yield self.event_bus.trigger("build.begin")

                try:
                    # synchronize the code host with upstreams
                    # this will return a build token and build host for each
                    # component
                    sync_command = ["synchronize"] + components
                    code_host = Host.from_hostname(self.code_host)
                    (sync,) = yield self.process_host(
                        code_host, [sync_command])
                    yield self.event_bus.trigger("build.sync", sync_info=sync)

                    # this is where we build up the final deploy command
                    # resulting from all our syncing and building
                    deploy_command = ["deploy"]

                    # collect the results of the sync per-buildhost
                    by_buildhost = collections.defaultdict(list)
                    for component, sync_info in sync.iteritems():
                        component_ref = component + "@" + sync_info["token"]

                        build_host = sync_info.get("buildhost", None)
                        if build_host:
                            by_buildhost[build_host].append(component_ref)
                        else:
                            # no build host means we just pass the sync token
                            # straight through as a deploy token
                            deploy_command.append(component_ref)

                    # ask each build host to build our components and return
                    # a deploy token
                    for build_hostname, build_refs in by_buildhost.iteritems():
                        build_command = ["build"] + build_refs
                        build_host = Host.from_hostname(build_hostname)
                        (tokens,) = yield self.process_host(
                            build_host, [build_command])

                        for ref in build_refs:
                            component, at, sync_token = ref.partition("@")
                            assert at == "@"
                            try:
                                deploy_ref = component + "@" + tokens[ref]
                            except KeyError:
                                raise ComponentNotBuiltError(component)
                            deploy_command.append(deploy_ref)
                except Exception:
                    traceback.print_exc()
                    raise DeployError("unexpected error in sync/build")
                else:
                    # inject our built-up deploy command at the beginning of
                    # the command list for each host
                    commands = [deploy_command] + commands

                yield self.event_bus.trigger("build.end")

            parallelism_limiter = DeferredSemaphore(tokens=self.parallel)
            host_deploys = []
            first_host = True
            for host in hosts:
                if not first_host:
                    for i in xrange(self.sleeptime, 0, -1):
                        yield self.event_bus.trigger(
                            "deploy.sleep", host=host, count=i)
                        yield sleep(1)
                else:
                    first_host = False

                deferred = parallelism_limiter.run(
                    self.process_host, host, commands,
                    timeout=self.execution_timeout)
                deferred.addErrback(self.on_host_error)
                host_deploys.append(deferred)

                yield self.event_bus.trigger(
                    "deploy.enqueue", deploys=host_deploys)
            deferred_list = DeferredList(host_deploys)

            def component_report_cb(deferred_list_results):
                report = collections.defaultdict(lambda: collections.Counter())
                for host_deploy_result in deferred_list_results:
                    # TODO: Document what this output should look like to work
                    # properly.  I think it should probably go elsewhere.
                    success, command_outputs = host_deploy_result
                    if not success:
                        continue
                    try:
                        # Multiple commands' outputs can end up here.  Right
                        # now we just try to optimistically process it as a
                        # component report, but this makes it more difficult /
                        # impossible to distinguish between actual errors below.
                        for command_output in command_outputs:
                            components = command_output['components']
                            for component, sha in components.iteritems():
                                report[component][sha] += 1
                    except:
                        # TODO: Report errors?  Log exceptions to
                        # disk?
                        continue

                # TODO: Where should this go?  I'm thinking the information
                # should be provided to the frontend.
                #
                # TODO: Make this smarter.  It should only show anomolous
                # stuff.  Or maybe color that stuff differently.  But that
                # should be in frontends.
                print "----------------"
                print "component report"
                print "----------------"
                print "COMPONENT\tSHA\tCOUNT"
                for component in report.keys():
                    for sha, count in report[component].iteritems():
                        print "%s\t%s\t%s" % (component, sha, count)

            # Commands are a list of lists.  Each sublist contains the command
            # and any arguments passed to the command.  We just need to figure
            # out whether the user has requested a component report.
            for command in commands:
                try:
                    if command[0] == 'component_report':
                        deferred_list.addCallback(component_report_cb)
                        break
                except IndexError:
                    continue

            yield deferred_list
        except (DeployError, AbortDeploy, TransportError) as e:
            yield self.abort(str(e))
        else:
            yield self.event_bus.trigger("deploy.end")

    @inlineCallbacks
    def abort(self, reason):
        yield self.event_bus.trigger("deploy.abort", reason=reason)
        reactor.stop()
