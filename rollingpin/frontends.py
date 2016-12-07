# -*- coding: utf-8 -*-
from __future__ import division

import collections
import logging
import re
import sys
import termios
import time
import tty

from twisted.internet.defer import (
    Deferred,
    DeferredList,
    inlineCallbacks,
    returnValue,
)
from twisted.internet.protocol import Protocol
from twisted.internet.stdio import StandardIO

from .deploy import AbortDeploy
from .utils import sorted_nicely


class Color(object):
    RED = "31"
    GREEN = "32"
    YELLOW = "33"
    BLUE = "34"
    MAGENTA = "35"
    CYAN = "36"
    WHITE = "37"

    @staticmethod
    def BOLD(color):
        return "1;" + color


def colorize(text, color):
    start = "\033[%sm" % color
    return start + text + "\033[0m"


def at_position(text, row, col):
    # From the top left corner.  Numbering starts at 1.
    start = "\033[%d;%dH" % (row, col)
    return start + text


def clear_screen():
    return "\033[2J"


COLOR_BY_LOGLEVEL = {
    logging.DEBUG: Color.WHITE,
    logging.INFO: Color.BOLD(Color.WHITE),
    logging.WARNING: Color.YELLOW,
    logging.ERROR: Color.BOLD(Color.RED),
}


class HostFormatter(logging.Formatter):
    def __init__(self, longest_hostname):
        self.hostname_format = "[%%%ds] " % (longest_hostname + 2)
        logging.Formatter.__init__(self)

    def format(self, record):
        formatted = logging.Formatter.format(self, record)

        if hasattr(record, "host"):
            formatted = (self.hostname_format % record.host) + formatted

        color = COLOR_BY_LOGLEVEL[record.levelno]
        return colorize(formatted, color)


class HeadlessFrontend(object):
    def __init__(self, event_bus, hosts, args):
        longest_hostname = max(len(host) for host in hosts)

        formatter = HostFormatter(longest_hostname)
        self.log_handler = logging.StreamHandler()
        self.log_handler.setFormatter(formatter)
        if args.verbose_logging:
            self.enable_verbose_logging()
        else:
            self.disable_verbose_logging()

            # temporarily boost logging during the build phase
            event_bus.register({
                "build.begin": self.enable_verbose_logging,
                "build.end": self.disable_verbose_logging,
            })

        root = logging.getLogger()
        root.addHandler(self.log_handler)

        self.host_results = dict.fromkeys(hosts, None)
        self.start_time = None

        event_bus.register({
            "deploy.begin": self.on_deploy_begin,
            "deploy.end": self.on_deploy_end,
            "deploy.abort": self.on_deploy_abort,
            "host.end": self.on_host_end,
            "host.abort": self.on_host_abort,
        })

    def enable_verbose_logging(self):
        self.log_handler.setLevel(logging.DEBUG)

    def disable_verbose_logging(self):
        self.log_handler.setLevel(logging.INFO)

    def on_deploy_begin(self):
        self.start_time = time.time()
        print colorize("*** starting deploy", Color.BOLD(Color.GREEN))

    def count_hosts(self):
        return len(self.host_results)

    def count_completed_hosts(self):
        return sum(1 for v in self.host_results.itervalues() if v)

    def percent_complete(self):
        return (self.count_completed_hosts() / self.count_hosts()) * 100

    def on_host_end(self, host):
        if host in self.host_results:
            self.host_results[host] = "success"
            print colorize("*** %d%% done" % self.percent_complete(), Color.GREEN)

    def on_host_abort(self, host, error, should_be_alive):
        if host in self.host_results:
            if should_be_alive:
                self.host_results[host] = "error"
            else:
                self.host_results[host] = "warning"

    def on_deploy_abort(self, reason):
        print colorize(
            "*** deploy aborted: %s" % reason, Color.BOLD(Color.RED))

    def on_deploy_end(self):
        by_result = collections.defaultdict(list)
        for host, result in self.host_results.iteritems():
            by_result[result].append(host)

        print colorize("*** deploy complete!", Color.BOLD(Color.GREEN))

        if by_result["warning"]:
            warning_hosts = by_result["warning"]
            print ("*** encountered errors on %d possibly terminated "
                   "hosts:" % len(warning_hosts))
            print "      ", " ".join(
                colorize(host, Color.YELLOW)
                for host in sorted_nicely(warning_hosts))

        if by_result["error"]:
            error_hosts = by_result["error"]
            print ("*** encountered unexpected errors on %d "
                   "healthy hosts:" % len(error_hosts))
            print "      ", " ".join(
                colorize(host, Color.RED)
                for host in sorted_nicely(error_hosts))

        successful_hosts = len(by_result["success"])
        print "*** processed %d hosts successfully" % successful_hosts

        elapsed = time.time() - self.start_time
        print "*** elapsed time: %d seconds" % elapsed


class StdioListener(Protocol):
    def __init__(self):
        self.character_waiter = Deferred()
        self.old_termio_settings = None

    def connectionMade(self):
        self.disable_echo()

    def disable_echo(self):
        fileno = sys.stdin.fileno()
        self.old_termio_settings = termios.tcgetattr(fileno)
        tty.setcbreak(fileno)

    def restore_terminal_settings(self):
        fileno = sys.stdin.fileno()
        termios.tcsetattr(fileno, termios.TCSADRAIN, self.old_termio_settings)

    def dataReceived(self, data):
        waiter = self.character_waiter
        self.character_waiter = Deferred()
        waiter.callback(data)

    def connectionLost(self, reason):
        self.restore_terminal_settings()

    @inlineCallbacks
    def read_character(self):
        while True:
            character = yield self.character_waiter
            if character:
                returnValue(character)

    @inlineCallbacks
    def raw_input(self, prompt):
        self.restore_terminal_settings()

        try:
            sys.stdout.write(prompt)
            sys.stdout.flush()

            line = yield self.character_waiter
            returnValue(line.rstrip("\n"))
        finally:
            self.disable_echo()


class HeadfulFrontend(HeadlessFrontend):
    def __init__(self, event_bus, hosts, args):
        HeadlessFrontend.__init__(self, event_bus, hosts, args)

        self.console_input = StdioListener()
        StandardIO(self.console_input)

        event_bus.register({
            "deploy.sleep": self.on_sleep,
            "deploy.enqueue": self.on_enqueue,
        })

        self.pause_after = args.pause_after
        self.enqueued_hosts = 0

    def on_sleep(self, host, count):
        print colorize("*** sleeping %d..." % count, Color.BOLD(Color.BLUE))

    @inlineCallbacks
    def on_enqueue(self, deploys):
        # the deployer has added a host to the queue to deploy to
        self.enqueued_hosts += 1

        # we won't pause the action if we're near the end or have room for more
        completed_hosts = self.count_completed_hosts()
        if completed_hosts + self.pause_after >= self.count_hosts():
            return

        if not self.pause_after or self.enqueued_hosts < self.pause_after:
            return

        # wait for outstanding hosts to finish up
        yield DeferredList(deploys, consumeErrors=True)

        # prompt the user for what to do now
        while True:
            print colorize(
                "*** waiting for input: e[x]it, [c]ontinue, [a]ll remaining, "
                "[p]ercentage", Color.BOLD(Color.CYAN))

            c = yield self.console_input.read_character()

            if c == "a":
                self.pause_after = 0
                break
            elif c == "x":
                raise AbortDeploy("x pressed")
            elif c == "c":
                self.pause_after = 1
                break
            elif c == "p":
                min_percent = self.percent_complete() + 1
                prompt = "how far? (%d-100) " % min_percent
                prompt_input = yield self.console_input.raw_input(prompt)

                try:
                    desired_percent = int(prompt_input)
                except ValueError:
                    continue

                if not (min_percent <= desired_percent <= 100):
                    print("must be an integer between %d and 100" % min_percent)
                    continue

                completed_hosts = self.count_completed_hosts()
                desired_host_index = int((desired_percent / 100) * self.count_hosts())
                self.pause_after = desired_host_index - completed_hosts
                break

        self.enqueued_hosts = 0


class OrderedDefaultDict(collections.OrderedDict, collections.defaultdict):
    def __init__(self, default_factory=None, *args, **kwargs):
        super(OrderedDefaultDict, self).__init__(*args, **kwargs)
        self.default_factory = default_factory


class RenderNode(object):

    def __init__(self, col, text):
        self.col = col
        self.text = text

    def _strip_ansi(self, text):
        # Strip color
        text = re.sub('\033' + r'\[[\d]+m(.*)', r'\1', text)
        # Strip ansi ends
        text = text.replace("\033[0m", "")
        return text

    def __len__(self):
        return len(self._strip_ansi(self.text))


class RenderFrame(object):

    def __init__(self):
        self.grid = []

    def add(self, text, row, col):

        # Extend the grid
        if len(self.grid) < row:
            needed_rows = row - len(self.grid)
            self.grid.extend(([],) * needed_rows)

        # Add the node
        self.grid[row - 1].append(RenderNode(col, text))

    def write_to_screen(self):
        for row in self.grid:
            to_print = ''
            sorted_nodes = sorted(row, key=lambda x: x.col)

            # Build a string of what should be printed for this row, filling in
            # spaces where needed.
            i = 0
            for node in sorted_nodes:
                spaces_needed = max(0, node.col - i)
                if spaces_needed:
                    to_print += ' ' * spaces_needed
                    i += spaces_needed
                to_print += node.text
                i += len(node)

            print to_print


class AnimatedFrontend(HeadfulFrontend):

    def __init__(self, event_bus, hosts, args):
        super(AnimatedFrontend, self).__init__(event_bus, hosts, args)
        self.parallel = args.parallel
        event_bus.register({
            "host.begin": self.on_host_begin,
            "render": self.on_render,
        })

        # Keep track of rendering state
        self.host_ticks = OrderedDefaultDict(lambda: 0)

    @inlineCallbacks
    def on_enqueue(self, deploys):
        # the deployer has added a host to the queue to deploy to
        self.enqueued_hosts += 1

        # we won't pause the action if we're near the end or have room for more
        completed_hosts = self.count_completed_hosts()
        if completed_hosts + self.pause_after >= self.count_hosts():
            return

        if not self.pause_after or self.enqueued_hosts < self.pause_after:
            return

        # wait for outstanding hosts to finish up
        yield DeferredList(deploys, consumeErrors=True)

        # prompt the user for what to do now
        while True:

            # Signal to renderer that we should show the main prompt
            self.needs_main_prompt = True
            c = yield self.console_input.read_character()
            self.needs_main_prompt = False

            if c == "a":
                self.pause_after = 0
                break
            elif c == "x":
                raise AbortDeploy("x pressed")
            elif c == "c":
                self.pause_after = 1
                break
            elif c == "p":
                min_percent = self.percent_complete() + 1

                # Signal to renderer that we need percent prompt
                self.needs_percent_prompt = True
                prompt_input = yield self.console_input.raw_input('')
                self.needs_percent_prompt = False

                try:
                    desired_percent = int(prompt_input)
                except ValueError:
                    continue

                if not (min_percent <= desired_percent <= 100):
                    continue

                completed_hosts = self.count_completed_hosts()
                desired_host_index = int(
                    (desired_percent / 100) * self.count_hosts())
                self.pause_after = desired_host_index - completed_hosts
                break

        self.enqueued_hosts = 0

    def _draw_box(self, render_frame, row, col, width, height):
        # Top
        render_frame.add('+' + '-' * (width - 2) + '+', row, col)

        # Left Side
        for i in xrange(row + 1, row + height - 1):
            render_frame.add('|', i, col)

        # Right Side
        for i in xrange(row + 1, row + height - 1):
            render_frame.add('|', i, col + width - 1)

        # Bottom
        render_frame.add('+' + '-' * (width - 2) + '+', row + height - 1, col)

    def on_render(self):
        print clear_screen()

        # Describes the inner bounding box (i.e. where the hosts will be drawn
        # and animated
        row, col = 2, 2
        max_row, max_col = min(max(self.parallel, 15), 30), 120

        # Draw main bounding box
        render_frame = RenderFrame()
        self._draw_box(render_frame, row - 1, col - 1, max_col + 1, max_row + 1)

        for host, count in self.host_ticks.iteritems():

            # Calculate what we'll put on screen
            num_chars_onscreen = max(max_col - count - 1, 0)

            if num_chars_onscreen:
                sliced_host = host[:num_chars_onscreen]

                # Color the host
                if not self.host_results[host]:
                    colored_host = sliced_host
                elif self.host_results[host] == 'success':
                    colored_host = colorize(sliced_host, Color.GREEN)
                elif self.host_results[host] == 'warning':
                    colored_host = colorize(sliced_host, Color.YELLOW)
                elif self.host_results[host] == 'error':
                    colored_host = colorize(sliced_host, Color.RED)

                # Put on screen
                render_frame.add(colored_host, row, col + count)

                self.host_ticks[host] += 1

            row += 1
            if row > max_row:
                row = 2

        # Show status count boxes
        counter = collections.Counter(self.host_results.values())
        WIDTH = 10
        render_frame = RenderFrame()
        self._draw_box(render_frame, row=max_row + 1, col=1, width=WIDTH,
                       height=3)
        render_frame.add(
            colorize(str(counter['success']), Color.GREEN), max_row + 2, 3)
        self._draw_box(render_frame, row=max_row + 1, col=2 + WIDTH,
                       width=WIDTH, height=3)
        render_frame.add(
            colorize(str(counter['warning']), Color.YELLOW),
            max_row + 2, 4 + WIDTH)
        self._draw_box(
            render_frame, row=max_row + 1, col=3 + WIDTH * 2, width=WIDTH,
            height=3)
        render_frame.add(
            colorize(str(counter['error']), Color.RED),
            max_row + 2, 5 + WIDTH * 2)
        render_frame.write_to_screen()

        # Show prompts if necessary
        if getattr(self, 'needs_main_prompt', False):
            print at_position(colorize(
                "*** waiting for input: e[x]it, [c]ontinue, [a]ll remaining, "
                "[p]ercentage", Color.BOLD(Color.CYAN)),
                row=max_row + 1 + 3 + 1, col=1)
        if getattr(self, 'needs_percent_prompt', False):
            min_percent = self.percent_complete() + 1
            prompt = "how far? (%d-100) " % min_percent
            print at_position(prompt, row=max_row + 1 + 3 + 1, col=1)

    def on_host_begin(self, host):
        self.host_ticks[host] += 1

    def on_host_end(self, host):
        if host in self.host_results:
            self.host_results[host] = "success"
