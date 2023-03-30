# Comet VOEvent Broker.
# Example event handler: write an event to file.

import os
import string
from contextlib import contextmanager

from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

import voeventparse
from ovro_alert import alert_client
import time

from zope.interface import implementer
from twisted.plugin import IPlugin
from twisted.python import lockfile

from comet.icomet import IHandler, IHasOptions
import comet.log as log


# Used when building filenames to avoid over-writing.
FILENAME_PAD = "_"
SLACK_TOKEN = os.environ.get("SLACK_TOKEN")

def string_to_filename(input_string):
    # Strip weird, confusing or special characters from input_string so that
    # we can safely use it as a filename.
    # Replace "/" and "\" with "_" for readability.
    # Allow ".", but not as the first character.
    if input_string[0] == ".":
        input_string = input_string[1:]
    return "".join(
        x
        for x in input_string.replace("/", "_").replace("\\", "_")
        if x in string.digits + string.ascii_letters + "_."
    )


@contextmanager
def event_file(ivoid, dirname=None):
    # Return a file object into which we can write an event.
    # If a directory is specified, write into that; otherwise, use the cwd.
    # We use a lock to ensure we don't clobber other files with the same name.
    if not dirname:
        dirname = os.getcwd()
    fname = os.path.join(dirname, string_to_filename(ivoid))
    lock = lockfile.FilesystemLock(string_to_filename(ivoid) + "-lock")
    lock.lock()
    try:
        while os.path.exists(fname):
            fname += FILENAME_PAD
        with open(fname, "w") as f:
            yield f
    finally:
        lock.unlock()


# Event handlers must implement IPlugin and IHandler.
# Implementing IHasOptions enables us to use command line options.
@implementer(IPlugin, IHandler, IHasOptions)
class EventWriter(object):
    # Simple example of an event handler plugin. This saves the events to
    # disk.

    # The name attribute enables the user to specify plugins they want on the
    # command line.
    name = "save-event"

    def __init__(self):
        self.directory = os.getcwd()
        self.testcount = 0
        self.starttime = time.time()

    # When the handler is called, it is passed an instance of
    # comet.utility.xml.xml_document.
    def __call__(self, event):
        """
        Save an event to disk and update slack.
        """
        if not os.path.exists(self.directory):
            os.makedirs(self.directory)

        with event_file(event.element.attrib["ivorn"], self.directory) as f:
            log.info("Writing to %s" % (f.name,))
            f.write(event.raw_bytes.decode(event.encoding))

        # create voevent
        log.info("Creating voevent")
        voevent = voeventparse.loads(event.raw_bytes) 
        role = voevent.get('role')

        # send all non-tests, plus ever 24th test
        if (role != 'test') or (self.testcount % 24):
            self.update_relay(voevent)
            self.update_slack(voevent)

        if role == 'test':
            log.info("Incrementing test counter")
            self.testcount += 1

    def get_options(self):
        return [("directory", self.directory, "Directory in which to save events")]

    def set_option(self, name, value):
        if name == "directory":
            self.directory = value

    def update_slack(self, voevent):
        """ parse VOEvent file and send a message to slack 
        """

        log.info("Parsing event for slack")

        client = WebClient(token=SLACK_TOKEN)

        role = voevent.get('role')
        if role != "test":
            # Load the VOEvent file
            dm = voeventparse.convenience.get_grouped_params(voevent)['event parameters']['dm']['value']
            toa = voeventparse.convenience.get_event_time_as_utc(voevent)
            position = voeventparse.convenience.get_event_position(voevent)
            message = f"CHIME/FRB VOEvent Received: \n TOA: {toa} \n Event Position: {position} \n DM: {dm}",
        else:
            date = voevent.Who.find("Date")
            if self.testcount > 0:
                testrate = ((time.time()-self.starttime)/3600)/self.testcount
                message = f"CHIME/FRB test report at {date}: received {self.testcount} events since start at rate of {testrate:.1f} per hour."
            else:
                message = f"CHIME/FRB test report at {date}: received first test event and will report every 24th event."

        log.info("Sending to slack")

        # Post to slack
        try:
            response = client.chat_postMessage(channel="#candidates", text=message, icon_emoji = ":zap:")
            log.info(response)
        except SlackApiError as e:
            log.error("Error sending message: {}".format(e))

    def update_relay(self, voevent):
        """ parse VOEvent file and send info to relay server
        """

        log.info("Parsing event for relay")

        dsac = alert_client.AlertClient('dsa')

        role = voevent.get('role')
        if role != "test":
            dm = voeventparse.convenience.get_grouped_params(voevent)['event parameters']['dm']['value']
            toa = voeventparse.convenience.get_event_time_as_utc(voevent).isoformat()
            position = voeventparse.convenience.get_event_position(voevent)
            args = {"dm": dm, "toa": toa, "position": f"{position.ra},{position.dec},{position.err}"}
        else:
            date = voevent.Who.find("Date").text
            description = voevent.What.find("Description").text
            args = {"role": role, "date": date, "description": description}

        log.info("Set to relay")
        dsac.set("CHIME FRB", args=args)

# This instance of the handler is what actually constitutes our plugin.
save_event = EventWriter()
