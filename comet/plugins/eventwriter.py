# Comet VOEvent Broker.
# Example event handler: write an event to file.

import os
import string
from contextlib import contextmanager

from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

import voeventparse
from ovro_alert import alert_client
from astropy import time

from zope.interface import implementer
from twisted.plugin import IPlugin
from twisted.python import lockfile

from comet.icomet import IHandler, IHasOptions
import comet.log as log


# Used when building filenames to avoid over-writing.
FILENAME_PAD = "_"
token = os.environ.get("SLACK_TOKEN_DSA")

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

    # When the handler is called, it is passed an instance of
    # comet.utility.xml.xml_document.
    def __call__(self, event):
        """
        Save an event to disk and update slack.
        """
        if not os.path.exists(self.directory):
            os.makedirs(self.directory)

        # create voevent
        log.info("Creating voevent")
        voevent = voeventparse.loads(event.raw_bytes) 
        role = voevent.get('role')

        if role == 'observation':
            with event_file(event.element.attrib["ivorn"], self.directory) as f:
                log.info("Writing to %s" % (f.name,))
                f.write(event.raw_bytes.decode(event.encoding))

        # send all non-tests, plus ever 24th test
        if (role != 'test') or (not self.testcount % 24):
            if self.testcount == 0 and role == 'test':  # reference time for first test
                self.starttime = time.Time.now().unix
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

        client = WebClient(token=token)

        role = voevent.get('role')
        if role == "observation":
            # Load the VOEvent file
            dm = voeventparse.convenience.get_grouped_params(voevent)['event parameters']['dm']['value']
            toa = voeventparse.convenience.get_event_time_as_utc(voevent)
            position = voeventparse.convenience.get_event_position(voevent)
            params = voeventparse.convenience.get_grouped_params(voevent)
            event_no = params['event parameters']['event_no']['value']
            snr = params['event parameters']['snr']['value']
            message = f"CHIME/FRB event {event_no}: \n MJD {time.Time(toa).mjd} \n Event Position: {position.ra:.1f},{position.dec:.1f},{position.err:.1f} \n DM: {float(dm):.1f} \n SNR: {float(snr):.1f}"

            known = params['event parameters']['known_source_name']['value']
            if known is not '':
                message += f' \n Associated with known source: {known}'

        elif role == "utility":
            # Load the VOEvent file
            ivorn = voevent.get('ivorn')
            if "RETRACTION" in ivorn:
                message = f"CHIME/FRB retraction issued: {ivorn}"
            else:
                message = f"CHIME/FRB utility event issued (unknown reason)"

        elif role == 'test':
             if self.testcount > 0:
                nhours = (time.Time.now().unix-self.starttime)/3600
                testrate = self.testcount/nhours
                message = f"CHIME/FRB test event: received {testrate:.1f} per hour in last day."
                if nhours > 24:
                    self.testcount = 0
                    self.starttime = time.Time.now().unix
             else:
                 message = f"CHIME/FRB test event: will report events after 24hrs"
 
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

        chimec = alert_client.AlertClient('chime')
        
        role = voevent.get('role')
        if role == "observation":
            dm = voeventparse.convenience.get_grouped_params(voevent)['event parameters']['dm']['value']
            toa = voeventparse.convenience.get_event_time_as_utc(voevent).isoformat()
            position = voeventparse.convenience.get_event_position(voevent)
            known = params['event parameters']['known_source_name']['value']
            args = {"role": role, "dm": dm, "toa": toa, "position": f"{position.ra},{position.dec},{position.err}", "known": known}
        elif role == "test":
            date = voevent.Who.find("Date").text
            description = voevent.What.find("Description").text
            args = {"role": role, "date": date, "description": description}

        log.info("Set to relay")
        chimec.set(role, args=args)

# This instance of the handler is what actually constitutes our plugin.
save_event = EventWriter()
