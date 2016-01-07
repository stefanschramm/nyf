#!/usr/bin/env python

import nntplib
import socket
import exceptions
import sys
import os
import yaml

from logging import fatal
from logging import warning
from logging import debug

def main():
	fetcher = NNTPFetcher()
	lines = fetcher.fetch_segment(sys.argv[1])
	if not lines:
		print "Problem."
		sys.exit(1)
	for l in lines:
		print l

class NNTPFetcher:
	def __init__(self):
		# nntp connection
		self.nntp = False

	def connect_nntp(self):
		try:
			settings = yaml.load(open(os.path.expanduser('~/.nyfrc')))
			debug("connect_nntp - settings: " + str(settings))
			self.nntp = nntplib.NNTP(settings['server'], settings['port'], settings['user'], settings['password'])
		except Exception as e:
			fatal("Unable to connect to NNTP server: %s" % e)

	def fetch_segment(self, msgid, tries=3):
		"""Fetch one message (one file segment)."""

		debug("fetch_segment - msgid: %s" % msgid)
		if not self.nntp:
			self.connect_nntp()
		try:
			response, returned_msgid, x, lines = self.nntp.body("<" + msgid + ">")
			debug("fetch_segment - got %i lines - response: %s" % (len(lines), response) )
			# DEBUG: fetch_segment - got 3062 lines - response: 222 0 <7VvHrm3mNkUYDCuhR4t8_7o20@JBinUp.local> body
		except exceptions.EOFError as e:
			warning("EOF Error Exception while fetching message body: %s %s" % (str(type(e)), str(e)))
			# often followed by socket.error in next try
			if tries >= 0:
				# TODO: sleep?
				self.nntp = None # TODO: try to disconnect?
				return self.fetch_segment(msgid, tries - 1)
			else:
				fatal("- aborting because no tries left")
		except socket.error as e:
			warning("Socket Error Exception while fetching message body: %s %s" % (str(type(e)), str(e)))
			if tries >= 0:
				# TODO: sleep?
				self.nntp = None # TODO: try to disconnect?
				return self.fetch_segment(msgid, tries - 1)
			else:
				fatal("- aborting because no tries left")
			# Examples:
			# [Errno 32] Broken pipe
		# TODO: retry reconnect: sleep 5*(2**n) (0 <= n <= 10 == max_retries) + only print warnings, no fatal exit
		except Exception as e:
			warning("Exception while fetching message body: %s %s" % (str(type(e)), str(e)))
			# WARNING: Exception while fetching message body: 400 reader.xsusenet.com: Idle timeout.
			# WARNING: Exception while fetching message body: <class 'nntplib.NNTPTemporaryError'> 400 reader.xsusenet.com: Session timeout.
			# WARNING: Problem while fetching.
			# WARNING: Exception while fetching message body:
			# WARNING: Exception while fetching message body: [Errno 32] Broken pipe
			#warning("Skipping segment and trying to reconnect...")
			#connect_nntp()
			return False
		return lines


if __name__ == '__main__':
	main()

