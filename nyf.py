#!/usr/bin/env python

import re
import xml.etree.cElementTree as ET
import sys
import os
import binascii
import threading

import db
import nntpfetcher
import yencextractor
import logging
from logging import fatal
from logging import warning
from logging import debug

def main():
	try:
		logging.basicConfig(level=logging.DEBUG)

		if len(sys.argv) < 2:
			print "Usage: %s action arguments" % sys.argv[0]
			print "Actions: start, continue, verify"
			sys.exit(1)

		if sys.argv[1] == "start":
			action_start()
		elif sys.argv[1] == "continue":
			action_continue()
		elif sys.argv[1] == "verify":
			action_verify()
		else:
			print "Unknown action."

	except KeyboardInterrupt:
		debug("Canceled by user")

def action_start():
	if len(sys.argv) < 3:
		print "Usage: %s start nzbfile [target_directory]" % sys.argv[0]
		sys.exit(1)
	nzbfile = sys.argv[2]

	target_dir = nzbfile + "_content"
	if len(sys.argv) == 4:
		target_dir = sys.argv[3]
	dbfile = target_dir + "/parts.db"

	if os.path.isdir(target_dir):
		print "Target already exists."
		# TODO: prompt for continuation?
		return 

	os.mkdir(target_dir)

	# open nzb and create database
	nzb = ET.parse(nzbfile).getroot()
	ddb = db.DownloadDBSqlite(dbfile)

	# determine if namespace prefixes used
	try:
		ns = re.match("^(\{.*\}).*$", nzb.tag).group(1)
	except:
		ns = ""

	# write files and segments into db
	files = nzb.findall(ns + "file")
	for f in files:
		# insert file
		groups = map(lambda g: g.text, f.findall(".//" + ns + "group"))
		file_id = ddb.insert_file(f.attrib["poster"], f.attrib["date"], f.attrib["subject"], groups)
		segments = f.findall(".//" + ns + "segment")
		for s in segments:
			# insert segment
			ddb.insert_segment(file_id, s.text, s.attrib["bytes"], "", s.attrib["number"], len(segments), "", "", "", 0, 0, 0)
		ddb.commit()
		# TODO: catch sqlite3.OperationalError

class FetcherTask(threading.Thread):
	def __init__(self, pool, fetcher, msgid):
		print "TASK: ", msgid, " FETCHER: ", fetcher
		self.pool = pool
		self.fetcher = fetcher
		self.msgid = msgid
		self.content = None
		threading.Thread.__init__(self)
	def get_content(self):
		return self.content
	def run(self):
		self.content = self.fetcher.fetch_segment(self.msgid)
		self.pool.finish(self)

class FetcherPool:
	def __init__(self, threads):
		self.cond = threading.Condition()
		self.fetchers_idle = []
		self.active_tasks = []
		for i in range(0, threads):
			self.fetchers_idle.append(nntpfetcher.NNTPFetcher())

	def is_fetching(self, msgid):
		for task in self.active_tasks:
			if task.msgid == msgid:
				return True
		return False

	def finish(self, task):
		self.cond.acquire()
		print "Received ", len(task.content), " bytes."
		self.fetchers_idle.append(task.fetcher)
		self.cond.notify()
		# TODO: process data: extract, update db
		self.active_tasks.remove(task)
		self.cond.release()

	def fetch(self, msgid):
		self.cond.acquire()
		while len(self.fetchers_idle) <= 0:
			print "POOL WAITING"
			self.cond.wait()
		fetcher = self.fetchers_idle.pop()
		task = FetcherTask(self, fetcher, msgid)
		self.active_tasks.append(task)
		task.start()
		self.cond.release()
		
		

def action_continue():
	max_tries = 3
	poolsize = 2

	if len(sys.argv) < 3:
		print "Usage: %s continue target_directory" % sys.argv[0]
		sys.exit(1)

	target_dir = sys.argv[2]
	if not os.path.isdir(target_dir):
		print "Target directory %s doesn't exist." % target_dir
		sys.exit(1)

	#pool = FetcherPool(poolsize)

	fetcher = nntpfetcher.NNTPFetcher()
	extractor = yencextractor.yEncExtractor(target_dir)

	dbfile = target_dir + "/parts.db"
	ddb = db.DownloadDBSqlite(dbfile)
	while True:
		# TODO: always query just one or directly 5?... db ACID questions...
		metadata = False
		incomplete_segments = list(ddb.select("SELECT file_id, part, msgid FROM segments WHERE complete = 0 AND tries < ? ORDER BY tries, file_id, part LIMIT 1;", [max_tries]))
		if len(incomplete_segments) == 0:
			print "Finished"
			return
		for row in incomplete_segments:
			file_id, part, msgid = row

			lines = fetcher.fetch_segment(msgid)
			#pool.fetch(msgid)
			#continue
			# TODO


			if not lines:
				# skip segment
				warning("Problem while fetching.")
				continue
			metadata = extractor.extract(lines)
			# TODO: threading stuff
		if metadata:
			ddb.update("UPDATE segments SET filename=?,begin=?,end=?,pcrc32=?,complete=1 WHERE file_id=? AND part=?;", [metadata['name'], metadata['begin'], metadata['end'], metadata['pcrc32'], file_id, part])
			ddb.commit()
		else:
			pass # TODO: tries += 1

def action_verify():
	if len(sys.argv) < 3:
		print "Usage: %s continue target_directory" % sys.argv[0]
		sys.exit(1)

	target_dir = sys.argv[2]
	if not os.path.isdir(target_dir):
		print "Target directory %s doesn't exist." % target_dir
		sys.exit(1)

	dbfile = target_dir + "/parts.db"
	ddb = db.DownloadDBSqlite(dbfile)
	print "Verifying completed segments..."

	files = list(ddb.select("SELECT files.id, segments.filename FROM files, segments WHERE files.id = segments.file_id AND segments.complete = 1 GROUP BY files.id;"))
	# NOTE: actual SQL would require an aggregate function for segments.filename here (e.g. FIRST())
	for row in files:
		file_id, filename = row
		if filename == "":
			continue # wrong/old entry
		f = open(target_dir + os.sep + filename, 'rb')
		completed_segments = list(ddb.select("SELECT filename, part, total, begin, end, pcrc32 FROM segments WHERE complete = 1 AND file_id = ? ORDER BY begin;", [file_id]))
		for row in completed_segments:
			filename, part, total, begin, end, pcrc32 = row
			if filename == "":
				continue # wrong/old entry
			f.seek(begin - 1)
			data = f.read(end - begin + 1)
			crc32 = binascii.crc32(data) & 0xffffffff
			if crc32 != pcrc32:
				warning("CHECKSUM MISMATCH: %s part %i" % (filename, part))
			else:
				# debug("Checksum OK: %s part %i" % (filename, part))
				pass
		f.close()

if __name__ == "__main__":
	main()

