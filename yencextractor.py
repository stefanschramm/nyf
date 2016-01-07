#!/usr/bin/env python

import re
import os
import sys
import yenc

import logging

# some regular expressions for ybegin/-part/-end parsing
res = {
	'name': re.compile("^.* name=(.+)$"),
	'line': re.compile("^.* line=(\d{3}) .*$"),
	'size': re.compile("^.* size=(\d+).*$"),
	'part': re.compile("^.* part=(\d+) .*$"),
	'crc32': re.compile("^.* crc32=(\w+) .*$"),
	'begin': re.compile("^.* begin=(\d+).*$"),
	'end': re.compile("^.* end=(\d+).*$"),
	'part': re.compile("^.* part=(\d+).*$"),
	'total': re.compile("^.* total=(\d+).*$"),
	'pcrc32': re.compile("^.* pcrc32=(\w+).*$")
}

def main():
	"""extract yenc data (of file specified as first argument) into current directory"""
	extractor = yEncExtractor("./")
	f = open(sys.argv[1], 'rb')
	lines = f.readlines()
	f.close()
	extractor.extract(lines)

class yEncExtractor:
	def __init__(self, target_dir):
		self.target_dir = target_dir

	def extract(self, lines):
		try:
			metadata = extract_yenc(lines, self.target_dir)
		except Exception as e:
			logging.warning("Exception while ydecoding: %s" % str(e))
			return False
		return metadata

def parse_ybegin(line):
	"""Parse =ybegin header"""
	logging.debug("parse_ybegin: %s" % line)
	ybegin = {}
	try:
		ybegin['name'] = res['name'].match(line).group(1)
		ybegin['line'] = int(res['line'].match(line).group(1))
		ybegin['size'] = int(res['size'].match(line).group(1))
	except:
		# incorrect header
		return False
	# optional: multipart
	m = res['part'].match(line)
	if m:
		ybegin['part'] = int(m.group(1))
	# optional: total
	m = res['total'].match(line)
	if m:
		ybegin['total'] = int(m.group(1))
	return ybegin

def parse_ypart(line):
	"""Parse =ypart header"""
	logging.debug("parse_ypart: %s" % line)
	ypart = {}
	try:
		ypart['begin'] = int(res['begin'].match(line).group(1))
		ypart['end'] = int(res['end'].match(line).group(1))
	except:
		# incorrect header
		return False
	return ypart

def parse_yend(line):
	"""Parse =yend trailer"""
	logging.debug("parse_yend: %s" % line)
	yend = {}
	try:
		yend['size'] = int(res['size'].match(line).group(1))
	except:
		# incorrect header
		return False
	# optional: pcrc32 (actually mandatory if multipart)
	m = res['pcrc32'].match(line)
	if m:
		yend['pcrc32'] = int(m.group(1), 16)
	# optional: part (actually mandatory if multipart)
	m = res['part'].match(line)
	if m:
		yend['part'] = int(m.group(1))
	# optional: crc32
	m = res['crc32'].match(line)
	if m:
		yend['crc32'] = int(m.group(1), 16)
	return yend

def write_data(data_in, target_dir, filename, begin = False):
	"""Use yenc.Decoder to decode data from data_in and write into target_dir/filename using begin-1 as offset"""
	# ensure filename doesn't contain stuff that would allow messing around in the filesystem
	filename = filename.replace(os.sep, "_")
	# open (or if necessary create) file
	mode = "r+b" if os.path.isfile(target_dir + os.sep + filename) else "wb"
	file_out = open(target_dir + os.sep + filename, mode)
	# write data to correct position in file
	file_out.seek(0 if begin == False else begin - 1)
	dec = yenc.Decoder(file_out)
	while True:
		try:
			data = data_in.next()
		except StopIteration:
			logging.warning("=yend missing (StopIteration)")
			break
		if data.startswith("=yend"):
			# trailer
			yend = parse_yend(data)
			if "pcrc32" in yend:
				calculated_crc = int(dec.getCrc32(), 16) & 0xffffffff
				if yend["pcrc32"] != calculated_crc:
					logging.warning("CRC32 mismatch: %x vs %x" % (yend["pcrc32"], calculated_crc))
			dec.flush()
			return yend
		else:
			# usual data line
			dec.feed(data)

def extract_yenc(lines, target_dir):
	"""Search for =ybegin headers in lines and save files to target_dir."""
	ybegin = {}
	ypart = {}
	yend = {}
	# TODO: has iter() a problem with to long lists (len(lines) >= 10147)?
	i = iter(lines)
	# collect some metadata (useful for creating a map file part -> msgid)
	metadata = []
	while True:
		try:
			line = i.next()
		except StopIteration:
			break
		if not line:
			continue # skip empty lines (no break)
		elif line.startswith("=ybegin "):
			ybegin = parse_ybegin(line)
			if not ybegin:
				logging.warning("Incorrect ybegin: %s" % line)
				continue
			if ybegin["part"]:
				# consists of parts - next line must be =ypart
				line = i.next()
				ypart = parse_ypart(line)
				if not ypart:
					logging.warning("Incorrect ypart: %s" % line)
					continue
				yend = write_data(i, target_dir, ybegin["name"], ypart["begin"])
				continue
			else:
				# has no parts
				yend = write_data(i, target_dir, ybegin["name"], False)
				continue
		elif line.startswith("=ypart"):
			logging.warning("Unexpected =ypart")
			continue
	metadata = {
		"name": ybegin["name"],
		"begin": ypart["begin"],
		"end": ypart["end"],
		"pcrc32": yend["pcrc32"]
	}
	return metadata

if __name__ == "__main__":
	main()

