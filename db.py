#!/usr/bin/env python

import sys
import os
import re
import sqlite3

filename_re = re.compile("^.*\"(?P<filename>.*)\".*$")

def main():
	db = DownloadDBSqlite(sys.argv[1])

class DownloadDBSqlite():
	def __init__(self, filename):
		new = False
		if not os.path.isfile(filename):
			new = True
		self.conn = sqlite3.connect(filename)
		if new:
			self.exec_script("schema_files.sql")
			self.exec_script("schema_segments.sql")

	def exec_script(self, filename):
		f = open(filename)
		schema_sql = f.read()
		f.close()
		cur = self.conn.cursor()
		cur.execute(schema_sql)
		self.conn.commit()

	def insert_file(self, poster, date, subject, groups):

		# try to guess filename by subject (later useful downloading files in alphabetical order)
		match = filename_re.match(subject)
		guessed_filename = match.group('filename') if match else ""

		cur = self.conn.cursor()
		cur.execute('INSERT INTO files (poster, date, subject, guessed_filename, groups) VALUES (?, ?, ?, ?, ?);', (poster, date, subject, guessed_filename, ";".join(groups)))
		self.conn.commit()
		cur.execute('SELECT seq FROM sqlite_sequence WHERE name = "files";')
		return cur.fetchone()[0]

	def insert_segment(self, file_id, msgid, bytes_nzb, filename, part, total, begin, end, pcrc32, tries, errors, complete):
		cur = self.conn.cursor()
		cur.execute('INSERT INTO segments (file_id, msgid, bytes_nzb, filename, part, total, begin, end, pcrc32, tries, errors, complete) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);', (file_id, msgid, bytes_nzb, filename, part, total, begin, end, pcrc32, tries, errors, complete))
		return True

	def select(self, sql, arguments=[]):
		cur = self.conn.cursor()
		for row in cur.execute(sql, arguments):
			yield row

	def update(self, sql, arguments=[]):
		cur = self.conn.cursor()
		return cur.execute(sql, arguments) # TODO return what?

	def commit(self):
		# TODO: implement some kind of commit-timer? + add force-commit option?
		self.conn.commit()

if __name__ == '__main__':
	main();

