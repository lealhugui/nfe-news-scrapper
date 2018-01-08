# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html

import sqlite3
import datetime
from scrapy.mail import MailSender


class NfeParserPipeline(object):

	def __init__(self, db_path, sts):
		self.db_path = db_path
		self.sts = sts
		
	@classmethod
	def from_crawler(cls, crawler):
		return cls(sts=crawler.settings ,db_path=crawler.settings.get('DB_PATH', 'news.db'))

	def open_spider(self, spider):
		self.conn = sqlite3.connect(self.db_path)
		self.cursor = self.conn.cursor()

		cursor = self.cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='news'").fetchall()
		if len(cursor) == 0:
			
			self.cursor.execute("""
				CREATE TABLE news (
					id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
					title TEXT NOT NULL,
					link TEXT NOT NULL,
					enviado INTEGER NOT NULL DEFAULT 0,
					created_at DATETIME NOT NULL
					);
				""")
			

	def close_spider(self, spider):
		#self.cursor.commit()
		self.conn.commit()
		items_enviados = list()
		try:
			crs = self.cursor.execute("""select id, title, link from news where enviado=0""").fetchall()
			news = list()
			for item in crs:
				news.append('<p><a href="{link}">{title}</a></p>'.format(link=item[2], title=item[1]))
				items_enviados.append(str(item[0]))
			if len(news):
				mailer = MailSender.from_settings(self.sts)
				body = "<h1>Novidades NF-e!</h1><br><div>{body}</div>".format(body="".join(news))
				send_to = self.sts.MAIL_TO
				if len(send_to) > 0:
					mailer.send(to=send_to, subject='Novidades NF-e', body=body, mimetype="text/html")
		except Exception as e:
			print(e)
			pass
		else:
			if len(items_enviados) > 0:
				self.cursor.execute("""update news set enviado=1 where id in ({})""".format(", ".join(items_enviados)))
				self.conn.commit()



		self.cursor.close()
		self.conn.close()

	def process_item(self, item, spider):
		cursor = self.cursor.execute("SELECT * FROM news WHERE title=? AND link=?", (item["title"], item["link"])).fetchall()
		if len(cursor) == 0:
			cursor = self.conn
			self.cursor.execute("""
				INSERT INTO news(title, link, created_at)
				VALUES(?, ?, ?)
				""", (item["title"], item["link"], datetime.datetime.now()))

