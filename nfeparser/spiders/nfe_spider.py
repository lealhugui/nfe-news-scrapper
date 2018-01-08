# -*- coding: utf-8 -*-

import scrapy


class NFeSpider(scrapy.Spider):
	name = "nfe"
	base_url = "http://www.nfe.fazenda.gov.br/portal"
	start_urls = [base_url + "/listaConteudo.aspx?tipoConteudo=tW+YMyk/50s="]

	def parse(self, response):
		filename = "out.json"
		for item in response.css('.indentacaoNormal').css('p'):
			text = item.css('span.tituloConteudo::text').extract_first()
			link = item.css('a::attr(href)').extract_first()
			if text is not None and link is not None:
				yield {
					"title": text,
					"link": self.base_url + "/" + link.strip()
				}