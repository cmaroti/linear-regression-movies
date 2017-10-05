#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Sep 27 15:03:39 2017

@author: christinemaroti
"""

import scrapy


class MovieSpider(scrapy.Spider):

    name = 'movies'

    custom_settings = {
        "DOWNLOAD_DELAY": 2,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 5,
        "HTTPCACHE_ENABLED": True
    }

    start_urls = [
        'http://www.boxofficemojo.com/yearly/chart/?yr=2016&p=.htm/',
        'http://www.boxofficemojo.com/yearly/chart/?page=2&view=releasedate&view2=domestic&yr=2016&p=.htm',
        'http://www.boxofficemojo.com/yearly/chart/?yr=2015&p=.htm',
        'http://www.boxofficemojo.com/yearly/chart/?page=2&view=releasedate&view2=domestic&yr=2015&p=.htm',
        'http://www.boxofficemojo.com/yearly/chart/?yr=2014&p=.htm',
        'http://www.boxofficemojo.com/yearly/chart/?page=2&view=releasedate&view2=domestic&yr=2014&p=.htm',
        'http://www.boxofficemojo.com/yearly/chart/?yr=2013&p=.htm',
        'http://www.boxofficemojo.com/yearly/chart/?page=2&view=releasedate&view2=domestic&yr=2013&p=.htm',
        'http://www.boxofficemojo.com/yearly/chart/?yr=2012&p=.htm',
        'http://www.boxofficemojo.com/yearly/chart/?page=2&view=releasedate&view2=domestic&yr=2012&p=.htm',
        'http://www.boxofficemojo.com/yearly/chart/?yr=2011&p=.htm',
        'http://www.boxofficemojo.com/yearly/chart/?page=2&view=releasedate&view2=domestic&yr=2011&p=.htm'
        'http://www.boxofficemojo.com/yearly/chart/?yr=2010&p=.htm',
        'http://www.boxofficemojo.com/yearly/chart/?page=2&view=releasedate&view2=domestic&yr=2010&p=.htm'
    ]

    def parse(self, response):
        boxoff = 'http://www.boxofficemojo.com'
        url_list = response.xpath('//div[@id="body"]/table[3]/tr/td[1]/table/tr[2]/td/table/tr/td[2]/b/font/a/@href').extract()
        title_list = response.xpath('//div[@id="body"]/table[3]/tr/td[1]/table/tr[2]/td/table/tr/td[2]/b/font/a/text()').extract()
        for url in url_list:
            href = boxoff + url
            yield scrapy.Request(
                url=href,
                callback=self.parse_movie,
                meta={'url': href, 'title': title_list[url_list.index(url)]}
            )


    def parse_movie(self, response):

        url = response.request.meta['url']

        title = response.request.meta['title']

        genre = response.xpath('//td[@valign= "top"]/b/text()').extract()[0]

        run_time = response.xpath('//td[@valign= "top"]/b/text()').extract()[1]

        mpaa = response.xpath('//td[@valign= "top"]/b/text()').extract()[2]

        budget = response.xpath('//td[@valign= "top"]/b/text()').extract()[3]

        distributor = response.xpath('//td[@valign= "top"]/b/a/text()').extract()[0]

        release_date = response.xpath('//td[@valign= "top"]/b/nobr/a/text()').extract()[0]

        dom_gross = response.xpath('//div[@class= "mp_box_content"]/table/tr/td[2]/b/text()').extract()[0]

        foreign_gross = response.xpath('//div[@class= "mp_box_content"]/table/tr/td[2]/text()').extract()[1]

        num_theaters = response.xpath("//td[contains(text(), 'Widest')]//following-sibling::td/text()").extract_first()

        days_out = response.xpath("//td[contains(text(), 'In Release')]//following-sibling::td/text()").extract_first()

        opening_weekend = response.xpath("//a[contains(text(), 'Opening')]/parent::*/following-sibling::td/text()").extract_first()

        foreign_url = 'http://www.boxofficemojo.com' + response.xpath("//a[contains(text(), 'Foreign')]/@href").extract_first()

        yield scrapy.Request(
            url=foreign_url,
            callback=self.parse_foreign,
            meta={
                'url': url,
                'title': title,
                'genre': genre,
                'run_time': run_time,
                'rating': mpaa,
                'budget': budget,
                'distributor': distributor,
                'release_date': release_date,
                'domestic_gross': dom_gross,
                'foreign_gross': foreign_gross,
                'num_theaters': num_theaters,
                'days_out': days_out,
                'opening_weekend': opening_weekend}
        )

    def parse_foreign(self, response):

        url = response.request.meta['url']
        title = response.request.meta['title']
        genre = response.request.meta['genre']
        run_time = response.request.meta['run_time']
        rating = response.request.meta['rating']
        budget = response.request.meta['budget']
        distributor = response.request.meta['distributor']
        release_date = response.request.meta['release_date']
        domestic_gross = response.request.meta['domestic_gross']
        foreign_gross = response.request.meta['foreign_gross']
        num_theaters = response.request.meta['num_theaters']
        days_out = response.request.meta['days_out']
        opening_weekend = response.request.meta['opening_weekend']

        country_list = response.xpath('//td[@valign="top"]/table[2]/tr/td/table/tr/td/font/b/a/text()').extract()[1:]
        country_gross_list = response.xpath('//td[@valign="top"]/table[2]/tr/td/table/tr/td[6]/font/b/text()').extract()

        yield {
            'url': url,
            'title': title,
            'genre': genre,
            'run_time': run_time,
            'rating': rating,
            'budget': budget,
            'distributor': distributor,
            'release_date': release_date,
            'domestic_gross': domestic_gross,
            'foreign_gross': foreign_gross,
            'num_theaters': num_theaters,
            'days_out': days_out,
            'opening_weekend': opening_weekend,
            'country_list': country_list,
            'country_gross_list': country_gross_list}
