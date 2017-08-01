import scrapy


class CrawlerSaudePMF(scrapy.Spider):
    name = "saude_pmf"

    custom_settings = {
        'HTTPCACHE_ENABLED': True
    }

    # é possível trocar o método start_requests pelo atributo 
    # de classe start_urls = []

    def start_requests(self):
        urls = [
            'http://www.pmf.sc.gov.br/sistemas/saude/unidades_saude/populacao/uls_2015_index.php'
        ]

        for url in urls:
            yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):
        # Get all tables from page
        tables = response.css('table')
        self.log('Num tables: {}'.format(len(tables)))

        # Get lines from table 1
        table_trs = tables[1].css('tr')
        entries = []
        for tr in table_trs:
            entries.append(self.get_micro_areas(tr.css('td')))

        # self.log('YEAR: {}'.format(self.get_year(response.url)))
        # filename = 'resp.html'
        # with open(filename, 'wb') as f:
        #     f.write(response.body)
        # self.log('Saved file {}'.format(filename))

    def get_micro_areas(self, tds):
        distrito_sanitario = tds[0].css('a::text').extract_first()
        centro_saude = tds[1].css('a::text').extract_first()

        self.log('Distrito Sanitário: {}'.format(distrito_sanitario))
        self.log('Centro de Saúde {}'.format(centro_saude))

        micro_areas = []
        links = tds[2].css('a')
        self.log('Num links: {}'.format(len(links)))
        for link in links:
            # links.append(td.css('a'))
            micro_area_name = link.css('a::text').extract_first()
            micro_area_link = link.css('a::attr(href)').extract_first()
            self.log('micro area name: {}'.format(micro_area_name))
            self.log('micro area link: {}'.format(micro_area_link))

            micro_area = {}
            micro_area['name'] = micro_area_name
            micro_area['link'] = micro_area_link
            micro_areas.append(micro_area)

        entry = {}
        entry['distrito_sanitario'] = distrito_sanitario
        entry['centro_saude'] = centro_saude
        entry['micro_areas'] = micro_areas
        return entry

    def get_year(self, url):
        last_part = url.split('/')[-1]
        begin = last_part.find('_')
        end = last_part.rfind('_')
        return last_part[begin + 1:end]
